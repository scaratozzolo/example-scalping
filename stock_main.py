from datetime import datetime
import asyncio
import pandas as pd
import pytz
import sys
import logging

from alpaca.data.live import StockDataStream
from alpaca.trading.client import TradingClient
from alpaca.data.timeframe import TimeFrame
from alpaca.data.historical.stock import StockHistoricalDataClient
from alpaca.data.requests import StockBarsRequest, StockLatestTradeRequest, StockLatestBarRequest
from alpaca.trading.stream import TradingStream
from alpaca.data.enums import DataFeed
from alpaca.trading.requests import LimitOrderRequest, OrderRequest, MarketOrderRequest

logger = logging.getLogger()


ALPACA_API_KEY = ""
ALPACA_SECRET_KEY = ""


class ScalpAlgo:
    def __init__(self, api, hist_data_api, symbol, lot):
        self._api = api
        self._hist_data_api = hist_data_api
        self._symbol = symbol
        self._lot = lot
        self._bars = []
        self._l = logger.getChild(self._symbol)
        # self._l = logger.bind(symbol=self._symbol)

        now = pd.Timestamp.now(tz='America/New_York').floor('1min')
        market_open = now.replace(hour=9, minute=30)
        while 1:
            # at inception this results sometimes in api errors. this will work
            # around it. feel free to remove it once everything is stable
            try:
                bars_request = StockBarsRequest(
                    symbol_or_symbols=symbol,
                    start=market_open,
                    end=now,
                    timeframe=TimeFrame.Minute,
                    adjustment='raw',
                    feed=DataFeed.IEX,
                )
                data = hist_data_api.get_stock_bars(bars_request).df

                bars=data
                break
            except Exception as e:
                # make sure we get bars
                self._l.error(f"data failed: {e}")
                pass
            

        self._bars = bars

        self._init_state()

    def _init_state(self):
        symbol = self._symbol
        order = [o for o in self._api.get_orders() if o.symbol == symbol]
        position = [p for p in self._api.get_all_positions()
                    if p.symbol == symbol]
        self._order = order[0] if len(order) > 0 else None
        self._position = position[0] if len(position) > 0 else None
        if self._position is not None:
            if self._order is None:
                self._state = 'TO_SELL'
            else:
                self._state = 'SELL_SUBMITTED'
                if self._order.side != 'sell':
                    self._l.warn(
                        f'state {self._state} mismatch order {self._order}')
        else:
            if self._order is None:
                self._state = 'TO_BUY'
            else:
                self._state = 'BUY_SUBMITTED'
                if self._order.side != 'buy':
                    self._l.warn(
                        f'state {self._state} mismatch order {self._order}')

    def _now(self):
        return pd.Timestamp.now(tz='America/New_York')

    def _outofmarket(self):
        return self._now().time() >= pd.Timestamp('15:55').time()

    def checkup(self, position):
        # self._l.info('periodic task')

        now = self._now()
        order = self._order
        if (order is not None and
            order.side == 'buy' and now -
                pd.Timestamp(order.submitted_at).tz_convert(tz='America/New_York') > pd.Timedelta('2 min')):
            last_price = self._get_last_trade().price
            self._l.info(
                f'canceling missed buy order {order.id} at {order.limit_price} '
                f'(current price = {last_price})')
            self._cancel_order()

        if self._position is not None and self._outofmarket():
            self._cancel_order()
            # this places a new sell order that will trigger on_order_update
            # once the sell is filled on_order_update runs as if something was bought and places a sell order again, shorting the asset
            # might be caused be init_state in _cancel_order
            self._submit_sell(bailout=True)

    def _cancel_order(self):
        if self._order is not None:
            self._api.cancel_order_by_id(self._order.id)
            # self._init_state()

    def _calc_buy_signal(self):
        mavg = self._bars.rolling(20).mean().close.values
        change = (self._bars.iloc[-20:].pct_change(fill_method=None) > 0).close.values
        change_pct = change.sum()/len(change)
        closes = self._bars.close.values
        if closes[-2] < mavg[-2] and closes[-1] > mavg[-1]:
            self._l.info(
                f'buy signal: closes[-2] {closes[-2]} < mavg[-2] {mavg[-2]} '
                f'closes[-1] {closes[-1]} > mavg[-1] {mavg[-1]} ')
            return True
        else:
            self._l.debug(
                f'closes[-2:] = {closes[-2:]}, mavg[-2:] = {mavg[-2:]}, {change_pct=}')
            return False

    def on_bar(self, bar):
        self._bars = pd.concat([self._bars, pd.DataFrame({
            'open': bar.open,
            'high': bar.high,
            'low': bar.low,
            'close': bar.close,
            'volume': bar.volume,
        }, index=[pd.Timestamp(bar.timestamp).tz_convert(pytz.UTC)])])

        self._l.info(
            f'received bar start: {pd.Timestamp(bar.timestamp)}, close: {bar.close}, len(bars): {len(self._bars)}')
        if len(self._bars) < 21:
            return
        if self._outofmarket():
            return
        if self._state == 'TO_BUY':
            signal = self._calc_buy_signal()
            if signal:
                self._submit_buy()

    def on_order_update(self, event, order):
        self._l.info(f'order update: {event} = {order}')
        if event == 'fill':
            self._order = None
            if self._state == 'BUY_SUBMITTED':
                self._position = self._api.get_open_position(self._symbol)
                self._transition('TO_SELL')
                self._submit_sell()
                return
            elif self._state == 'SELL_SUBMITTED':
                self._position = None
                self._transition('TO_BUY')
                return
        elif event == 'partial_fill':
            self._position = self._api.get_open_position(self._symbol)
            self._order = self._api.get_order_by_id(order.id)
            return
        elif event in ('canceled', 'rejected'):
            if event == 'rejected':
                self._l.warn(f'order rejected: current order = {self._order}')
            self._order = None
            if self._state == 'BUY_SUBMITTED':
                if self._position is not None:
                    self._transition('TO_SELL')
                    self._submit_sell()
                else:
                    self._transition('TO_BUY')
            elif self._state == 'SELL_SUBMITTED':
                self._transition('TO_SELL')
                self._submit_sell(bailout=True)
            else:
                self._l.warn(f'unexpected state for {event}: {self._state}')

    def _submit_buy(self):
        trade = self._get_last_trade()
        # self._l.info(f"last trade: {trade=}")
        amount = int(self._lot / trade.price)
        try:
            order_request = LimitOrderRequest(
                symbol=self._symbol,
                side='buy',
                type='limit',
                qty=amount,
                time_in_force='day',
                limit_price=round(trade.price,2),
            )
            order = self._api.submit_order(order_request)
        except Exception as e:
            self._l.error(e)
            self._transition('TO_BUY')
            return

        self._order = order
        self._l.info(f'submitted buy {order}')
        self._transition('BUY_SUBMITTED')

    def _submit_sell(self, bailout=False):
        order_class = OrderRequest
        params = dict(
            symbol=self._symbol,
            side='sell',
            qty=self._position.qty,
            time_in_force='day',
        )
        if bailout:
            params['type'] = 'market'
            order_class = MarketOrderRequest
        else:
            current_price = float(
                self._get_last_trade().price)
            cost_basis = float(self._position.avg_entry_price)
            limit_price = round(max(cost_basis + 0.01, current_price),2)
            params.update(dict(
                type='limit',
                limit_price=limit_price,
            ))
            order_class = LimitOrderRequest

        try:
            order_request = order_class(**params)
            order = self._api.submit_order(order_request)
        except Exception as e:
            self._l.error(e)
            self._transition('TO_SELL')
            return

        self._order = order
        self._l.info(f'submitted sell {order}')
        self._transition('SELL_SUBMITTED')

    def _transition(self, new_state):
        self._l.info(f'transition from {self._state} to {new_state}')
        self._state = new_state

    def _get_last_trade(self):

        return self._hist_data_api.get_stock_latest_trade(
            StockLatestTradeRequest(
                symbol_or_symbols=self._symbol,
            )
        )[self._symbol]


def add_to_fleet(data):
    change = (data.iloc[-1]['close']/data.iloc[0]['close'])-1
    return change > 0



def main(args):

    stream = StockDataStream(
        ALPACA_API_KEY,
        ALPACA_SECRET_KEY,
    )
    logger.info("stock stream created")

    api = TradingClient(
        ALPACA_API_KEY,
        ALPACA_SECRET_KEY,
        paper=True,
    )
    logger.info("trading client created")

    hist_stock_api = StockHistoricalDataClient(
        ALPACA_API_KEY,
        ALPACA_SECRET_KEY,
    )
    logger.info(f"historical data client created")

    trading_stream = TradingStream(
        ALPACA_API_KEY,
        ALPACA_SECRET_KEY,
        paper=True,
    )
    logger.info(f"trading stream client created")


    fleet = {}
    symbols = args.symbols

    end = pd.Timestamp.now(tz='America/New_York').floor('1min')
    start = end - pd.Timedelta("7 days")
    bars_request = StockBarsRequest(
        symbol_or_symbols=args.symbols,
        start=start,
        end=end,
        timeframe=TimeFrame.Minute,
        adjustment='raw',
        feed=DataFeed.IEX,
    )
    week_data = hist_stock_api.get_stock_bars(bars_request).df

    for symbol in symbols:
        if add_to_fleet(week_data.loc[symbol]):
            logger.debug(f"{symbol} added to fleet")
            algo = ScalpAlgo(api, hist_stock_api, symbol, lot=2000)
            fleet[symbol] = algo
    logger.info(f"fleet initialized: {len(fleet)}")

    async def on_bars(data):
        if data.symbol in fleet:
            fleet[data.symbol].on_bar(data)

    for symbol in symbols:
        stream.subscribe_bars(on_bars, symbol)
    logger.info("fleet subscribed")

    async def on_trade_updates(data):
        logger.debug(f'trade_updates {data}')
        symbol = data.order.symbol
        if symbol in fleet:
            fleet[symbol].on_order_update(data.event, data.order)

    
    trading_stream.subscribe_trade_updates(on_trade_updates)

    async def periodic():
        while True:
            if not api.get_clock().is_open:
                logger.info('exit as market is not open')
                sys.exit(0)
            await asyncio.sleep(30)
            positions = api.get_all_positions()
            for symbol, algo in fleet.items():
                pos = [p for p in positions if p.symbol == symbol]
                algo.checkup(pos[0] if len(pos) > 0 else None)

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(asyncio.gather(
        stream._run_forever(),
        periodic(),
        trading_stream._run_forever(),
    ))
    loop.close()



if __name__ == '__main__':
    import argparse

    fmt = '%(asctime)s | %(filename)s:%(lineno)d | %(levelname)s | %(name)s | %(message)s'
    logging.basicConfig(level=logging.INFO, format=fmt)
    fh = logging.FileHandler('console.log')
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(logging.Formatter(fmt))
    logger.addHandler(fh)

    # parser = argparse.ArgumentParser()
    # parser.add_argument('symbols', nargs='+')
    # parser.add_argument('--lot', type=float, default=2000)

    class parser:
        lot = 2000
        with open("watchlist.txt", "r") as f:
            symbols = [i.strip() for i in f.readlines()]

    main(parser)
