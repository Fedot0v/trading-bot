import asyncio
import json
import time
import csv
from collections import defaultdict

import websockets

TRADE_WS = "wss://stream.binance.com:9443/ws/btcusdt@aggTrade"
DEPTH_WS = "wss://stream.binance.com:9443/ws/btcusdt@depth10@100ms"

OUTPUT = "collector_test.csv"


class Collector:
    def __init__(self):
        self.current_sec = int(time.time())
        self.reset()

        self.best_bid = 0
        self.best_ask = 0
        self.bids = []
        self.asks = []

        self._init_csv()

    def reset(self):
        self.trade_count = 0
        self.buy_vol = 0
        self.sell_vol = 0
        self.price_last = 0

    def _init_csv(self):
        with open(OUTPUT, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow([
                "sec", "price_last",
                "trade_count", "buy_vol", "sell_vol",
                "best_bid", "best_ask", "spread",
                "bid_vol_1", "ask_vol_1",
                "bid_vol_5", "ask_vol_5",
                "imbalance_1", "imbalance_5"
            ])

    def write_row(self):
        spread = self.best_ask - self.best_bid

        bid_vol_1 = sum(float(q) for _, q in self.bids[:1])
        ask_vol_1 = sum(float(q) for _, q in self.asks[:1])

        bid_vol_5 = sum(float(q) for _, q in self.bids[:5])
        ask_vol_5 = sum(float(q) for _, q in self.asks[:5])

        def imbalance(b, a):
            if b + a == 0:
                return 0
            return (b - a) / (b + a)

        row = [
            self.current_sec,
            self.price_last,
            self.trade_count,
            self.buy_vol,
            self.sell_vol,
            self.best_bid,
            self.best_ask,
            spread,
            bid_vol_1,
            ask_vol_1,
            bid_vol_5,
            ask_vol_5,
            imbalance(bid_vol_1, ask_vol_1),
            imbalance(bid_vol_5, ask_vol_5),
        ]

        with open(OUTPUT, "a", newline="") as f:
            csv.writer(f).writerow(row)

    def on_trade(self, data):
        price = float(data["p"])
        qty = float(data["q"])
        is_sell = data["m"]

        vol = price * qty

        self.price_last = price
        self.trade_count += 1

        if is_sell:
            self.sell_vol += vol
        else:
            self.buy_vol += vol


    def on_depth(self, data):
        self.bids = data.get("b", data.get("bids", []))
        self.asks = data.get("a", data.get("asks", []))

        if self.bids:
            self.best_bid = float(self.bids[0][0])
        if self.asks:
            self.best_ask = float(self.asks[0][0])


collector = Collector()


async def trade_stream():
    async with websockets.connect(TRADE_WS) as ws:
        async for msg in ws:
            data = json.loads(msg)
            collector.on_trade(data)


async def depth_stream():
    async with websockets.connect(DEPTH_WS) as ws:
        async for msg in ws:
            data = json.loads(msg)
            collector.on_depth(data)


async def aggregator():
    while True:
        await asyncio.sleep(1)

        now = int(time.time())

        if now != collector.current_sec:
            trades = collector.trade_count
            spread = collector.best_ask - collector.best_bid

            collector.write_row()
            collector.reset()
            collector.current_sec = now

            print(f"{now} | trades={trades} | spread={spread:.2f}")


async def main():
    await asyncio.gather(
        trade_stream(),
        depth_stream(),
        aggregator()
    )


if __name__ == "__main__":
    asyncio.run(main())