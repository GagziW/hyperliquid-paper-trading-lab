"""Binance one-second bars as market data for a backtest over a long period.

Your own tape starts on the day when you start the paper trader, and Hyperliquid gives no long
public history of its order book. The free Binance archive (data.binance.vision) has one-second
ETHUSDT spot bars for several years. RESEARCH.md section 3.2 compares the two markets.

A bar is not an order book and not a list of trades. One bar (one second: open, high, low, close,
volume, volume of the buyers) becomes these events:

    + 0.3 s   an order book around the OPEN price
    + 0.6 s   one trade "B" (a buyer took liquidity): the buyers' volume, at the HIGH price
    + 0.6 s   one trade "A" (a seller took liquidity): the sellers' volume, at the LOW price
    + 1.0 s   an order book around the CLOSE price

Limits. Read them before you trust a result:

- The order book is invented: one Hyperliquid tick wide, with the same size at each level. A
  signal that uses the spread or the sizes in the book has no meaning on bars.
- The engine sees a price two times each second. It does not see a target or a stop that the
  price touches and leaves again inside one second.
- Binance spot is another market than the Hyperliquid perpetual, and fills are easier there.
"""

import io
import zipfile
from datetime import date, timedelta
from decimal import Decimal
from functools import lru_cache

import httpx

from pipeline.market import Asset, Book
from pipeline.orders import round_price

URL = "https://data.binance.vision/data/spot/daily/klines/ETHUSDT/1s/ETHUSDT-1s-{day}.zip"
ASSET = Asset(asset_id=1, size_decimals=4)  # the ETH perpetual on Hyperliquid mainnet
LEVEL_SIZE = Decimal("50")  # ETH at each level of the invented book
CENT = Decimal("0.01")
BOOK_OPEN_MS, TRADES_MS, BOOK_CLOSE_MS = 300, 600, 1000


def days(spec):
    """``"2026-11-01..2026-11-07"`` or ``"2026-11-01"`` as a list of dates."""
    first, _, last = spec.partition("..")
    try:
        start, stop = date.fromisoformat(first), date.fromisoformat(last or first)
    except ValueError:
        raise ValueError("Give the days as YYYY-MM-DD..YYYY-MM-DD") from None
    if stop < start:
        raise ValueError("The last day is before the first day")
    return [start + timedelta(days=offset) for offset in range((stop - start).days + 1)]


def load_day(day, cache):
    """The zip file of one day: from the cache directory, or from Binance (then it is cached)."""
    path = cache / f"ETHUSDT-1s-{day}.zip"
    if path.exists():
        return path.read_bytes()
    # Some networks cannot reach Binance. The course data pack has the same files.
    hint = f"If your network cannot reach Binance, put the files of the course data pack into {cache}"
    try:
        response = httpx.get(URL.format(day=day), timeout=120, follow_redirects=True)
    except httpx.HTTPError as exc:
        raise ValueError(f"Binance download failed for {day}: {type(exc).__name__}. {hint}") from None
    if response.status_code == 404:
        raise ValueError(f"Binance has no file for {day}. A day appears in the archive one day later.")
    if response.status_code != 200:
        raise ValueError(f"Binance download failed for {day}: HTTP {response.status_code}. {hint}")
    cache.mkdir(parents=True, exist_ok=True)
    temp = path.with_name(path.name + ".tmp")
    temp.write_bytes(response.content)
    temp.replace(path)
    return response.content


def bars(blob):
    """Yield ``(open_ms, open, high, low, close, volume, buyers_volume)`` for each bar of a zip file."""
    with zipfile.ZipFile(io.BytesIO(blob)) as archive:
        with archive.open(archive.namelist()[0]) as raw:
            for line in io.TextIOWrapper(raw, encoding="ascii"):
                part = line.split(",")
                if not part[0][:1].isdigit():
                    continue  # header row, if any
                open_ms = int(part[0])
                if open_ms > 10**14:
                    open_ms //= 1000  # the files from 2025 use microseconds
                prices = (Decimal(text).quantize(CENT) for text in part[1:5])
                yield (open_ms, *prices, Decimal(part[5]), Decimal(part[9]))


@lru_cache(maxsize=4096)
def levels(price):
    """Invented book levels around ``price``: one Hyperliquid tick wide, five levels each side."""
    tick = Decimal(10) ** max(price.adjusted() - 4, -(6 - ASSET.size_decimals))
    bid = round_price(price, ASSET.size_decimals, True)
    bids = tuple((bid - tick * index, LEVEL_SIZE) for index in range(5))
    asks = tuple((bid + tick * (index + 1), LEVEL_SIZE) for index in range(5))
    return bids, asks


def book_at(price, time_ms):
    return Book(*levels(price), time_ms, time_ms / 1000, time_ms, 1)


def bar_records(rows):
    """Yield the same ``(kind, received_ms, data, waited_ms)`` records as ``pipeline.tape.read_tape``."""
    trade_id = 0
    for open_ms, first, high, low, last, volume, buyers_volume in rows:
        yield "book", open_ms + BOOK_OPEN_MS, book_at(first, open_ms + BOOK_OPEN_MS), 0.0
        time_ms = open_ms + TRADES_MS
        for side, price, size in (("B", high, buyers_volume), ("A", low, volume - buyers_volume)):
            if size > 0:
                trade_id += 1
                trade = {
                    "coin": "ETH",
                    "side": side,
                    "px": str(price),
                    "sz": str(size),
                    "time": time_ms,
                    "tid": trade_id,
                }
                yield "trade", time_ms, trade, 0.0
        yield "book", open_ms + BOOK_CLOSE_MS, book_at(last, open_ms + BOOK_CLOSE_MS), 0.0


def binance_records(day_list, cache, on_day=None):
    """The records of all days. After each day, ``on_day(day, number_of_bars)`` is called."""
    for day in day_list:
        count = 0
        for record in bar_records(bars(load_day(day, cache))):
            count += record[0] == "book"
            yield record
        if on_day:
            on_day(day, count // 2)
