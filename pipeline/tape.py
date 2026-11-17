"""The tape: a recording of the market data that the paper trader received.

The paper trader writes gzip files into ``<output>/_tape/``. Each line is one JSON record, in the
order in which the strategies saw the events:

    {"k": "h", "v": 1, "coin": "ETH", "network": "mainnet", "asset": {...}}      start of a recording
    {"k": "b", "r": ..., "t": ..., "g": 1, "b": [[px, sz], ...], "a": [...]}     order book
    {"k": "t", "r": ..., "d": {"coin", "side", "px", "sz", "time", "tid"}}       public trade
    {"k": "g", "r": ..., "d": "feed_disconnected"}                               data gap

``r`` is the time of receipt on this computer and ``t`` is the exchange time, in milliseconds.
A new file starts with each UTC day and with each start of the program. The file name is the
time of its first record, so the names sort by time.

``pipeline/backtest.py`` replays these files through the same engine as the live run.
"""

import glob
import gzip
import json
import zlib
from dataclasses import asdict
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

from pipeline.market import Asset, Book

VERSION = 1
TRADE_KEYS = ("coin", "side", "px", "sz", "time", "tid")


def book_record(book, waited_ms=0.0):
    record = {
        "k": "b",
        "r": book.received_wall_ms,
        "t": book.exchange_ms,
        "g": book.generation,
        "b": [[str(price), str(size)] for price, size in book.bids],
        "a": [[str(price), str(size)] for price, size in book.asks],
    }
    if waited_ms >= 1:  # only an unusual delay between receipt and processing is worth a field
        record["w"] = round(waited_ms, 1)
    return record


def record_book(record):
    """The ``Book`` of a tape record. Its ``age_ms()`` has no meaning in a backtest."""
    received = record["r"]
    return Book(
        tuple((Decimal(price), Decimal(size)) for price, size in record["b"]),
        tuple((Decimal(price), Decimal(size)) for price, size in record["a"]),
        record["t"],
        received / 1000,
        received,
        record["g"],
    )


class TapeWriter:
    def __init__(self, directory, asset, network, coin="ETH"):
        self.directory = Path(directory)
        self.directory.mkdir(parents=True, exist_ok=True)
        self.header = {"k": "h", "v": VERSION, "coin": coin, "network": network, "asset": asdict(asset)}
        self.file = None
        self.day = None

    def write(self, record, received_ms):
        day = datetime.fromtimestamp(received_ms / 1000, timezone.utc).strftime("%Y-%m-%d")
        if self.day is None or day > self.day:
            self.close()
            self.file, self.day = self.create(int(received_ms)), day
            self.line(self.header)
        self.line(record)

    def create(self, stamp_ms):
        # A new file for each start: after a crash only the end of ONE file is damaged, and an
        # existing recording is never overwritten.
        while True:
            name = datetime.fromtimestamp(stamp_ms / 1000, timezone.utc).strftime("%Y-%m-%dT%H%M%S")
            path = self.directory / f"{name}.{stamp_ms % 1000:03d}.jsonl.gz"
            try:
                return gzip.open(path, "xt", encoding="utf-8", newline="\n")
            except FileExistsError:
                stamp_ms += 1

    def line(self, record):
        self.file.write(json.dumps(record, separators=(",", ":"), allow_nan=False) + "\n")

    def book(self, book, waited_ms=0.0):
        self.write(book_record(book, waited_ms), book.received_wall_ms)

    def trade(self, trade, received_ms):
        data = {key: trade[key] for key in TRADE_KEYS if key in trade}
        self.write({"k": "t", "r": received_ms, "d": data}, received_ms)

    def gap(self, reason, received_ms):
        self.write({"k": "g", "r": received_ms, "d": reason}, received_ms)

    def flush(self):
        if self.file:
            self.file.flush()  # the data up to this point stays readable after a crash

    def close(self):
        if self.file:
            self.file.close()
            self.file = None


def tape_files(paths):
    """The tape files of the given files and directories, sorted by time."""
    files = []
    for path in map(Path, paths):
        # PowerShell does not expand "*" itself, so a pattern such as _tape/2026-11-17T* works here.
        matches = [path] if path.exists() else sorted(map(Path, glob.glob(str(path))))
        if not matches:
            raise ValueError(f"No tape file or directory: {path}")
        for match in matches:
            files.extend(sorted(match.glob("*.jsonl.gz")) if match.is_dir() else [match])
    return files


def read_lines(path, damaged=None):
    """The JSON records of one tape file.

    A crash can cut off the end of a file. Such a file is read up to the damage, and its path is
    added to the list ``damaged``.
    """
    try:
        with gzip.open(path, "rt", encoding="utf-8") as file:
            for line in file:
                if line.endswith("\n"):
                    yield json.loads(line)
    except (EOFError, zlib.error, gzip.BadGzipFile):
        if damaged is not None:
            damaged.append(path)


def tape_header(files):
    """The header of the first tape file: which coin, and its size decimals on the exchange."""
    for path in files:
        for record in read_lines(path):
            if record.get("k") == "h":
                check_header(record, path)
                return record
            break
    raise ValueError("No readable tape file. The paper trader records the tape while it runs.")


def check_header(record, path):
    if record.get("v") != VERSION:
        raise ValueError(f"{path}: unsupported tape version {record.get('v')!r}")


def header_asset(header):
    return Asset(**header["asset"])


def read_tape(files, on_file=None):
    """Yield ``(kind, received_ms, data, waited_ms)`` for each record of the tape files.

    ``kind`` is "book" (``data`` is a ``Book``), "trade" (the trade dict) or "gap" (the reason).
    After each file, ``on_file(path, number_of_events, cut_off)`` is called.
    """
    first = None
    for path in files:
        count, damaged = 0, []
        for record in read_lines(path, damaged):
            kind = record["k"]
            if kind == "b":
                count += 1
                yield "book", record["r"], record_book(record), record.get("w", 0.0)
            elif kind == "t":
                count += 1
                yield "trade", record["r"], record["d"], 0.0
            elif kind == "g":
                yield "gap", record["r"], record["d"], 0.0
            elif kind == "h":
                check_header(record, path)
                identity = (record["coin"], record["network"], record["asset"])
                first = first or identity
                if identity != first:
                    raise ValueError(f"{path}: this tape is from another market than the first tape file")
        if on_file:
            on_file(path, count, bool(damaged))
