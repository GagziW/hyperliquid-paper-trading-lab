import argparse
import asyncio
import gzip
import io
import json
import sys
import time
import zipfile
from dataclasses import replace
from datetime import date
from decimal import Decimal

import httpx
import pytest
from test_paper import BASE, Scripted, book, mock_metadata

from pipeline import backtest, bars, paper
from pipeline.market import Asset
from pipeline.paper_engine import Entry, PaperEngine, SimulationConfig
from pipeline.session import Session
from pipeline.strategies import Strategy, load_strategy
from pipeline.strategies.spike_fade import Params, SpikeFade
from pipeline.tape import TapeWriter, read_tape, tape_files, tape_header

ASSET = Asset(1, 4)
TRADE = {"coin": "ETH", "side": "B", "px": "101", "sz": "1", "time": BASE + 2300, "tid": 7}


def spike_records():
    """A rise, a fill of the short entry and a fall to the profit target, as backtest records."""
    yield "gap", BASE - 10, "feed_connected", 0.0
    for seconds, mid in ((0, "100"), (1, "100"), (2, "100.60"), (2.275, "100.60")):
        yield "book", BASE + int(seconds * 1000), book(seconds, mid), 0.0
    yield "trade", BASE + 2350, TRADE, 0.0
    for seconds in (3, 3.4):
        yield "book", BASE + int(seconds * 1000), book(seconds, "100.30"), 0.0


def strip(trades):
    return [{k: v for k, v in trade.items() if k != "trade_id"} for trade in trades]


class Recorder:
    """An engine stand-in that records what a Session gives to it."""

    def __init__(self):
        self.calls = []

    def on_book(self, book):
        self.calls.append("book")

    def on_trade(self, trade, received_ms):
        self.calls.append("trade")

    def gap(self, now_ms, reason):
        self.calls.append(reason)

    def tick(self, now_ms):
        pass


# ── Session: the freshness rules that live and backtest share ───────────────────────────


def test_session_reports_a_stale_book_once_and_drops_trades_until_a_fresh_book():
    engine = Recorder()
    session = Session(SimulationConfig(), [engine])
    session.on_book(book(0), BASE, waited_ms=0)
    session.on_trade(TRADE, BASE + 10, book_age_ms=10)
    old = replace(book(1), exchange_ms=BASE - 5000)  # the exchange time is 6 s before the receipt
    session.on_book(old, BASE + 1000)
    session.on_book(old, BASE + 1100)
    session.on_trade(TRADE, BASE + 1200, book_age_ms=100)
    session.on_book(book(2), BASE + 2000, waited_ms=2000)  # waited too long in the local queue
    session.on_book(book(3), BASE + 3000)
    session.on_trade(TRADE, BASE + 3010, book_age_ms=10)
    assert engine.calls == ["book", "trade", "stale_book_or_clock_skew", "book", "trade"]


def test_session_rejects_a_book_from_the_future_and_trades_without_a_book():
    engine = Recorder()
    session = Session(SimulationConfig(), [engine])
    session.on_trade(TRADE, BASE, book_age_ms=None)
    session.on_trade(TRADE, BASE, book_age_ms=1600)
    session.on_book(replace(book(0), exchange_ms=BASE + 300), BASE)
    assert engine.calls == ["stale_book_or_clock_skew"]


def test_session_reports_a_quiet_feed_once():
    engine = Recorder()
    session = Session(SimulationConfig(), [engine])
    session.tick(BASE, None)
    session.tick(BASE, 1500)
    session.tick(BASE + 200, 1700)
    session.tick(BASE + 400, 1900)
    assert engine.calls == ["stale_local_book"]


# ── Tape: write and read the market recording ───────────────────────────────────────────


def test_tape_gives_back_the_same_books_trades_and_gaps(tmp_path):
    tape = TapeWriter(tmp_path, ASSET, "mainnet")
    first = replace(book(0, "100.5"), received_wall_ms=BASE + 0.4375)
    tape.gap("feed_connected", BASE - 5)
    tape.book(first)
    tape.book(book(1), waited_ms=1700.04)
    tape.trade({**TRADE, "hash": "0xabc", "users": ["0x1", "0x2"]}, BASE + 2350)
    tape.close()
    files = tape_files([tmp_path])
    assert len(files) == 1
    assert tape_header(files)["asset"] == {"asset_id": 1, "size_decimals": 4}
    records = list(read_tape(files))
    assert [r[0] for r in records] == ["gap", "book", "book", "trade"]
    assert records[0][1:3] == (BASE - 5, "feed_connected")
    restored = records[1][2]
    assert (restored.bids, restored.asks) == (first.bids, first.asks)
    assert (restored.exchange_ms, restored.received_wall_ms, restored.generation) == (BASE, BASE + 0.4375, 1)
    assert records[2][3] == 1700.0  # an unusual wait is kept, so the backtest judges this book the same
    assert records[3][2] == TRADE  # only the fields that a strategy needs; no user addresses


def test_tape_starts_a_new_file_each_day_and_never_overwrites(tmp_path):
    day = 86_400_000
    midnight = (BASE // day + 1) * day
    for _ in range(2):  # two starts of the program at the same millisecond
        tape = TapeWriter(tmp_path, ASSET, "mainnet")
        tape.gap("feed_connected", midnight - 1000)
        tape.gap("feed_connected", midnight + 1000)
        tape.gap("late_record_of_the_old_day", midnight - 1)  # does not go back to the old file
        tape.close()
    files = tape_files([tmp_path])
    assert len(files) == 4
    assert [f.name for f in files] == sorted(f.name for f in files)
    assert sum(1 for _ in read_tape(files)) == 6
    new_day = f"{files[-1].name[:10]}T*"  # a pattern that the shell did not expand
    assert tape_files([tmp_path / new_day]) == files[2:]


def test_a_tape_that_a_crash_cut_off_is_read_up_to_the_damage(tmp_path):
    tape = TapeWriter(tmp_path, ASSET, "mainnet")
    for index in range(200):
        tape.book(book(index, 100 + index % 7))
    tape.flush()
    for index in range(200, 400):
        tape.book(book(index))
    tape.close()
    path = tape_files([tmp_path])[0]
    path.write_bytes(path.read_bytes()[:-40])
    seen = []
    records = list(read_tape([path], on_file=lambda p, count, cut_off: seen.append((count, cut_off))))
    assert 200 <= len(records) < 400
    assert seen == [(len(records), True)]


def test_tapes_of_two_markets_do_not_mix(tmp_path):
    for name, asset in (("a", ASSET), ("b", Asset(0, 5))):
        tape = TapeWriter(tmp_path / name, asset, "mainnet")
        tape.gap("feed_connected", BASE)
        tape.close()
    with pytest.raises(ValueError, match="another market"):
        list(read_tape(tape_files([tmp_path / "a", tmp_path / "b"])))
    with pytest.raises(ValueError, match="No tape file"):
        tape_files([tmp_path / "missing"])
    (tmp_path / "old.jsonl.gz").write_bytes(gzip.compress(b'{"k":"h","v":0}\n'))
    with pytest.raises(ValueError, match="version"):
        tape_header([tmp_path / "old.jsonl.gz"])


# ── Replay: the backtest gives the same result as the engine and as the live run ────────


def test_replay_gives_the_same_trade_as_direct_engine_calls():
    direct = PaperEngine(SimulationConfig(), SpikeFade(Params(lookback_s=2)), ASSET)
    for kind, received_ms, data, _ in spike_records():
        if kind == "book":
            direct.on_book(data)
        elif kind == "trade":
            direct.on_trade(data, received_ms)
    assert len(direct.trades) == 1
    events = []
    engines, last_ms = backtest.replay(
        spike_records(),
        SimulationConfig(),
        [SpikeFade(Params(lookback_s=2)), load_strategy("my_strategy")],
        ASSET,
        on_events=lambda engine, batch: events.extend((engine.strategy.name, e["event"]) for e in batch),
    )
    assert strip(engines[0].trades) == strip(direct.trades)
    assert Decimal(engines[0].trades[0]["pnl_after_fees_usdc"]) > 0
    assert engines[1].trades == [] and engines[1].books_observed == engines[0].books_observed == 6
    assert last_ms == BASE + 3400
    assert ("spike_fade", "position_closed") in events and ("my_strategy", "position_closed") not in events


def test_replay_is_deterministic():
    runs = [
        backtest.replay(spike_records(), SimulationConfig(), [SpikeFade(Params(lookback_s=2))], ASSET)[0][0]
        for _ in range(2)
    ]
    assert strip(runs[0].trades) == strip(runs[1].trades)
    assert runs[0].summary(BASE)["paper_balance_usdc"] == runs[1].summary(BASE)["paper_balance_usdc"]


def test_replay_runs_the_timers_between_two_records():
    # A pause of 30 s after the fill: the live loop reports the quiet feed after about 1.5 s and
    # requests a risk exit. The backtest must do this at the same time, not 30 s later.
    records = [r for r in spike_records() if r[1] <= BASE + 2350]
    records.append(("book", BASE + 32000, book(32, "100.60"), 0.0))
    events = []
    engines, _ = backtest.replay(
        records,
        SimulationConfig(),
        [SpikeFade(Params(lookback_s=2))],
        ASSET,
        on_events=lambda engine, batch: events.extend(batch),
    )
    gaps = [e for e in events if e["event"] == "signal_history_reset"]
    assert [e["reason"] for e in gaps] == ["feed_connected", "stale_local_book"]
    assert BASE + 2275 + 1500 < gaps[1]["time_ms"] <= BASE + 2275 + 1500 + backtest.TICK_MS
    assert engines[0].trades[0]["exit_reason"] == "data_gap"


def test_replay_rejects_data_that_goes_back_in_time():
    records = [*spike_records(), ("book", BASE - 3_600_000, book(-3600), 0.0)]
    with pytest.raises(ValueError, match="back in time"):
        backtest.replay(records, SimulationConfig(), [SpikeFade()], ASSET)


def test_a_feed_reconnect_drops_trades_until_the_next_book(monkeypatch):
    engine = Recorder()

    class OneEngine(Session):
        def __init__(self, simulation, engines):
            super().__init__(simulation, [engine])

    records = [
        ("book", BASE, book(0), 0.0),
        ("gap", BASE + 100, "feed_disconnected", 0.0),
        ("trade", BASE + 200, TRADE, 0.0),
        ("book", BASE + 300, book(0.3), 0.0),
        ("trade", BASE + 400, TRADE, 0.0),
    ]
    monkeypatch.setattr(backtest, "Session", OneEngine)
    backtest.replay(records, SimulationConfig(), [SpikeFade()], ASSET)
    assert engine.calls == ["book", "feed_disconnected", "book", "trade"]


def test_backtest_of_the_live_tape_gives_the_trades_of_the_live_run(tmp_path, monkeypatch):
    mock_metadata(monkeypatch)

    class FakeFeed:
        def __init__(self, settings, emit, on_book, on_trade):
            self.emit, self.on_book, self.on_trade = emit, on_book, on_trade
            self.book = None

        async def run(self):
            self.emit({"event": "feed_connected"})
            for index, mid in enumerate(("100", "100", "100.6", "100.3", "100.3")):
                await asyncio.sleep(0.025)
                now = time.time() * 1000
                self.book = replace(
                    book(index, mid),
                    exchange_ms=int(now),
                    received_wall_ms=now,
                    received_mono=time.monotonic(),
                )
                self.on_book(self.book)
                if index == 2:
                    await asyncio.sleep(0.005)
                    self.on_trade(
                        {"coin": "ETH", "side": "B", "px": "101", "sz": "1", "time": int(time.time() * 1000)}
                    )
            await asyncio.sleep(10)

    monkeypatch.setattr(paper, "Feed", FakeFeed)
    simulation = SimulationConfig(latency_ms=0)

    def strategies():
        return [SpikeFade(Params(lookback_s=0.03)), load_strategy("my_strategy")]

    args = argparse.Namespace(output=tmp_path, duration=0.18)
    assert asyncio.run(paper.run(simulation, strategies(), args)) == 0
    live = json.loads(next((tmp_path / "spike_fade").glob("*/state.json")).read_text())
    assert len(live["trades"]) == 1

    files = tape_files([tmp_path / "_tape"])
    assert tape_header(files)["network"] == "mainnet"
    engines, _ = backtest.replay(read_tape(files), simulation, strategies(), ASSET)
    assert strip(engines[0].trades) == strip(live["trades"])
    assert engines[0].books_observed == live["books_observed"] == 5
    assert engines[0].entries_requested == live["entries_requested"]
    assert engines[1].trades == []


def test_the_live_run_writes_no_tape_when_it_is_switched_off(tmp_path, monkeypatch):
    mock_metadata(monkeypatch)

    class QuietFeed:
        def __init__(self, settings, emit, on_book, on_trade):
            self.book = None

        async def run(self):
            await asyncio.sleep(10)

    monkeypatch.setattr(paper, "Feed", QuietFeed)
    args = argparse.Namespace(output=tmp_path, duration=0.05, no_tape=True)
    assert asyncio.run(paper.run(SimulationConfig(), [SpikeFade()], args)) == 0
    assert not (tmp_path / "_tape").exists()


# ── Binance one-second bars ─────────────────────────────────────────────────────────────


def bar_zip(rows):
    blob = io.BytesIO()
    with zipfile.ZipFile(blob, "w") as archive:
        archive.writestr("ETHUSDT-1s.csv", "".join(",".join(map(str, row)) + "\n" for row in rows))
    return blob.getvalue()


def bar(second, first, high, low, last, volume="2", buyers="1", micro=False):
    open_ms = BASE + second * 1000
    stamp = open_ms * 1000 if micro else open_ms  # the files from 2025 use microseconds
    return (stamp, first, high, low, last, volume, open_ms + 999, "0", 3, buyers)


def test_one_bar_becomes_two_books_and_two_trades():
    rows = [
        bar(0, "2487.69000000", "2487.80", "2487.60", "2487.70", micro=True),
        bar(1, "2487.7", "2487.7", "2487.7", "2487.7", "0", "0"),
    ]
    records = list(bars.bar_records(bars.bars(bar_zip(rows))))
    assert [(r[0], r[1] - BASE) for r in records] == [
        ("book", 300),
        ("trade", 600),
        ("trade", 600),
        ("book", 1000),
        ("book", 1300),  # the second bar has no volume: no trades
        ("book", 2000),
    ]
    first = records[0][2]
    assert (first.bid, first.ask) == (Decimal("2487.6"), Decimal("2487.7"))  # one tick wide, price inside
    assert (
        len(first.bids) == len(first.asks) == 5 and first.exchange_ms == first.received_wall_ms == BASE + 300
    )
    buyers, sellers = records[1][2], records[2][2]
    assert (buyers["side"], buyers["px"], buyers["sz"]) == ("B", "2487.80", "1")
    assert (sellers["side"], sellers["px"], sellers["sz"]) == ("A", "2487.60", "1")
    assert records[3][2].bid == Decimal("2487.7")


def test_bars_fill_a_passive_entry_only_when_the_price_trades_through_it():
    def run(low_of_the_next_bar):
        rows = [
            bar(0, "100.00", "100.00", "100.00", "100.00"),
            bar(1, "100.00", "100.00", low_of_the_next_bar, "100.00"),
            bar(2, "100.00", "100.00", "100.00", "100.00"),
        ]
        request = Entry(side="long", max_holding_s=60)
        engines, _ = backtest.replay(
            bars.bar_records(bars.bars(bar_zip(rows))), SimulationConfig(), [Scripted(request)], bars.ASSET
        )
        return engines[0]

    assert run("100.00").position is None  # a touch of the bid at 100 is not a fill
    filled = run("99.99").position
    assert filled["side"] == "long" and filled["entry_price"] == Decimal("100")


def test_days_and_download_errors(tmp_path, monkeypatch):
    assert bars.days("2026-11-01") == [date(2026, 11, 1)]
    assert len(bars.days("2026-10-30..2026-11-02")) == 4
    for wrong in ("yesterday", "2026-11-02..2026-11-01"):
        with pytest.raises(ValueError):
            bars.days(wrong)
    codes = iter((404, 500))
    monkeypatch.setattr(bars.httpx, "get", lambda url, **kwargs: httpx.Response(next(codes)))
    with pytest.raises(ValueError, match="one day later"):
        bars.load_day(date(2026, 11, 1), tmp_path)
    with pytest.raises(ValueError, match="HTTP 500.*data pack"):
        bars.load_day(date(2026, 11, 1), tmp_path)
    assert not list(tmp_path.iterdir())


# ── Command line ────────────────────────────────────────────────────────────────────────


def command(tmp_path, monkeypatch, *options):
    """Run ``python -m pipeline.backtest`` with a small paper.toml and the given options."""
    config = tmp_path / "paper.toml"
    config.write_text('run = ["spike_fade", "my_strategy"]\n[strategies.spike_fade]\nlookback_s = 2.0\n')
    monkeypatch.setattr(sys, "argv", ["backtest", "--config", str(config), *map(str, options)])
    with pytest.raises(SystemExit) as stop:
        backtest.main()
    assert stop.value.code == 0


def test_command_backtests_a_tape_and_writes_results_like_a_live_run(tmp_path, monkeypatch, capsys):
    tape = TapeWriter(tmp_path / "_tape", ASSET, "mainnet")
    for kind, received_ms, data, _ in spike_records():
        if kind == "book":
            tape.book(data)
        elif kind == "trade":
            tape.trade(data, received_ms)
        else:
            tape.gap(data, received_ms)
    tape.close()
    output = tmp_path / "out"
    command(tmp_path, monkeypatch, "--tape", tmp_path / "_tape", "--output", output)
    status = json.loads((output / "spike_fade" / "status.json").read_text())
    assert status["mode"] == "backtest" and status["running"] is False
    assert status["data"]["source"] == "tape" and status["data"]["events"] == 8
    assert status["closed_trades"] == 1 and status["orders_submitted"] == 0
    directory = next(p for p in (output / "spike_fade").iterdir() if p.is_dir())
    assert directory.name == paper.config_hash(SimulationConfig(), SpikeFade(Params(lookback_s=2.0)))
    assert (directory / "trades.csv").read_text().count("take_profit") == 1
    assert "position_closed" in (directory / "events.jsonl").read_text()
    assert not (directory / "state.json").exists()  # a backtest always starts with a new account
    printed = capsys.readouterr().out
    assert "backtest (tape)" in printed and "my_strategy" in printed


def test_command_backtests_binance_bars_from_the_cache(tmp_path, monkeypatch, capsys):
    rows = [bar(second, "100", "100", "100", "100") for second in range(5)]
    (tmp_path / "cache").mkdir()
    (tmp_path / "cache" / "ETHUSDT-1s-2026-10-10.zip").write_bytes(bar_zip(rows))
    monkeypatch.setattr(bars.httpx, "get", lambda *args, **kwargs: pytest.fail("the cache has this day"))
    places = ("--cache", tmp_path / "cache", "--output", tmp_path / "out")
    command(tmp_path, monkeypatch, "--binance", "2026-10-10", "--strategy", "spike_fade", *places)
    status = json.loads((tmp_path / "out" / "spike_fade" / "status.json").read_text())
    assert status["data"]["source"] == "binance_1s_bars" and status["books_observed"] == 10
    printed = capsys.readouterr().out
    assert "backtest (Binance bars)" in printed and "my_strategy" not in printed


def test_command_explains_a_missing_tape_and_skips_strategies_with_a_model(tmp_path, monkeypatch, capsys):
    class Model(Strategy):
        async def background(self, feed, client):
            return None

    monkeypatch.setattr(backtest, "load_config", lambda path, selected: (SimulationConfig(), [Model()]))
    args = argparse.Namespace(
        config=None, strategy=None, tape=None, binance=None, output=tmp_path, cache=tmp_path
    )
    with pytest.raises(ValueError, match="No strategy"):
        backtest.run(args, tmp_path / "_tape")
    assert "background task" in capsys.readouterr().out
    monkeypatch.setattr(backtest, "load_config", lambda path, selected: (SimulationConfig(), [SpikeFade()]))
    with pytest.raises(ValueError, match="--binance"):
        backtest.run(args, tmp_path / "_tape")
