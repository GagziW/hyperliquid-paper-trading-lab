"""Backtest: run your strategies on RECORDED market data, with the same engine as the live run.

    uv run python -m pipeline.backtest --binance 2026-11-01..2026-11-07
    uv run python -m pipeline.backtest
    uv run python -m pipeline.backtest --tape runs/paper/_tape/2026-11-17T* --strategy my_strategy

``--binance`` uses Binance one-second bars (see ``pipeline/bars.py`` for their limits). Without
it, the backtest replays your own tape: the market data that the paper trader recorded into
``runs/paper/_tape`` (see ``pipeline/tape.py``).

The strategies and their parameters come from paper.toml, as in a live run. The results are in
``runs/backtest/<strategy>/`` in the format of a live run, and ``pipeline.compare`` can read them.

A backtest does not replace the 24-hour live run. You can run a backtest many times and keep the
best parameters; then the result looks better than the strategy is. The live run comes after
that choice, on data that nobody has seen, and for this reason it is the honest test.
"""

import argparse
import json
import time
from datetime import datetime, timezone
from pathlib import Path

from pipeline import bars, compare
from pipeline.paper import config_hash, load_config, write_json, write_trades
from pipeline.paper_engine import PaperEngine
from pipeline.session import Session
from pipeline.strategies import has_background
from pipeline.tape import header_asset, read_tape, tape_files, tape_header

TICK_MS = 200  # the live loop wakes up at least this often to check its timers
BACKWARDS_MS = 60_000  # a larger step back in time means that two data files overlap
FEED_GAPS = {"feed_connected", "feed_disconnected"}  # after these the feed has no book


def replay(records, simulation, strategies, asset, on_events=None):
    """Give market records to one engine for each strategy, as the live loop does.

    ``records`` are ``(kind, received_ms, data, waited_ms)`` tuples from ``pipeline.tape.read_tape``
    or ``pipeline.bars.bar_records``. Time comes from the records only, never from the clock of
    this computer. Returns the engines and the time of the last record.
    """
    engines = [PaperEngine(simulation, strategy, asset) for strategy in strategies]
    session = Session(simulation, engines)
    quiet_ms = simulation.max_book_age_ms + 2 * TICK_MS
    clock = book_ms = None

    def book_age_ms(now_ms):
        return None if book_ms is None else now_ms - book_ms

    for kind, received_ms, data, waited_ms in records:
        if clock is None:
            clock = received_ms
        elif received_ms < clock - BACKWARDS_MS:
            raise ValueError("The data goes back in time. Do two files cover the same period?")
        else:
            # Between two events the live loop wakes up each 0.2 s for its timers. Do the same,
            # but only while a timer can still change something.
            tick_ms, stop_ms = clock + TICK_MS, min(received_ms, clock + quiet_ms)
            while tick_ms < stop_ms:
                session.tick(tick_ms, book_age_ms(tick_ms))
                tick_ms += TICK_MS
            clock = max(clock, received_ms)
        if kind == "book":
            book_ms = received_ms
            session.on_book(data, received_ms + waited_ms, waited_ms)
        elif kind == "trade":
            session.on_trade(data, received_ms, book_age_ms(received_ms))
        elif kind == "gap":
            if data in FEED_GAPS:
                book_ms = None
            session.gap(received_ms, data)
        else:
            raise ValueError(f"Unknown record kind {kind!r}")
        session.tick(clock, book_age_ms(clock))
        for engine in engines:
            if engine.events:
                events = engine.drain_events()
                if on_events:
                    on_events(engine, events)
    return engines, clock


def utc(ms):
    return datetime.fromtimestamp(ms / 1000, timezone.utc).isoformat(timespec="seconds")


def run(args, default_tape):
    simulation, strategies = load_config(args.config, args.strategy)
    for strategy in strategies:
        if has_background(strategy):
            print(
                f"Skipped {strategy.name}: it has a background task (for example an AI model). "
                "A backtest cannot ask a model again for each moment of the past."
            )
    strategies = [strategy for strategy in strategies if not has_background(strategy)]
    if not strategies:
        raise ValueError("No strategy to backtest")
    if args.binance and args.tape:
        raise ValueError("Use --tape or --binance, not both")

    counts = {"events": 0}
    if args.binance:
        day_list = bars.days(args.binance)
        asset = bars.ASSET
        data = {
            "source": "binance_1s_bars",
            "first_day": str(day_list[0]),
            "last_day": str(day_list[-1]),
            "note": "Invented order book and two trades for each second. See pipeline/bars.py.",
        }

        def on_day(day, count):
            print(f"  {day}  {count:,} bars", flush=True)

        records = bars.binance_records(day_list, args.cache, on_day)
    else:
        if not args.tape and not default_tape.is_dir():
            raise ValueError(
                f"No tape in {default_tape}. The paper trader records it while it runs. "
                "To backtest without a tape, use --binance YYYY-MM-DD..YYYY-MM-DD"
            )
        files = tape_files(args.tape or [default_tape])
        header = tape_header(files)
        asset = header_asset(header)
        data = {
            "source": "tape",
            "network": header["network"],
            "files": len(files),
            "first_file": files[0].name,
            "last_file": files[-1].name,
        }

        def on_file(path, count, cut_off):
            note = "  (the end of this file is damaged; read up to the damage)" if cut_off else ""
            print(f"  {path.name}  {count:,} events{note}", flush=True)

        records = read_tape(files, on_file)

    def counted(records):
        for record in records:
            counts["events"] += 1
            counts.setdefault("first_ms", record[1])
            yield record

    if not simulation.is_standard():
        print("Warning: the [simulation] settings differ from the lab values. NOT comparable.")
    directories = {s.name: args.output / s.name / config_hash(simulation, s) for s in strategies}
    logs = {}
    started = time.monotonic()
    try:
        for name, directory in directories.items():
            directory.mkdir(parents=True, exist_ok=True)
            logs[name] = (directory / "events.jsonl").open("w")

        def on_events(engine, events):
            file = logs[engine.strategy.name]
            for event in events:
                file.write(json.dumps(event, allow_nan=False) + "\n")

        engines, last_ms = replay(counted(records), simulation, strategies, asset, on_events)
    finally:
        for file in logs.values():
            file.close()
    if last_ms is None:
        raise ValueError("The data has no events")

    first_utc, last_utc = utc(counts["first_ms"]), utc(last_ms)
    data = {**data, "first_utc": first_utc, "last_utc": last_utc, "events": counts["events"]}
    for engine in engines:
        directory = directories[engine.strategy.name]
        summary = {
            **engine.summary(last_ms),
            "mode": "backtest",
            "data": data,
            "running": False,
            "updated_utc": datetime.now(timezone.utc).isoformat(),
            "output_directory": str(directory),
            "error": None,
        }
        write_trades(directory / "trades.csv", engine.trades)
        write_json(directory / "summary.json", summary)
        write_json(directory.parent / "status.json", summary)
    elapsed = time.monotonic() - started
    print(f"\n{counts['events']:,} events from {first_utc} to {last_utc} in {elapsed:.1f} s\n")
    print(compare.table(args.output, [strategy.name for strategy in strategies]))
    print(f"\nTrades and events of each strategy: {args.output}/<strategy>/<config-hash>/")
    return 0


def main():
    root = Path(__file__).resolve().parent.parent
    parser = argparse.ArgumentParser(
        description="Backtest the paper strategies of paper.toml on recorded data. No exchange access."
    )
    parser.add_argument("--config", type=Path, default=root / "paper.toml")
    parser.add_argument(
        "--strategy",
        action="append",
        help="Backtest only this strategy (repeat the option for more). Default: run = [...] in paper.toml",
    )
    parser.add_argument(
        "--tape", type=Path, nargs="+", help="Tape files or directories. Default: runs/paper/_tape"
    )
    parser.add_argument(
        "--binance",
        metavar="DAY..DAY",
        help="Use Binance 1 s bars and not a tape, for example 2026-11-01..2026-11-07",
    )
    parser.add_argument("--output", type=Path, default=root / "runs" / "backtest")
    parser.add_argument(
        "--cache", type=Path, default=root / "runs" / "binance", help="Directory for the Binance files"
    )
    args = parser.parse_args()
    try:
        code = run(args, root / "runs" / "paper" / "_tape")
    except (ValueError, OSError, TypeError) as exc:
        parser.error(str(exc))
    raise SystemExit(code)


if __name__ == "__main__":
    main()
