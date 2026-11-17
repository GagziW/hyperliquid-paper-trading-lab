"""Compare the strategies of one paper run or of one backtest side by side.

Usage:  uv run python -m pipeline.compare            (reads runs/paper/*/status.json)
        uv run python -m pipeline.compare runs/backtest
"""

import argparse
import json
from decimal import Decimal
from pathlib import Path


def run_type(status):
    if status.get("mode") != "backtest":
        return "live paper"
    bars = (status.get("data") or {}).get("source") == "binance_1s_bars"
    return "backtest (Binance bars)" if bars else "backtest (tape)"


ROWS = [
    ("run type", run_type),
    ("simulation settings", lambda s: "standard" if s.get("simulation_standard") else "NOT COMPARABLE"),
    ("running", lambda s: "yes" if s.get("running") else "no"),
    ("observed hours", lambda s: f"{s['observed_s'] / 3600:.1f}" if s.get("observed_s") else "-"),
    ("order books observed", lambda s: s.get("books_observed", "-")),
    ("data gaps", lambda s: s.get("history_resets", "-")),
    ("entries requested", lambda s: s.get("entries_requested", "-")),
    ("entries rejected", lambda s: s.get("entries_rejected", "-")),
    ("entries expired", lambda s: s.get("entries_expired", "-")),
    ("closed trades", lambda s: s.get("closed_trades", "-")),
    ("win rate", lambda s: f"{s['win_rate'] * 100:.1f} %" if s.get("win_rate") is not None else "-"),
    ("P&L after fees (USDC)", lambda s: number(s.get("closed_pnl_after_fees_usdc"), 4)),
    ("mean net per trade (bp)", lambda s: number(s.get("mean_net_bps_per_trade"), 2)),
    ("max drawdown (USDC)", lambda s: number(s.get("max_observed_drawdown_usdc"), 4)),
    ("model calls", lambda s: s.get("model_calls", 0)),
    ("error", lambda s: s.get("error") or "-"),
]


def number(value, places):
    return "-" if value is None else f"{Decimal(str(value)):.{places}f}"


def table(output, names=None):
    """The comparison as text, or None without results. ``names`` selects strategies."""
    statuses = {p.parent.name: json.loads(p.read_text()) for p in sorted(output.glob("*/status.json"))}
    if names is not None:
        statuses = {name: statuses[name] for name in names if name in statuses}
    if not statuses:
        return None
    names = list(statuses)
    if "spike_fade" in names:  # the benchmark first
        names.insert(0, names.pop(names.index("spike_fade")))
    rows = [["", *names]] + [[label, *(str(f(statuses[n])) for n in names)] for label, f in ROWS]
    widths = [max(len(row[i]) for row in rows) for i in range(len(rows[0]))]
    lines = ["  ".join(cell.ljust(width) for cell, width in zip(row, widths, strict=True)) for row in rows]
    lines.insert(1, "  ".join("-" * width for width in widths))
    lines.append("\nFunding is not modelled. Paper results do not show live performance.")
    return "\n".join(lines)


def main():
    root = Path(__file__).resolve().parent.parent
    parser = argparse.ArgumentParser(description="Compare paper strategies of one run")
    parser.add_argument("output", nargs="?", type=Path, default=root / "runs" / "paper")
    args = parser.parse_args()
    text = table(args.output)
    if text is None:
        parser.error(f"No */status.json in {args.output}")
    print(text)


if __name__ == "__main__":
    main()
