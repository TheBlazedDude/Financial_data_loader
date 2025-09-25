"""
Helper commands to run inside the container for quick manual testing.
These simply call into the existing loader functions with sensible defaults.

IMPORTANT: Run the following examples from your HOST shell with `docker exec`, not from inside the container shell.
If you see errors like "/bin/sh: 1: docker: not found" or "-it: not found", you are likely typing inside the container.
Exit the container shell (e.g., `exit`) and run the commands on the host.

Global options (apply to all commands):
- --market {spot,futures}
- --pair PAIR (examples: BTC/USDT, ETH/USDT, SOL/USDT; BTCUSDT is also accepted)

Usage from host (examples):
- docker exec -it financial-loader python -m container_tools --market futures --pair BTC/USDT backfill --start 2020-01-01 --end 2020-01-03
- docker exec -it financial-loader python -m container_tools --market futures --pair ETH/USDT live --minutes 3
- docker exec -it financial-loader python -m container_tools --market spot --pair SOL/USDT combine --tz Europe/Zurich
- docker exec -it financial-loader python -m container_tools --market futures --pair BTC/USDT status
- docker exec -it financial-loader python -m container_tools --market spot --pair BTC/USDT health
"""
from __future__ import annotations
import argparse
import sys
import time
from typing import Optional
import os

from binance_usdm_loader import (
    setup_logging,
    backfill as _backfill,
    build_combined_parquet as _combine,
    load_manifest,
    status as _status,
    build_health,
    iso_to_ms,
    utc_now_ms,
)


def cmd_backfill(args: argparse.Namespace) -> int:
    logger = setup_logging()
    start_ms = iso_to_ms(args.start) if args.start else None
    end_ms = iso_to_ms(args.end) if args.end else None
    _backfill(
        logger,
        start_ms=start_ms,
        end_ms=end_ms,
        chunk_size=args.chunk_size,
        align_window=args.align_window,
    )
    return 0


essential_live_minutes_default = 2

def cmd_live(args: argparse.Namespace) -> int:
    logger = setup_logging()
    # For testing, instead of running the infinite live loop, fetch the last N minutes via backfill
    minutes = args.minutes if args.minutes is not None else essential_live_minutes_default
    target_end = utc_now_ms() - 60_000
    start_ms = target_end - minutes * 60_000 + 60_000  # exclusive of end
    if start_ms < 0:
        start_ms = 0
    _backfill(logger, start_ms=int(start_ms), end_ms=int(target_end), chunk_size=args.chunk_size, align_window=False)
    return 0


def cmd_combine(args: argparse.Namespace) -> int:
    logger = setup_logging()
    manifest = load_manifest()
    _combine(manifest, logger, from_index=args.from_index, to_index=args.to_index, tz_name=args.tz)
    return 0


def cmd_status(args: argparse.Namespace) -> int:
    logger = setup_logging()
    _status(logger)
    return 0


def cmd_health(args: argparse.Namespace) -> int:
    logger = setup_logging()
    h = build_health(logger)
    print(h)
    return 0


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="container_tools", description="Helper commands for testing inside the Docker container")
    # Global options applicable to all subcommands
    p.add_argument("--market", choices=["spot", "futures"], default=None, help="Market type to use: spot or futures (default from loader)")
    p.add_argument("--pair", default=None, help="Trading pair, e.g., BTC/USDT or BTCUSDT")
    sub = p.add_subparsers(dest="cmd", required=True)

    p_backfill = sub.add_parser("backfill", help="Run a backfill window")
    p_backfill.add_argument("--start", help="Start ISO8601, e.g. 2020-01-01 or 2020-01-01T00:00:00Z", default=None)
    p_backfill.add_argument("--end", help="End ISO8601", default=None)
    p_backfill.add_argument("--chunk-size", type=int, default=1440)
    p_backfill.add_argument("--align-window", action="store_true")
    p_backfill.set_defaults(func=cmd_backfill)

    p_live = sub.add_parser("live", help="Fetch the last N minutes (testing)")
    p_live.add_argument("--minutes", type=int, default=essential_live_minutes_default, help="How many last minutes to fetch")
    p_live.add_argument("--chunk-size", type=int, default=1440)
    p_live.set_defaults(func=cmd_live)

    p_combine = sub.add_parser("combine", help="Build combined parquet (optionally in a range)")
    p_combine.add_argument("--from-index", type=int, default=None)
    p_combine.add_argument("--to-index", type=int, default=None)
    p_combine.add_argument("--tz", type=str, default=None, help="Timezone name or +HH:MM offset for filename suffix")
    p_combine.set_defaults(func=cmd_combine)

    p_status = sub.add_parser("status", help="Show dataset status")
    p_status.set_defaults(func=cmd_status)

    p_health = sub.add_parser("health", help="Print health dictionary")
    p_health.set_defaults(func=cmd_health)

    return p


def _apply_market_pair(args: argparse.Namespace):
    # Adjust binance_usdm_loader globals to reflect selected market/pair
    if args.market or args.pair:
        import binance_usdm_loader as l
        # Defaults from module if not provided
        market = args.market if args.market else l.MARKET_TYPE
        symbol = args.pair if args.pair else l.SYMBOL
        # Normalize
        symbol = symbol.replace(" ", "").replace("-", "/").upper()
        # Update globals similarly to l.main()
        l.MARKET_TYPE = market
        l.EXCHANGE_ID = "binanceusdm" if market == "futures" else "binance"
        l.SYMBOL = symbol
        l.PAIR_TOKEN = l._symbol_to_pair_token(symbol)
        # Recompute data paths based on market and pair
        l.DEFAULT_DATA_BASE = os.environ.get("MARKET_DATA_DIR", r"C:\\MarketData")
        l.DATA_ROOT = os.path.join(l.DEFAULT_DATA_BASE, l.EXCHANGE_ID, l.PAIR_TOKEN, l.TIMEFRAME)
        l.CHUNKS_DIR = os.path.join(l.DATA_ROOT, "chunks")
        l.COMBINED_DIR = os.path.join(l.DATA_ROOT, "combined")
        l.COMBINED_FILE = os.path.join(l.COMBINED_DIR, f"{l.PAIR_TOKEN}_{l.TIMEFRAME}_all.parquet")
        l.MANIFEST_FILE = os.path.join(l.DATA_ROOT, "manifest.json")
        l.HEALTH_FILE = os.path.join(l.DATA_ROOT, "health.json")
        l.PENDING_DIR = os.path.join(l.DATA_ROOT, "pending")
        l.PENDING_TMP_DIR = os.path.join(l.PENDING_DIR, ".tmp")


def main(argv: Optional[list[str]] = None) -> int:
    argv = list(sys.argv[1:] if argv is None else argv)
    parser = build_parser()
    args = parser.parse_args(argv)
    _apply_market_pair(args)
    return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main())
