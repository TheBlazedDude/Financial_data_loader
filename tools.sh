#!/bin/sh
# Convenience wrapper inside the container
# Usage examples from HOST:
#   docker exec -it financial-loader /app/tools.sh status
#   docker exec -it financial-loader /app/tools.sh backfill 2020-01-01 2020-01-03 futures BTC/USDT
# If no args, prints help.

set -e

CMD="$1"
case "$CMD" in
  status)
    shift
    exec python -m container_tools --market "${1:-futures}" --pair "${2:-BTC/USDT}" status
    ;;
  health)
    shift
    exec python -m container_tools --market "${1:-futures}" --pair "${2:-BTC/USDT}" health
    ;;
  backfill)
    # backfill START END [market] [pair]
    START="$2"; END="$3"; MARKET="${4:-futures}"; PAIR="${5:-BTC/USDT}"
    exec python -m container_tools --market "$MARKET" --pair "$PAIR" backfill --start "$START" --end "$END"
    ;;
  combine)
    # combine [tz] [market] [pair]
    TZNAME="${2:-}"; MARKET="${3:-futures}"; PAIR="${4:-BTC/USDT}"
    if [ -n "$TZNAME" ]; then
      exec python -m container_tools --market "$MARKET" --pair "$PAIR" combine --tz "$TZNAME"
    else
      exec python -m container_tools --market "$MARKET" --pair "$PAIR" combine
    fi
    ;;
  live)
    # live [minutes] [market] [pair]
    MIN="${2:-2}"; MARKET="${3:-futures}"; PAIR="${4:-BTC/USDT}"
    exec python -m container_tools --market "$MARKET" --pair "$PAIR" live --minutes "$MIN"
    ;;
  *)
    echo "Usage: /app/tools.sh <status|health|backfill|combine|live> ..."
    echo "Examples:"
    echo "  docker exec -it financial-loader /app/tools.sh status"
    echo "  docker exec -it financial-loader /app/tools.sh backfill 2020-01-01 2020-01-03 futures BTC/USDT"
    echo "  docker exec -it financial-loader /app/tools.sh combine Europe/Zurich spot SOL/USDT"
    exit 1
    ;;
 esac
