Financial Data Loader (Binance Spot or USDM Futures, 1m)

Overview
- Loads 1-minute klines (12 fields) from Binance Spot or Binance USDM Futures via ccxt raw API.
- Supports any trading pair (default BTC/USDT). If the requested pair is unavailable on the selected market, it automatically falls back to BTC/USDT.
- Historical backfill starting from 2020-01-01 01:00 UTC by default.
- Stores data as chunked Parquet files with Zstandard compression (default 1,440 rows per chunk = one day of minutes by default); the chunks directory is the dataset.
- Maintains a manifest (manifest.json) tracking chunk metadata to guarantee continuity, integrity (sha256), and resume capability.
- Live mode keeps the dataset updated every minute with only closed klines.
- Memory-safe: at most a single active chunk is in memory; all I/O is streamed to disk.

Key Paths
- Data root: data\<exchange_id>\<PAIR_TOKEN>\1m\ (e.g., data\binanceusdm\BTCUSDT\1m\ or data\binance\BTCUSDT\1m\)
  - chunks\chunk_000001_...parquet, chunk_000002_...parquet, ...
  - manifest.json
  - combined\<PAIR_TOKEN>_1m_all.parquet (e.g., combined\BTCUSDT_1m_all.parquet)
- Logs: logs\loader.log (rotating)

Rate Limits and Stability
- Uses ccxt with enableRateLimit to comply with Binance limits (each request up to 1500 klines):
  - Spot: ~6000 weights/min
  - USDM Futures: ~2400 weights/min
- Additional retry with exponential backoff on transient errors.
- Continuity checks between pages and between chunks, plus manifest verification command.

Installation
1) Create/activate a virtual environment (optional but recommended)
   PowerShell example:
   `py -m venv .venv`
   `.\\.venv\\Scripts\\Activate.ps1`

2) Install dependencies:
   `pip install -r requirements.txt`

Usage
Show CLI help:
`py binance_usdm_loader.py --help`

If "py" is not recognized on your system, use one of the following instead:
- `python binance_usdm_loader.py --help`
- `python -m binance_usdm_loader --help`
- PowerShell venv (example): `.\\.venv\\Scripts\\python.exe binance_usdm_loader.py --help`

Select market and pair (examples):
- Spot BTC/USDT backfill: py binance_usdm_loader.py --market spot --pair BTC/USDT backfill
- Futures BTC/USDT backfill: py binance_usdm_loader.py --market futures --pair BTC/USDT backfill
- Custom pair on spot with time window: py binance_usdm_loader.py --market spot --pair ETH/USDT backfill --start 2021-01-01T00:00:00Z --end 2021-02-01T00:00:00Z

Backfill historical data with defaults (market=futures, pair=BTC/USDT):
`py binance_usdm_loader.py backfill`

Backfill with custom time window and chunk size:
`py binance_usdm_loader.py --market futures --pair BTC/USDT --align-tz Europe/Zurich backfill --start 2020-01-01T01:00:00Z --end 2020-02-01T00:00:00Z --chunk-size 1440`
- Alignment: start/end are auto-aligned to 00:00-24:00 of the selected timezone (UTC by default). Storage is always UTC; each chunk corresponds to one local day (1440 1-minute rows).

Run live updater (keeps appending the newest closed klines every minute):
`py binance_usdm_loader.py --market spot --pair BTC/USDT live`

Verify continuity across chunks using the manifest:
`py binance_usdm_loader.py verify`
- Advanced: verify a specific window and optionally repair gaps by re-collecting from the first missing minute onward:
  - Verify only: `py binance_usdm_loader.py verify --start 2021-01-01T00:00:00Z --end 2021-01-10T00:00:00Z`
  - Verify and repair: `py binance_usdm_loader.py verify --start 2021-01-01T00:00:00Z --end 2021-01-10T00:00:00Z --repair`

Show status:
`py binance_usdm_loader.py status`

Combined Parquet (optional) — BUILD_COMBINED_ON_BACKFILL and 'combine' command
- By default, combined Parquet is NOT rebuilt at the end of backfill to avoid long blocking jobs and memory/IO spikes.
- You can enable automatic combined rebuild after backfill by setting the environment variable BUILD_COMBINED_ON_BACKFILL.
  Where to set it:
  - Windows PowerShell (current session only): `$Env:BUILD_COMBINED_ON_BACKFILL = "1"`
  - Windows cmd.exe (current session only): `set BUILD_COMBINED_ON_BACKFILL=1`
  - Linux/macOS bash/zsh (current session only): `export BUILD_COMBINED_ON_BACKFILL=1`
  - VS Code/IDE run configuration: add an environment variable named BUILD_COMBINED_ON_BACKFILL with value 1.
  - Permanent (Windows user env): System Properties -> Environment Variables -> New user variable BUILD_COMBINED_ON_BACKFILL=1 (restart shell).

Examples:
- One-off full rebuild after backfill (PowerShell):
  `$Env:BUILD_COMBINED_ON_BACKFILL = "1"; py binance_usdm_loader.py backfill`
  (remove with `Remove-Item Env:BUILD_COMBINED_ON_BACKFILL`)
- Incremental combine from Python REPL (from_index defines the start, to_index defines end if given, else till up to date):
  `import binance_usdm_loader as l; m = l.load_manifest(); l.build_combined_parquet(m, l.setup_logging(), from_index=2000, to_index=2067)`
- Manual combine via CLI with timezone suffix on filename:
  `py binance_usdm_loader.py combine --tz Europe/Zurich`
  or with indices: `py binance_usdm_loader.py combine --from-index 1 --to-index 100 --tz +02:00`

Timezone alignment for windows and live:
- Use --align-tz to align start/end to midnight of your local market time (default UTC). Example:
  `py binance_usdm_loader.py --align-tz Europe/Zurich backfill --start 2021-01-10 --end 2021-01-20`
  The data remains stored in UTC and chunked as full local-days (always 1440 1-minute candles).

Supported timezone formats and examples:
- Fixed numeric UTC offsets (no DST):
  - +00:00 (UTC), +01:00 (Europe/Zurich winter, Europe/Berlin winter), +02:00 (Helsinki/Athens or Central Europe summer),
    -04:00 (America/Halifax; also New York during summer is -04:00),
    -05:00 (America/New_York winter), -07:00 (America/Los_Angeles summer), +09:00 (Asia/Tokyo, no DST), etc.

Mapping examples (offset ⇄ typical zone at that offset on given season):
- UTC +00:00 → UTC, Europe/London (winter)
- UTC +01:00 → Europe/Zurich (winter), Europe/Berlin (winter), Europe/London (summer)
- UTC +02:00 → Europe/Zurich (summer), Europe/Berlin (summer), Africa/Cairo (no DST currently), Asia/Jerusalem (winter)
- UTC -05:00 → America/New_York (winter), America/Chicago (summer)
- UTC -06:00 → America/Chicago (winter), America/Denver (summer)
- UTC -07:00 → America/Denver (winter), America/Los_Angeles (summer)
- UTC -08:00 → America/Los_Angeles (winter)
- UTC +09:00 → Asia/Tokyo (year-round), Asia/Seoul (year-round)

Notes:
- IANA zones are recommended because fixed numeric offsets do not account for daylight saving time changes. If you use +02:00, it will always align to +02:00 even when your market switches to +01:00 or +03:00 seasonally.

Daylight Saving Time (DST) behavior:
- What changes: Many markets shift their offset twice per year (e.g., Europe/Zurich switches between +01:00 winter and +02:00 summer). On the spring-forward date, the local day has 23 hours; on the fall-back date, it has 25 hours.
- How the loader handles it: When you use an IANA zone (e.g., Europe/Zurich, America/New_York), the loader aligns start/end to local midnights in that zone, converts to UTC, and stores in UTC. This automatically yields 23h or 25h spans on the two transition days, with all minute bars preserved in UTC.
- Fixed-offset caveat: If you choose a fixed offset like +02:00, the offset never changes. Your local “day” will always be 24h, which may not match your market’s actual trading day during DST season changes.
- Recommendation: Prefer IANA timezone names for market-aligned windows. Use fixed offsets only if you explicitly want a constant offset regardless of DST.

Usage examples with other timezones:
- Align to New York trading day:
  `py binance_usdm_loader.py --align-tz America/New_York backfill --start 2021-01-10 --end 2021-01-20`
- Align to Tokyo trading day:
  `py binance_usdm_loader.py --align-tz Asia/Tokyo backfill --start 2021-01-10 --end 2021-01-20`
- Combine with explicit offset in filename suffix:
  `py binance_usdm_loader.py combine --tz America/Los_Angeles`
  or
  `py binance_usdm_loader.py combine --tz +02:00`

Notes
- The loader writes Parquet chunks with Zstandard compression. For analytics, read them with pandas.read_parquet.
- The chunks directory plus manifest.json are the source of truth. The combined file is optional and can be rebuilt at any time.
- If you need to throttle more conservatively, adjust EXTRA_SLEEP_SEC in the script. To change chunk size, use --chunk-size or edit CHUNK_SIZE constant (default 1440, i.e., one day of minutes).
- For long backfills, run in a stable environment (e.g., a screen/tmux equivalent or a background service). The loader is resumable via manifest.json.

Data Schema (columns)
- open_time (ms since epoch, UTC)
- open
- high
- low
- close
- volume
- close_time (ms since epoch, UTC)
- quote_volume
- trades (number of trades)
- taker_base_volume
- taker_quote_volume
- ignore (exchange reserved)

Caveats
- Ensure your system clock is accurate (NTP) to avoid scheduling drift in live mode.
- Live mode only appends fully-closed klines (one-minute bars). It waits near minute boundaries.
- If a requested pair does not exist on the selected market (spot/futures), the loader logs a warning and falls back to BTC/USDT.
- If Binance returns gaps due to temporary outages, retries/backoff handle most cases; you can re-run backfill to fill potential gaps.
