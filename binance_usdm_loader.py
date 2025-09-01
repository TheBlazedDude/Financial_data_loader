import os
import sys
import json
import time
import math
import hashlib
import logging
import gc
from logging.handlers import RotatingFileHandler
from datetime import datetime, timezone
from typing import List, Dict, Any, Optional, Tuple

try:
    import ccxt  # type: ignore
except Exception as e:  # pragma: no cover
    ccxt = None

try:
    import pandas as pd  # type: ignore
except Exception as e:  # pragma: no cover
    pd = None

try:
    from dateutil import parser as dateparser  # type: ignore
except Exception:
    dateparser = None

try:
    import pyarrow as pa  # type: ignore
    import pyarrow.parquet as pq  # type: ignore
except Exception:  # pragma: no cover
    pa = None
    pq = None

# --------------------------
# Configuration and Defaults
# --------------------------
# Market selection: 'futures' (USDT-margined perpetual via binanceusdm) or 'spot'
MARKET_TYPE = os.environ.get("MARKET_TYPE", "futures")  # futures | spot
EXCHANGE_ID = "binanceusdm" if MARKET_TYPE == "futures" else "binance"
SYMBOL = "BTC/USDT"  # Symbol name (ccxt format)
TIMEFRAME = "1m"
START_ISO_UTC = "2020-01-01T01:00:00Z"  # starting at 01.01.2020 01:00 UTC
# Legacy OHLCV limit (not used for klines anymore)
LIMIT = 1000
# Binance klines max limit per request
KLINE_LIMIT = 1500
CHUNK_SIZE = 1440  # rows per chunk file 1440 minuten = 24h


def _symbol_to_pair_token(symbol: str) -> str:
    try:
        return symbol.replace(" ", "").replace("-", "/").upper().replace("/", "")
    except Exception:
        return "BTCUSDT"


PAIR_TOKEN = _symbol_to_pair_token(SYMBOL)
DATA_ROOT = os.path.join("data", EXCHANGE_ID, PAIR_TOKEN, TIMEFRAME)
CHUNKS_DIR = os.path.join(DATA_ROOT, "chunks")
# Legacy combined CSV directory (no longer used with Parquet dataset)
COMBINED_DIR = os.path.join(DATA_ROOT, "combined")
COMBINED_FILE = os.path.join(COMBINED_DIR, f"{PAIR_TOKEN}_{TIMEFRAME}_all.parquet")
MANIFEST_FILE = os.path.join(DATA_ROOT, "manifest.json")
LOG_DIR = os.path.join("logs")
LOG_FILE = os.path.join(LOG_DIR, "loader.log")

# Kline schema (12 fields)
KLINE_COLUMNS = [
    "open_time", "open", "high", "low", "close", "volume",
    "close_time", "quote_volume", "trades", "taker_base_volume", "taker_quote_volume", "ignore"
]
PARQUET_COMPRESSION = "zstd"

# How many retries for transient errors
MAX_RETRIES = 5
RETRY_BACKOFF_BASE_SEC = 1.5

# Sleep safety margin between requests (additional to ccxt enableRateLimit)
EXTRA_SLEEP_SEC = 0.0  # keep 0 by default; adjust if needed

# --------------------------
# Helpers
# --------------------------

def ensure_dirs():
    os.makedirs(CHUNKS_DIR, exist_ok=True)
    os.makedirs(COMBINED_DIR, exist_ok=True)
    os.makedirs(LOG_DIR, exist_ok=True)


def setup_logging(verbose: bool = True):
    ensure_dirs()
    logger = logging.getLogger("dataloader")
    logger.setLevel(logging.INFO)
    formatter = logging.Formatter(fmt="%(asctime)s | %(levelname)s | %(message)s", datefmt="%Y-%m-%d %H:%M:%S")

    # Rotating file handler
    fh = RotatingFileHandler(LOG_FILE, maxBytes=5 * 1024 * 1024, backupCount=3, encoding="utf-8")
    fh.setFormatter(formatter)
    logger.addHandler(fh)

    if verbose:
        ch = logging.StreamHandler(sys.stdout)
        ch.setFormatter(formatter)
        logger.addHandler(ch)

    return logger


def utc_now_ms() -> int:
    return int(datetime.now(timezone.utc).timestamp() * 1000)


def iso_to_ms(iso_str: str) -> int:
    if dateparser is None:
        # Fallback: try fromisoformat (requires 'Z' removal)
        s = iso_str.replace("Z", "+00:00")
        dt = datetime.fromisoformat(s)
        return int(dt.timestamp() * 1000)
    dt = dateparser.isoparse(iso_str)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return int(dt.timestamp() * 1000)


def ms_to_iso(ms: int) -> str:
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).isoformat()


def hash_file_sha256(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            h.update(chunk)
    return h.hexdigest()


def normalize_kline_rows(rows: List[List[Any]], logger: logging.Logger) -> List[List[Any]]:
    """Coerce kline rows to proper numeric types where needed.
    Ensures open_time (index 0) is an int so time comparisons don't fail when API returns strings.
    Also tries to coerce close_time (6) and trades (8) to ints. Rows with non-convertible open_time are skipped.
    """
    normalized: List[List[Any]] = []
    for r in rows:
        if not r or len(r) < 12:
            continue
        try:
            # open_time
            open_time = int(r[0]) if not isinstance(r[0], (int, float)) else int(r[0])
        except Exception:
            try:
                open_time = int(float(r[0]))
            except Exception:
                logger.warning(f"Skipping kline with non-numeric open_time: {r[0]}")
                continue
        rr = list(r)
        rr[0] = open_time
        # best-effort coercions
        for idx in (6, 8):  # close_time, trades
            try:
                rr[idx] = int(rr[idx]) if not isinstance(rr[idx], (int, float)) else int(rr[idx])
            except Exception:
                pass
        normalized.append(rr)
    return normalized


def ensure_numeric_schema(df: 'pd.DataFrame', logger: logging.Logger) -> 'pd.DataFrame':
    """Ensure DataFrame has consistent numeric dtypes for Parquet writing.
    - Enforce column order to KLINE_COLUMNS
    - Cast price/volume columns to float64
    - Cast time columns and trades to int64
    Drops rows that cannot be coerced (very rare) with a warning.
    """
    if pd is None:
        return df
    # Ensure columns presence/order
    try:
        df = df[KLINE_COLUMNS]
    except Exception:
        missing = [c for c in KLINE_COLUMNS if c not in df.columns]
        if missing:
            logger.warning(f"ensure_numeric_schema: missing columns {missing}; attempting to continue with existing")
    # Define dtypes
    float_cols = ["open", "high", "low", "close", "volume", "quote_volume", "taker_base_volume", "taker_quote_volume"]
    int_cols = ["open_time", "close_time", "trades"]
    # Coerce floats
    for c in float_cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    # Coerce ints
    for c in int_cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce").astype("Int64")
    # After coercion, drop rows with NaNs in critical fields
    before = len(df)
    critical = ["open_time", "open", "high", "low", "close", "close_time"]
    mask = df[critical].isna().any(axis=1)
    dropped = int(mask.sum()) if hasattr(mask, 'sum') else 0
    if dropped:
        logger.warning(f"ensure_numeric_schema: dropping {dropped} invalid row(s) out of {before}")
        df = df[~mask]
    # Final cast to concrete dtypes
    for c in int_cols:
        if c in df.columns:
            df[c] = df[c].astype("int64")
    for c in float_cols:
        if c in df.columns:
            df[c] = df[c].astype("float64")
    return df


# --------------------------
# Manifest Management
# --------------------------

def load_manifest() -> Dict[str, Any]:
    if not os.path.exists(MANIFEST_FILE):
        return {
            "exchange": EXCHANGE_ID,
            "symbol": SYMBOL,
            "timeframe": TIMEFRAME,
            "chunk_size": CHUNK_SIZE,
            "format": "parquet",
            "columns": KLINE_COLUMNS,
            "chunks": [],
            "last_candle_ms": None,
            "created_utc": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
            "updated_utc": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        }
    with open(MANIFEST_FILE, "r", encoding="utf-8") as f:
        return json.load(f)


def save_manifest(manifest: Dict[str, Any]):
    manifest["updated_utc"] = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    with open(MANIFEST_FILE, "w", encoding="utf-8") as f:
        json.dump(manifest, f, indent=2)


def get_resume_since_ms(manifest: Dict[str, Any], default_start_ms: int) -> int:
    last_ms = manifest.get("last_candle_ms")
    if last_ms is None:
        return default_start_ms
    return int(last_ms) + 60_000  # next minute


# --------------------------
# Exchange Client
# --------------------------

def resolve_market_symbol(ex, logger: logging.Logger, requested_symbol: str) -> str:
    markets = ex.load_markets()
    # Normalize requested symbol to CCXT format (BASE/QUOTE upper)
    try:
        rs = requested_symbol.replace(" ", "").replace("-", "/").upper()
    except Exception:
        rs = requested_symbol
    if rs in markets:
        return rs
    # Extract base/quote if possible
    base, quote = None, None
    if "/" in rs:
        base, quote = rs.split("/", 1)
    # Resolution differs by market type
    def _search_with(b: str, q: str) -> Optional[str]:
        if MARKET_TYPE == 'futures':
            candidates = []
            for m in markets.values():
                try:
                    if m.get('base') == b and m.get('quote') == q and m.get('linear'):
                        candidates.append(m)
                except Exception:
                    continue
            swaps = [m for m in candidates if m.get('type') == 'swap']
            chosen = swaps[0] if swaps else (candidates[0] if candidates else None)
            if chosen is not None:
                return chosen.get('symbol')
            for k in markets.keys():
                if k.startswith(f'{b}/{q}'):
                    return k
            return None
        else:
            for k, m in markets.items():
                try:
                    if m.get('spot') and m.get('base') == b and m.get('quote') == q:
                        return m.get('symbol', k)
                except Exception:
                    continue
            for k in markets.keys():
                if k.startswith(f'{b}/{q}'):
                    return k
            return None
    # Try requested base/quote
    if base and quote:
        resolved = _search_with(base, quote)
        if resolved:
            return resolved
    # Fallback to BTC/USDT
    logger.warning(f"Could not resolve requested symbol '{requested_symbol}' on {EXCHANGE_ID} {MARKET_TYPE}; falling back to BTC/USDT")
    fallback = _search_with('BTC', 'USDT')
    if fallback:
        return fallback
    sample = next(iter(markets.keys())) if markets else 'N/A'
    raise ValueError(f"Could not resolve symbol. Sample market key: {sample}")


def create_exchange(logger: logging.Logger):
    if ccxt is None:
        raise RuntimeError("ccxt is not installed. Please install dependencies from requirements.txt")
    if MARKET_TYPE == 'futures':
        ex = ccxt.binanceusdm({
            "enableRateLimit": True,
        })
    else:
        # Spot API has higher rate limits (6000 weights/min vs 2400 for futures)
        ex = ccxt.binance({
            "enableRateLimit": True,
        })
    # Resolve the correct symbol on this exchange (with fallback inside resolver)
    resolved = resolve_market_symbol(ex, logger, SYMBOL)
    return ex, resolved


# --------------------------
# IO helpers
# --------------------------

def write_chunk_parquet(df: 'pd.DataFrame', chunk_index: int, start_ms: int, end_ms: int, logger: logging.Logger) -> Tuple[str, str]:
    filename = f"chunk_{chunk_index:06d}_{start_ms}_{end_ms}.parquet"
    path = os.path.join(CHUNKS_DIR, filename)
    # Ensure proper dtypes and column order
    df = ensure_numeric_schema(df, logger)
    df.to_parquet(path, index=False, compression=PARQUET_COMPRESSION)
    sha256 = hash_file_sha256(path)
    logger.info(f"Saved chunk #{chunk_index} -> {path} (rows={len(df)}, sha256={sha256[:12]}...)")
    return path, sha256


def append_to_combined(df: 'pd.DataFrame', logger: logging.Logger):
    # No-op with Parquet dataset. We keep this function for backward compatibility of calls.
    logger.info(f"Appended {len(df)} rows to dataset (Parquet chunks); combined file maintenance is optional and can be enabled via BUILD_COMBINED_ON_BACKFILL or external compaction.")


def build_combined_parquet(manifest: Dict[str, Any], logger: logging.Logger, from_index: Optional[int] = None, to_index: Optional[int] = None) -> Optional[str]:
    """Build a single combined Parquet file from all existing chunk Parquet files.
    Uses pyarrow.ParquetWriter to stream row groups and avoid high memory usage.
    Returns the combined file path on success, or None if skipped/failure.
    """
    if pq is None:
        logger.warning("pyarrow is not available; skipping combined Parquet build.")
        return None

    chunks_all = sorted(manifest.get("chunks", []), key=lambda c: c["index"]) if manifest else []
    if from_index is not None or to_index is not None:
        lo = from_index if from_index is not None else (chunks_all[0]["index"] if chunks_all else 0)
        hi = to_index if to_index is not None else (chunks_all[-1]["index"] if chunks_all else -1)
        chunks = [c for c in chunks_all if lo <= c["index"] <= hi]
    else:
        chunks = chunks_all
    if not chunks:
        logger.info("No chunks present; skipping combined Parquet build.")
        return None

    os.makedirs(COMBINED_DIR, exist_ok=True)
    combined_tmp = os.path.join(COMBINED_DIR, f"{PAIR_TOKEN}_{TIMEFRAME}_all.tmp.parquet")
    combined_path = COMBINED_FILE

    # Remove temp file if it exists
    try:
        if os.path.exists(combined_tmp):
            os.remove(combined_tmp)
    except Exception:
        pass

    writer: Optional[pq.ParquetWriter] = None
    import gc
    total_rows = 0
    chunk_count = 0

    try:
        for ch in chunks:
            rel = ch.get("filename")
            if not rel:
                continue
            path = os.path.join(DATA_ROOT, rel)
            if not os.path.exists(path):
                logger.warning(f"Chunk file missing during combine: {path}")
                continue
            pf = pq.ParquetFile(path)
            if writer is None:
                # Initialize writer with the first file's schema
                schema = pf.schema_arrow
                writer = pq.ParquetWriter(combined_tmp, schema=schema, compression=PARQUET_COMPRESSION)
            # Write each row group to the combined file
            for rg in range(pf.num_row_groups):
                table = pf.read_row_group(rg)
                total_rows += table.num_rows
                writer.write_table(table)
                del table
                gc.collect()
            chunk_count += 1
    finally:
        if writer is not None:
            writer.close()

    if chunk_count == 0:
        logger.info("No valid chunk files found to combine.")
        try:
            if os.path.exists(combined_tmp):
                os.remove(combined_tmp)
        except Exception:
            pass
        return None

    # Atomically replace the previous combined file
    try:
        if os.path.exists(combined_path):
            os.remove(combined_path)
    except Exception:
        pass
    os.replace(combined_tmp, combined_path)
    logger.info(f"Built combined Parquet from {chunk_count} chunks -> {combined_path} (rows={total_rows})")
    return combined_path


# --------------------------
# Interval Utilities
# --------------------------

def compute_missing_intervals(request_start_ms: int, request_end_ms: int, chunks: List[Dict[str, Any]]) -> List[Tuple[int, int]]:
    """Compute the missing [start,end] inclusive 1-minute intervals within the requested window,
    given already existing chunk coverages in the manifest.

    - Both request and chunk intervals are treated as inclusive on open_time.
    - Returns a sorted list of non-overlapping intervals covering all gaps within [request_start_ms, request_end_ms].
    """
    if request_start_ms > request_end_ms:
        return []
    # Collect covered intervals intersected with request window
    covered: List[Tuple[int, int]] = []
    for ch in sorted(chunks or [], key=lambda c: (int(c.get("start_ms", 0)), int(c.get("end_ms", 0)))):
        s = int(ch.get("start_ms", 0))
        e = int(ch.get("end_ms", -1))
        if e < request_start_ms or s > request_end_ms:
            continue
        s = max(s, request_start_ms)
        e = min(e, request_end_ms)
        if s <= e:
            covered.append((s, e))
    # Merge covered intervals
    covered.sort()
    merged: List[Tuple[int, int]] = []
    for s, e in covered:
        if not merged or s > merged[-1][1] + 60_000:
            merged.append((s, e))
        else:
            merged[-1] = (merged[-1][0], max(merged[-1][1], e))
    # Subtract merged from request to get missing
    missing: List[Tuple[int, int]] = []
    cur = request_start_ms
    for s, e in merged:
        if cur < s:
            missing.append((cur, s - 60_000))  # inclusive minutes up to s-1min
        cur = e + 60_000
    if cur <= request_end_ms:
        missing.append((cur, request_end_ms))
    return missing


# --------------------------
# Core Loader
# --------------------------

def fetch_klines_page(ex, symbol: str, timeframe: str, since_ms: int, limit: int, logger: logging.Logger) -> List[List[Any]]:
    """Fetch a page of Binance klines (12 fields) using raw endpoint.
    Uses futures (fapiPublicGetKlines) for MARKET_TYPE='futures' and spot (publicGetKlines) for MARKET_TYPE='spot'.
    Fields: [open_time, open, high, low, close, volume, close_time, quote_volume, trades, taker_base_volume, taker_quote_volume, ignore]
    """
    last_exc: Optional[Exception] = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            market = ex.market(symbol)
            symbol_id = market.get('id', market.get('symbol', symbol).replace('/', ''))
            interval = ex.timeframes.get(timeframe, timeframe) if hasattr(ex, 'timeframes') else timeframe
            params = {
                'symbol': symbol_id,
                'interval': interval,
                'startTime': since_ms,
                'limit': min(limit, KLINE_LIMIT),
            }
            if MARKET_TYPE == 'futures':
                # USDT-M futures
                rows = ex.fapiPublicGetKlines(params)
            else:
                # Spot API
                rows = ex.publicGetKlines(params)
            if EXTRA_SLEEP_SEC > 0:
                time.sleep(EXTRA_SLEEP_SEC)
            return rows
        except Exception as e:  # pragma: no cover (network-dependent)
            last_exc = e
            wait = RETRY_BACKOFF_BASE_SEC ** attempt
            logger.warning(f"fetch_klines failed (attempt {attempt}/{MAX_RETRIES}): {e}; sleeping {wait:.2f}s")
            time.sleep(wait)
    if last_exc:
        raise last_exc
    return []


def backfill(logger: logging.Logger,
             start_ms: Optional[int] = None,
             end_ms: Optional[int] = None,
             chunk_size: int = CHUNK_SIZE):
    ensure_dirs()
    if pd is None:
        raise RuntimeError("pandas is required. Please install dependencies from requirements.txt")

    manifest = load_manifest()
    ex, resolved_symbol = create_exchange(logger)
    # Persist requested/resolved symbol and schema for transparency
    manifest["symbol_requested"] = SYMBOL
    manifest["symbol_resolved"] = resolved_symbol
    manifest["format"] = "parquet"
    manifest["columns"] = KLINE_COLUMNS
    save_manifest(manifest)

    # Determine requested window
    request_start_ms = iso_to_ms(START_ISO_UTC) if start_ms is None else start_ms
    if end_ms is None:
        request_end_ms = utc_now_ms() - 60_000  # up to last fully closed minute
    else:
        request_end_ms = end_ms

    # Compute missing intervals relative to existing chunks
    chunks_meta = manifest.get("chunks", [])
    missing_intervals = compute_missing_intervals(request_start_ms, request_end_ms, chunks_meta)

    if not missing_intervals:
        logger.info(f"No backfill needed for {ms_to_iso(request_start_ms)} to {ms_to_iso(request_end_ms)}; data already present.")
        return

    logger.info(
        f"Starting backfill for {len(missing_intervals)} gap(s) within {ms_to_iso(request_start_ms)} to {ms_to_iso(request_end_ms)} "
        f"with chunk_size={chunk_size}"
    )

    total_added = 0
    chunk_index = len(chunks_meta) + 1

    for gap_start, gap_end in missing_intervals:
        logger.info(f"Filling missing interval: {ms_to_iso(gap_start)} -> {ms_to_iso(gap_end)}")
        buffer: List[List[Any]] = []
        last_page_last_ms: Optional[int] = None
        since_ms = gap_start

        while since_ms <= gap_end:
            rows = fetch_klines_page(ex, resolved_symbol, TIMEFRAME, since_ms, KLINE_LIMIT, logger)
            if not rows:
                logger.info("No more klines returned by exchange; stopping this interval.")
                break

            # Normalize to ensure numeric open_time and relevant fields
            rows = normalize_kline_rows(rows, logger)

            # Ensure only within our gap_end (open_time index 0)
            rows = [r for r in rows if r and len(r) >= 12 and r[0] <= gap_end]
            if not rows:
                break

            # Continuity check with previous page of the same interval
            if last_page_last_ms is not None:
                expected = last_page_last_ms + 60_000
                if rows[0][0] != expected:
                    logger.warning(f"Gap or overlap detected between pages: expected {expected} got {rows[0][0]}")
            last_page_last_ms = rows[-1][0]

            buffer.extend(rows)
            total_added += len(rows)

            # If we exceeded chunk size, flush full chunks out of buffer
            while len(buffer) >= chunk_size:
                chunk = buffer[:chunk_size]
                buffer = buffer[chunk_size:]
                df = pd.DataFrame(chunk, columns=KLINE_COLUMNS)  # type: ignore
                start_ms_chunk = int(df.iloc[0]["open_time"])  # type: ignore
                end_ms_chunk = int(df.iloc[-1]["open_time"])  # type: ignore
                path, sha256 = write_chunk_parquet(df, chunk_index, start_ms_chunk, end_ms_chunk, logger)
                append_to_combined(df, logger)
                # Capture count before freeing the dataframe
                row_count = len(chunk)
                # Proactively drop dataframe and trigger GC to free memory between chunks
                try:
                    del df
                except Exception:
                    pass
                gc.collect()
                # update manifest
                manifest.setdefault("chunks", []).append({
                    "index": chunk_index,
                    "filename": os.path.relpath(path, DATA_ROOT),
                    "start_ms": start_ms_chunk,
                    "end_ms": end_ms_chunk,
                    "count": row_count,
                    "sha256": sha256,
                })
                manifest["last_candle_ms"] = max(int(manifest.get("last_candle_ms") or 0), end_ms_chunk)
                save_manifest(manifest)
                chunk_index += 1

            # Advance since_ms to one minute after the last kline open_time
            since_ms = rows[-1][0] + 60_000

            # Progress log occasionally
            if total_added % (1000 * 50) == 0:  # every ~50k
                logger.info(f"Progress: {total_added} klines fetched so far. Current since={ms_to_iso(since_ms)}")

        # Flush any remaining buffer for this interval as a final partial chunk
        if buffer:
            df = pd.DataFrame(buffer, columns=KLINE_COLUMNS)  # type: ignore
            start_ms_chunk = int(df.iloc[0]["open_time"])  # type: ignore
            end_ms_chunk = int(df.iloc[-1]["open_time"])  # type: ignore
            path, sha256 = write_chunk_parquet(df, chunk_index, start_ms_chunk, end_ms_chunk, logger)
            append_to_combined(df, logger)
            # Capture count before freeing the dataframe
            row_count = len(buffer)
            try:
                del df
            except Exception:
                pass
            gc.collect()
            manifest.setdefault("chunks", []).append({
                "index": chunk_index,
                "filename": os.path.relpath(path, DATA_ROOT),
                "start_ms": start_ms_chunk,
                "end_ms": end_ms_chunk,
                "count": row_count,
                "sha256": sha256,
            })
            manifest["last_candle_ms"] = max(int(manifest.get("last_candle_ms") or 0), end_ms_chunk)
            save_manifest(manifest)
            chunk_index += 1

    # Build combined Parquet from all chunks at the end of backfill only if environment variable requests it
    try:
        if os.environ.get("BUILD_COMBINED_ON_BACKFILL", "0") in ("1", "true", "TRUE", "yes", "YES"):
            build_combined_parquet(manifest, logger)
        else:
            logger.info("Skipping combined Parquet build after backfill (BUILD_COMBINED_ON_BACKFILL not enabled).")
    except Exception as e:
        logger.warning(f"Failed to build combined Parquet: {e}")

    logger.info(f"Backfill complete. Total klines added: {total_added}")
    # Explicitly free large objects and trigger a full garbage collection to minimize memory usage after backfill
    try:
        ex.close() if hasattr(ex, 'close') else None
    except Exception:
        pass
    locals_to_clear = ['df', 'buffer', 'rows', 'chunk', 'manifest']
    for name in locals_to_clear:
        if name in locals():
            try:
                del locals()[name]
            except Exception:
                pass
    gc.collect()


def verify_continuity(logger: logging.Logger) -> bool:
    """Verify that the manifest records continuous 1-minute data across chunks."""
    manifest = load_manifest()
    chunks = sorted(manifest.get("chunks", []), key=lambda c: c["index"])
    prev_end = None
    ok = True
    for ch in chunks:
        if prev_end is None:
            prev_end = ch["end_ms"]
            continue
        expected = prev_end + 60_000
        if ch["start_ms"] != expected:
            logger.error(f"Continuity error between chunks {ch['index']-1} and {ch['index']}: expected start {expected} got {ch['start_ms']}")
            ok = False
        prev_end = ch["end_ms"]
    if ok:
        logger.info("Continuity verified across chunks.")
    return ok


def status(logger: logging.Logger):
    manifest = load_manifest()
    chunks = manifest.get("chunks", [])
    last_ms = manifest.get("last_candle_ms")
    print(json.dumps({
        "exchange": manifest.get("exchange"),
        "symbol": manifest.get("symbol"),
        "timeframe": manifest.get("timeframe"),
        "format": manifest.get("format"),
        "columns": manifest.get("columns"),
        "chunk_count": len(chunks),
        "last_candle_iso": ms_to_iso(last_ms) if last_ms else None,
        "data_root": os.path.abspath(DATA_ROOT),
        "dataset_dir": os.path.abspath(CHUNKS_DIR),
    }, indent=2))


def live(logger: logging.Logger, chunk_size: int = CHUNK_SIZE):
    if pd is None:
        raise RuntimeError("pandas is required. Please install dependencies from requirements.txt")

    ensure_dirs()
    ex, resolved_symbol = create_exchange(logger)
    manifest = load_manifest()

    if manifest.get("last_candle_ms") is None:
        logger.info("Manifest has no data yet. Starting initial backfill first...")
        backfill(logger, chunk_size=chunk_size)
        manifest = load_manifest()

    # Always ensure we are fully caught up to now by computing gaps from last_candle_ms to last closed minute
    target_end = utc_now_ms() - 60_000
    request_start = int(manifest.get("last_candle_ms", 0)) + 60_000
    if request_start <= target_end:
        logger.info(f"Live mode pre-sync: filling gaps from {ms_to_iso(request_start)} to {ms_to_iso(target_end)} before starting the minute loop")
        backfill(logger, start_ms=request_start, end_ms=target_end, chunk_size=chunk_size)
        manifest = load_manifest()

    last_ms = int(manifest["last_candle_ms"])  # last closed candle time

    logger.info("Starting live updater. Will fetch any new klines every minute and maintain chunking.")
    while True:  # pragma: no cover (long-running loop)
        try:
            # Target up to the last fully-closed candle
            target_end = utc_now_ms() - 60_000
            since_ms = last_ms + 60_000
            if since_ms <= target_end:
                # Stream pages directly into chunk files without building a large in-memory list
                page_since = since_ms
                # Work buffer that we will flush into chunk files; keep it bounded
                work_buffer: Optional['pd.DataFrame'] = None

                # Load current manifest/chunk meta for appending logic
                manifest = load_manifest()
                chunks_meta = manifest.get("chunks", [])
                if chunks_meta:
                    last_chunk = chunks_meta[-1]
                    current_index = last_chunk["index"]
                    current_count = last_chunk["count"]
                else:
                    current_index = 1
                    current_count = 0

                rows = fetch_klines_page(ex, resolved_symbol, TIMEFRAME, page_since, KLINE_LIMIT, logger)
                while rows:
                    rows = normalize_kline_rows(rows, logger)
                    rows = [r for r in rows if r and len(r) >= 12 and r[0] <= target_end]
                    if not rows:
                        break

                    # Convert this page to a small DataFrame, process, then discard
                    page_df = pd.DataFrame(rows, columns=KLINE_COLUMNS)  # type: ignore
                    page_df = ensure_numeric_schema(page_df, logger)

                    # If there is an existing partial chunk, attempt to append remainder up to chunk_size
                    if chunks_meta and current_count < chunk_size:
                        prev_path = os.path.join(DATA_ROOT, chunks_meta[-1]["filename"])  # type: ignore
                        prev_df = pd.read_parquet(prev_path)
                        prev_df = ensure_numeric_schema(prev_df, logger)
                        remainder = chunk_size - current_count
                        head = page_df.iloc[:remainder]
                        if len(head) > 0:
                            head = ensure_numeric_schema(head.copy(), logger)
                            new_df = pd.concat([prev_df, head], ignore_index=True)
                            new_df = ensure_numeric_schema(new_df, logger)
                            end_ms_chunk = int(new_df.iloc[-1]["open_time"])  # type: ignore
                            new_df.to_parquet(prev_path, index=False, compression=PARQUET_COMPRESSION)
                            sha256 = hash_file_sha256(prev_path)
                            chunks_meta[-1].update({
                                "end_ms": end_ms_chunk,
                                "count": len(new_df),
                                "sha256": sha256,
                            })
                            manifest["chunks"] = chunks_meta
                            manifest["last_candle_ms"] = end_ms_chunk
                            save_manifest(manifest)
                            # Update counters and trim page_df
                            current_count = len(new_df)
                            page_df = page_df.iloc[len(head):]
                            try:
                                del new_df
                                del prev_df
                            except Exception:
                                pass
                            gc.collect()

                    # Fill the existing last chunk fully before creating any new chunk
                    while len(page_df) > 0:
                        # Refresh manifest and chunk meta to get the latest count/state
                        manifest = load_manifest()
                        chunks_meta = manifest.get("chunks", [])

                        # If there is a partial chunk, append to it up to the remainder
                        if chunks_meta and chunks_meta[-1]["count"] < chunk_size:
                            prev_path = os.path.join(DATA_ROOT, chunks_meta[-1]["filename"])  # type: ignore
                            prev_df = pd.read_parquet(prev_path)
                            prev_df = ensure_numeric_schema(prev_df, logger)
                            remainder = chunk_size - int(chunks_meta[-1]["count"])
                            head = page_df.iloc[:remainder]
                            if len(head) == 0:
                                break
                            head = ensure_numeric_schema(head.copy(), logger)
                            new_df = pd.concat([prev_df, head], ignore_index=True)
                            new_df = ensure_numeric_schema(new_df, logger)
                            end_ms_chunk = int(new_df.iloc[-1]["open_time"])  # type: ignore
                            new_df.to_parquet(prev_path, index=False, compression=PARQUET_COMPRESSION)
                            sha256 = hash_file_sha256(prev_path)
                            chunks_meta[-1].update({
                                "end_ms": end_ms_chunk,
                                "count": len(new_df),
                                "sha256": sha256,
                            })
                            manifest["chunks"] = chunks_meta
                            manifest["last_candle_ms"] = end_ms_chunk
                            save_manifest(manifest)
                            # Trim consumed rows and free
                            page_df = page_df.iloc[len(head):]
                            try:
                                del prev_df
                                del new_df
                            except Exception:
                                pass
                            gc.collect()
                            continue

                        # Otherwise, start a new chunk from remaining page_df (up to chunk_size)
                        take = min(chunk_size, len(page_df))
                        part = page_df.iloc[:take]
                        page_df = page_df.iloc[take:]

                        # Determine new chunk index (last chunk is full or absent)
                        current_index = (chunks_meta[-1]["index"] + 1) if chunks_meta else 1  # type: ignore
                        start_ms_chunk = int(part.iloc[0]["open_time"])  # type: ignore
                        end_ms_chunk = int(part.iloc[-1]["open_time"])  # type: ignore
                        path, sha256 = write_chunk_parquet(part, current_index, start_ms_chunk, end_ms_chunk, logger)
                        chunks_meta.append({
                            "index": current_index,
                            "filename": os.path.relpath(path, DATA_ROOT),
                            "start_ms": start_ms_chunk,
                            "end_ms": end_ms_chunk,
                            "count": len(part),
                            "sha256": sha256,
                        })
                        manifest["chunks"] = chunks_meta
                        manifest["last_candle_ms"] = end_ms_chunk
                        save_manifest(manifest)
                        # Explicitly free part DataFrame
                        try:
                            del part
                        except Exception:
                            pass
                        gc.collect()

                    # Update last_ms for the loop
                    if chunks_meta:
                        last_ms = chunks_meta[-1]["end_ms"]

                    # Fetch next page if we still have to catch up
                    next_since = rows[-1][0] + 60_000
                    if next_since > target_end:
                        rows = []
                    else:
                        page_since = next_since
                        rows = fetch_klines_page(ex, resolved_symbol, TIMEFRAME, page_since, KLINE_LIMIT, logger)

                    # Cleanup per page
                    try:
                        del page_df
                    except Exception:
                        pass
                    gc.collect()

                # End while rows
            else:
                logger.debug("Already up-to-date with last closed kline.")
        except Exception as e:
            logger.exception(f"Live updater error: {e}")
        # Periodic cleanup to keep memory low
        gc.collect()
        now = datetime.now(timezone.utc)
        sleep_sec = 60 - (now.second + now.microsecond / 1_000_000.0)
        time.sleep(max(1.0, sleep_sec))


# --------------------------
# CLI
# --------------------------

def _parse_args(argv: List[str]):
    import argparse
    p = argparse.ArgumentParser(description="Binance 1m Data Loader (spot or USDM futures) with chunking and live updates")
    sub = p.add_subparsers(dest="cmd", required=True)

    # Global options
    p.add_argument("--market", choices=["spot", "futures"], default=MARKET_TYPE, help="Market type: spot (Binance) or futures (Binance USDM)")
    p.add_argument("--pair", default=SYMBOL, help="Trading pair in CCXT format, e.g., BTC/USDT. Will fall back to BTC/USDT if unavailable.")
    p.add_argument("--quiet", action="store_true", help="Reduce console logging")

    p_backfill = sub.add_parser("backfill", help="Run historical backfill")
    p_backfill.add_argument("--start", help="Start ISO time (UTC). Default 2020-01-01T01:00:00Z", default=START_ISO_UTC)
    p_backfill.add_argument("--end", help="End ISO time (UTC). Default last closed minute", default=None)
    p_backfill.add_argument("--chunk-size", type=int, default=CHUNK_SIZE)

    p_live = sub.add_parser("live", help="Run continuous live updater (will trigger backfill if needed)")
    p_live.add_argument("--chunk-size", type=int, default=CHUNK_SIZE)

    sub.add_parser("verify", help="Verify continuity across chunks")
    sub.add_parser("status", help="Print current status from manifest")

    return p.parse_args(argv)


def main(argv: Optional[List[str]] = None):
    global MARKET_TYPE, EXCHANGE_ID, SYMBOL, PAIR_TOKEN, DATA_ROOT, CHUNKS_DIR, COMBINED_DIR, COMBINED_FILE, MANIFEST_FILE
    args = _parse_args(argv or sys.argv[1:])

    # Apply market and pair selection at runtime
    MARKET_TYPE = args.market
    EXCHANGE_ID = "binanceusdm" if MARKET_TYPE == "futures" else "binance"
    # Normalize pair
    SYMBOL = args.pair.replace(" ", "").replace("-", "/").upper()
    PAIR_TOKEN = _symbol_to_pair_token(SYMBOL)
    # Recompute data paths based on market and pair
    DATA_ROOT = os.path.join("data", EXCHANGE_ID, PAIR_TOKEN, TIMEFRAME)
    CHUNKS_DIR = os.path.join(DATA_ROOT, "chunks")
    COMBINED_DIR = os.path.join(DATA_ROOT, "combined")
    COMBINED_FILE = os.path.join(COMBINED_DIR, f"{PAIR_TOKEN}_{TIMEFRAME}_all.parquet")
    MANIFEST_FILE = os.path.join(DATA_ROOT, "manifest.json")

    logger = setup_logging(verbose=not args.quiet)

    if args.cmd == "backfill":
        start_ms = iso_to_ms(args.start) if args.start else iso_to_ms(START_ISO_UTC)
        end_ms = iso_to_ms(args.end) if args.end else None
        backfill(logger, start_ms=start_ms, end_ms=end_ms, chunk_size=args.chunk_size)
    elif args.cmd == "live":
        live(logger, chunk_size=args.chunk_size)
    elif args.cmd == "verify":
        ok = verify_continuity(logger)
        sys.exit(0 if ok else 2)
    elif args.cmd == "status":
        status(logger)


if __name__ == "__main__":
    main()
