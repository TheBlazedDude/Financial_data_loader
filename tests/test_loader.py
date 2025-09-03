import os
import unittest
import tempfile
import shutil
from datetime import datetime, timezone

import pandas as pd

# Optional parquet engine
try:
    import pyarrow  # type: ignore
    HAS_PARQUET = True
except Exception:
    HAS_PARQUET = False

import binance_usdm_loader as loader


@unittest.skipUnless(HAS_PARQUET, "pyarrow is required for Parquet tests")
class LoaderBackfillTest(unittest.TestCase):
    def setUp(self):
        # Create temp directories for data and logs
        self.tempdir = tempfile.mkdtemp(prefix="loader_test_")
        self.data_root = os.path.join(self.tempdir, "data", loader.EXCHANGE_ID, "BTCUSDT", loader.TIMEFRAME)
        self.chunks_dir = os.path.join(self.data_root, "chunks")
        self.logs_dir = os.path.join(self.tempdir, "logs")
        os.makedirs(self.chunks_dir, exist_ok=True)
        os.makedirs(self.logs_dir, exist_ok=True)

        # Redirect module paths
        loader.DATA_ROOT = self.data_root
        loader.CHUNKS_DIR = self.chunks_dir
        loader.MANIFEST_FILE = os.path.join(self.data_root, "manifest.json")
        loader.LOG_DIR = self.logs_dir
        loader.LOG_FILE = os.path.join(self.logs_dir, "loader.log")
        loader.COMBINED_DIR = os.path.join(self.data_root, "combined")
        loader.COMBINED_FILE = os.path.join(loader.COMBINED_DIR, "BTCUSDT_1m_all.parquet")

        # Build a logger (writes into temp logs)
        self.logger = loader.setup_logging(verbose=False)

        # Monkeypatch create_exchange to avoid ccxt and network
        loader.create_exchange = lambda logger: (object(), "BTC/USDT")

    def tearDown(self):
        try:
            # Ensure logging handlers are closed to avoid ResourceWarnings
            try:
                loader.shutdown_logging()
            except Exception:
                pass
            shutil.rmtree(self.tempdir)
        except Exception:
            pass

    def test_backfill_parquet_and_manifest_continuity(self):
        # Define a small window: 10 minutes
        start_iso = "2020-01-01T01:00:00Z"
        start_ms = loader.iso_to_ms(start_iso)
        end_ms = start_ms + 9 * 60_000  # inclusive 10 klines

        # Stub fetch_klines_page to return synthetic 12-field rows
        def fake_fetch(_ex, _symbol, _timeframe, since_ms, limit, _logger):
            # Generate rows starting at since_ms, 1-minute apart
            count = min(limit, 2000)  # just guard
            rows = []
            t = since_ms
            for _ in range(count):
                rows.append([
                    t,                # open_time
                    100.0,            # open
                    110.0,            # high
                    90.0,             # low
                    105.0,            # close
                    1.23,             # volume
                    t + 59_999,       # close_time
                    123.45,           # quote_volume
                    10,               # trades
                    0.5,              # taker_base_volume
                    50.0,             # taker_quote_volume
                    0                 # ignore
                ])
                t += 60_000
            return rows

        # Patch the fetch function
        loader.fetch_klines_page = fake_fetch

        # Run backfill with chunk_size=5 -> expect 2 chunks exactly (10 rows)
        loader.backfill(self.logger, start_ms=start_ms, end_ms=end_ms, chunk_size=5)

        # Validate manifest
        manifest = loader.load_manifest()
        self.assertEqual(manifest.get("format"), "parquet")
        self.assertEqual(manifest.get("columns"), loader.KLINE_COLUMNS)
        self.assertEqual(manifest.get("last_candle_ms"), end_ms)
        chunks = manifest.get("chunks", [])
        self.assertEqual(len(chunks), 2)

        # Check continuity meta
        self.assertEqual(chunks[0]["start_ms"], start_ms)
        self.assertEqual(chunks[0]["end_ms"], start_ms + 4 * 60_000)
        self.assertEqual(chunks[0]["count"], 5)
        self.assertEqual(chunks[1]["start_ms"], start_ms + 5 * 60_000)
        self.assertEqual(chunks[1]["end_ms"], start_ms + 9 * 60_000)
        self.assertEqual(chunks[1]["count"], 5)

        # Parquet files exist and columns match; filename layout and file hash integrity
        for ch in chunks:
            rel = ch["filename"]
            # Validate manifest filename structure
            self.assertEqual(os.path.dirname(rel), "chunks")
            base = os.path.basename(rel)
            self.assertTrue(base.startswith("chunk_"))
            self.assertTrue(base.endswith(".parquet"))

            path = os.path.join(loader.DATA_ROOT, rel)
            self.assertTrue(os.path.exists(path), f"Missing chunk file: {path}")
            self.assertTrue(path.lower().endswith(".parquet"))

            # Validate sha256 integrity recorded in manifest
            actual_sha = loader.hash_file_sha256(path)
            self.assertEqual(actual_sha, ch["sha256"])  # ensures file content correctness

            df = pd.read_parquet(path)
            self.assertEqual(list(df.columns), loader.KLINE_COLUMNS)
            self.assertEqual(len(df), ch["count"])

        # Verify continuity routine returns True
        self.assertTrue(loader.verify_continuity(self.logger))

    def test_align_midnight_ms_handles_dst_transitions(self):
        # Europe/Zurich: DST starts last Sunday in March (23-hour day) and ends last Sunday in October (25-hour day)
        tz = "Europe/Zurich"
        # Pick known dates: 2021-03-28 (spring forward), 2021-10-31 (fall back)
        # Evaluate UTC span of a single local day boundary computed by align_midnight_ms
        # Start with spring forward day - compute UTC ms for local 00:00 and next day 00:00
        start_local_spring = loader.iso_to_ms("2021-03-28T00:00:00Z")  # placeholder; we will convert via function
        # Instead pass any ms inside that day (noon UTC) so align_midnight_ms finds the day boundaries in tz
        inside_spring_ms = loader.iso_to_ms("2021-03-28T12:00:00Z")
        start_utc_ms = loader.align_midnight_ms(inside_spring_ms, tz, "start")
        end_utc_ms_plus = loader.align_midnight_ms(inside_spring_ms, tz, "end") + 60_000  # end is last minute open; add 1 minute to get next midnight
        # Duration in minutes should be 23 hours = 1380 minutes
        duration_min_spring = int(round((end_utc_ms_plus - start_utc_ms) / 60000))
        self.assertIn(duration_min_spring, (1380,))

        # Fall back day
        inside_fall_ms = loader.iso_to_ms("2021-10-31T12:00:00Z")
        start_utc_ms_fall = loader.align_midnight_ms(inside_fall_ms, tz, "start")
        end_utc_ms_plus_fall = loader.align_midnight_ms(inside_fall_ms, tz, "end") + 60_000
        duration_min_fall = int(round((end_utc_ms_plus_fall - start_utc_ms_fall) / 60000))
        self.assertIn(duration_min_fall, (1500,))

        # For a fixed offset, the duration should always be 1440
        tz_fixed = "+01:00"
        start_fixed = loader.align_midnight_ms(inside_spring_ms, tz_fixed, "start")
        end_fixed = loader.align_midnight_ms(inside_spring_ms, tz_fixed, "end") + 60_000
        duration_min_fixed = int(round((end_fixed - start_fixed) / 60000))
        self.assertEqual(duration_min_fixed, 1440)


    def test_chunk_loading_and_combination_correctness(self):
        # Define a window that yields 3 chunks with chunk_size=5: total 13 rows -> [5,5,3]
        start_iso = "2020-01-01T01:00:00Z"
        start_ms = loader.iso_to_ms(start_iso)
        end_ms = start_ms + 12 * 60_000  # inclusive 13 klines

        # Stub fetch_klines_page to return synthetic 12-field rows
        def fake_fetch(_ex, _symbol, _timeframe, since_ms, limit, _logger):
            count = min(limit, 2000)
            rows = []
            t = since_ms
            for _ in range(count):
                rows.append([
                    t,                # open_time
                    100.0,            # open
                    110.0,            # high
                    90.0,             # low
                    105.0,            # close
                    1.23,             # volume
                    t + 59_999,       # close_time
                    123.45,           # quote_volume
                    10,               # trades
                    0.5,              # taker_base_volume
                    50.0,             # taker_quote_volume
                    0                 # ignore
                ])
                t += 60_000
            return rows

        loader.fetch_klines_page = fake_fetch

        # Backfill with chunk_size=5 so we get multiple chunks
        loader.backfill(self.logger, start_ms=start_ms, end_ms=end_ms, chunk_size=5)

        manifest = loader.load_manifest()
        chunks = sorted(manifest.get("chunks", []), key=lambda c: c["index"])
        self.assertGreaterEqual(len(chunks), 3)

        # Expected total rows
        expected_rows = (end_ms - start_ms) // 60_000 + 1
        total_rows = 0

        prev_end_ms = None
        combined_frames = []
        for ch in chunks:
            # Validate manifest filename structure
            rel = ch["filename"]
            self.assertEqual(os.path.dirname(rel), "chunks")
            base = os.path.basename(rel)
            self.assertTrue(base.startswith("chunk_"))
            self.assertTrue(base.endswith(".parquet"))

            path = os.path.join(loader.DATA_ROOT, rel)
            self.assertTrue(os.path.exists(path), f"Chunk not found: {path}")
            self.assertTrue(path.lower().endswith(".parquet"))

            # Validate sha256 integrity recorded in manifest
            actual_sha = loader.hash_file_sha256(path)
            self.assertEqual(actual_sha, ch["sha256"])  # ensures file content correctness

            df = pd.read_parquet(path)

            # Schema checks
            self.assertEqual(list(df.columns), loader.KLINE_COLUMNS)
            self.assertEqual(len(df), ch["count"])

            # Start/End alignment
            self.assertEqual(int(df.iloc[0]["open_time"]), ch["start_ms"])  # type: ignore
            self.assertEqual(int(df.iloc[-1]["open_time"]), ch["end_ms"])   # type: ignore

            # Intra-chunk 1-minute spacing
            if len(df) > 1:
                diffs = df["open_time"].diff().dropna().astype(int)
                self.assertTrue((diffs == 60_000).all(), "Intra-chunk spacing is not 1 minute")

            # Cross-chunk continuity
            if prev_end_ms is not None:
                self.assertEqual(ch["start_ms"], prev_end_ms + 60_000)
            prev_end_ms = ch["end_ms"]

            total_rows += len(df)
            combined_frames.append(df)

        # Combined dataset checks
        combined = pd.concat(combined_frames, ignore_index=True)
        # Sort by open_time (should already be sorted)
        combined = combined.sort_values("open_time").reset_index(drop=True)

        # Uniqueness and boundaries
        self.assertTrue(combined["open_time"].is_unique)
        self.assertEqual(int(combined.iloc[0]["open_time"]), start_ms)
        self.assertEqual(int(combined.iloc[-1]["open_time"]), end_ms)
        self.assertEqual(len(combined), expected_rows)

        if len(combined) > 1:
            diffs = combined["open_time"].diff().dropna().astype(int)
            self.assertTrue((diffs == 60_000).all(), "Global spacing is not 1 minute")

        # Basic dtype sanity for numeric columns (except 'ignore' which may be int)
        numeric_cols = [c for c in loader.KLINE_COLUMNS if c != "ignore"]
        for c in numeric_cols:
            self.assertTrue(pd.api.types.is_numeric_dtype(combined[c]), f"Column {c} should be numeric")

        # And verify the verify_continuity helper
        self.assertTrue(loader.verify_continuity(self.logger))


if __name__ == "__main__":
    unittest.main(verbosity=2)
