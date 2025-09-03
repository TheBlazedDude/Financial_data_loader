import os
import tempfile
import shutil
import unittest

import pandas as pd

try:
    import pyarrow  # noqa: F401
    HAS_PARQUET = True
except Exception:
    HAS_PARQUET = False

import binance_usdm_loader as loader


@unittest.skipUnless(HAS_PARQUET, "pyarrow is required for live append tests")
class LiveAppendInvariantsTest(unittest.TestCase):
    def setUp(self):
        self.tempdir = tempfile.mkdtemp(prefix="loader_live_")
        self.data_root = os.path.join(self.tempdir, "data", loader.EXCHANGE_ID, "BTCUSDT", loader.TIMEFRAME)
        self.chunks_dir = os.path.join(self.data_root, "chunks")
        self.logs_dir = os.path.join(self.tempdir, "logs")
        os.makedirs(self.chunks_dir, exist_ok=True)
        os.makedirs(self.logs_dir, exist_ok=True)
        loader.DATA_ROOT = self.data_root
        loader.CHUNKS_DIR = self.chunks_dir
        loader.MANIFEST_FILE = os.path.join(self.data_root, "manifest.json")
        loader.LOG_DIR = self.logs_dir
        loader.LOG_FILE = os.path.join(self.logs_dir, "loader.log")
        loader.COMBINED_DIR = os.path.join(self.data_root, "combined")
        loader.COMBINED_FILE = os.path.join(loader.COMBINED_DIR, "BTCUSDT_1m_all.parquet")
        self.logger = loader.setup_logging(verbose=False)
        loader.create_exchange = lambda logger: (object(), "BTC/USDT")

    def tearDown(self):
        try:
            loader.shutdown_logging()
        except Exception:
            pass
        shutil.rmtree(self.tempdir, ignore_errors=True)

    def _make_df(self, start_ms: int, n: int) -> pd.DataFrame:
        rows = []
        t = start_ms
        for _ in range(n):
            rows.append([t, 100.0, 110.0, 90.0, 105.0, 1.0, t + 59_999, 100.0, 1, 0.5, 50.0, 0])
            t += 60_000
        df = pd.DataFrame(rows, columns=loader.KLINE_COLUMNS)
        return loader.ensure_numeric_schema(df, self.logger)

    def test_no_new_chunk_when_last_chunk_not_full(self):
        # Create a partial chunk of size 3 with chunk_size=5
        t0 = loader.iso_to_ms("2020-01-01T00:00:00Z")
        df = self._make_df(t0, 3)
        path, sha = loader.write_chunk_parquet(df, 1, int(df.iloc[0]["open_time"]), int(df.iloc[-1]["open_time"]), self.logger)
        rel = os.path.relpath(path, loader.DATA_ROOT)
        manifest = {
            "exchange": loader.EXCHANGE_ID,
            "symbol": "BTC/USDT",
            "timeframe": loader.TIMEFRAME,
            "chunk_size": 5,
            "format": "parquet",
            "columns": loader.KLINE_COLUMNS,
            "chunks": [{
                "index": 1,
                "filename": rel,
                "start_ms": int(df.iloc[0]["open_time"]),
                "end_ms": int(df.iloc[-1]["open_time"]),
                "count": len(df),
                "sha256": sha,
            }],
            "last_candle_ms": int(df.iloc[-1]["open_time"]),
            "created_utc": "",
            "updated_utc": "",
        }
        loader.save_manifest(manifest)
        # Simulate appending fewer than 2 rows (so last chunk still not full)
        df_more = self._make_df(t0 + 3*60_000, 1)
        # Emulate live appending logic: append to last chunk
        prev_path = path
        prev_df = loader._read_parquet(prev_path)
        new_df = pd.concat([prev_df, df_more], ignore_index=True)
        loader._atomic_write_parquet(new_df, prev_path, self.logger)
        man = loader.load_manifest()
        man["chunks"][-1]["count"] = len(new_df)
        man["chunks"][-1]["end_ms"] = int(new_df.iloc[-1]["open_time"])  # keep contiguous
        man["chunks"][-1]["sha256"] = loader.hash_file_sha256(prev_path)
        loader.save_manifest(man)
        # Now attempt to incorrectly create a new chunk while last is not full; our invariant should prevent this in code paths
        # Here we only assert that our check would detect it via verify_manifest + continuity
        # Manually craft a bad second chunk
        bad2 = self._make_df(int(new_df.iloc[-1]["open_time"]) + 60_000, 1)
        p2, sha2 = loader.write_chunk_parquet(bad2, 2, int(bad2.iloc[0]["open_time"]), int(bad2.iloc[-1]["open_time"]), self.logger)
        man = loader.load_manifest()
        man["chunks"].append({
            "index": 2,
            "filename": os.path.relpath(p2, loader.DATA_ROOT),
            "start_ms": int(bad2.iloc[0]["open_time"]),
            "end_ms": int(bad2.iloc[-1]["open_time"]),
            "count": len(bad2),
            "sha256": sha2,
        })
        loader.save_manifest(man)
        # Continuity should fail because there is a gap larger than 1 minute (while last was still partial)
        self.assertFalse(loader.verify_continuity(self.logger))

    def test_recovery_from_tmp_files_on_startup(self):
        # Create dangling temp files
        tmp1 = os.path.join(self.chunks_dir, "dangling.tmp")
        tmp2 = os.path.join(os.path.join(self.data_root, "combined"), "file.tmp.parquet")
        os.makedirs(os.path.dirname(tmp2), exist_ok=True)
        open(tmp1, 'wb').close()
        open(tmp2, 'wb').close()
        # Re-run setup_logging which calls sweep_tmp_files
        self.logger = loader.setup_logging(verbose=False)
        self.assertFalse(os.path.exists(tmp1))
        self.assertFalse(os.path.exists(tmp2))


if __name__ == "__main__":
    unittest.main(verbosity=2)
