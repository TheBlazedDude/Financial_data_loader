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


@unittest.skipUnless(HAS_PARQUET, "pyarrow is required for manifest integrity tests")
class ManifestIntegrityTest(unittest.TestCase):
    def setUp(self):
        self.tempdir = tempfile.mkdtemp(prefix="loader_manifest_")
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

    def test_manifest_filesystem_parity_detects_missing_and_orphan_files(self):
        # Build a small valid dataset of 1 chunk
        start_ms = loader.iso_to_ms("2020-01-01T00:00:00Z")
        df = self._make_df(start_ms, 5)
        path, sha = loader.write_chunk_parquet(df, 1, int(df.iloc[0]["open_time"]), int(df.iloc[-1]["open_time"]), self.logger)
        rel = os.path.relpath(path, loader.DATA_ROOT)
        manifest = {
            "exchange": loader.EXCHANGE_ID,
            "symbol": "BTC/USDT",
            "timeframe": loader.TIMEFRAME,
            "chunk_size": 5,
            "format": "parquet",
            "columns": loader.KLINE_COLUMNS,
            "chunks": [
                {
                    "index": 1,
                    "filename": rel,
                    "start_ms": int(df.iloc[0]["open_time"]),
                    "end_ms": int(df.iloc[-1]["open_time"]),
                    "count": len(df),
                    "sha256": sha,
                }
            ],
            "last_candle_ms": int(df.iloc[-1]["open_time"]),
            "created_utc": "",
            "updated_utc": "",
        }
        loader.save_manifest(manifest)
        # Verify OK initially
        self.assertTrue(loader.verify_manifest(self.logger))
        # Corrupt: delete file and create an orphan file
        os.remove(path)
        orphan_path = os.path.join(loader.CHUNKS_DIR, "chunk_999999_0_0.parquet")
        self._make_df(start_ms, 1).to_parquet(orphan_path, index=False)
        # Re-run -> should fail
        self.assertFalse(loader.verify_manifest(self.logger))

    def test_filename_metadata_consistency(self):
        start_ms = loader.iso_to_ms("2020-01-02T00:00:00Z")
        df = self._make_df(start_ms, 3)
        path, sha = loader.write_chunk_parquet(df, 1, int(df.iloc[0]["open_time"]), int(df.iloc[-1]["open_time"]), self.logger)
        # Tamper filename: rename to wrong start_ms
        bad_name = os.path.join(loader.CHUNKS_DIR, "chunk_000001_0_0.parquet")
        os.replace(path, bad_name)
        rel = os.path.relpath(bad_name, loader.DATA_ROOT)
        manifest = loader.load_manifest()
        manifest["chunks"] = [{
            "index": 1,
            "filename": rel,
            "start_ms": int(df.iloc[0]["open_time"]),
            "end_ms": int(df.iloc[-1]["open_time"]),
            "count": len(df),
            "sha256": loader.hash_file_sha256(bad_name),
        }]
        loader.save_manifest(manifest)
        self.assertFalse(loader.verify_manifest(self.logger))

    def test_duplicate_open_time_detection(self):
        # Create a chunk with a duplicated minute
        t0 = loader.iso_to_ms("2020-01-03T00:00:00Z")
        df = self._make_df(t0, 5)
        # duplicate last row's open_time in a new row
        dup = df.iloc[-1].copy()
        dup["open_time"] = int(df.iloc[-2]["open_time"])  # duplicate a previous minute
        df_bad = pd.concat([df.iloc[:-1], pd.DataFrame([dup])], ignore_index=True)
        df_bad = loader.ensure_numeric_schema(df_bad, self.logger)
        path, sha = loader.write_chunk_parquet(df_bad, 1, int(df_bad.iloc[0]["open_time"]), int(df_bad.iloc[-1]["open_time"]), self.logger)
        rel = os.path.relpath(path, loader.DATA_ROOT)
        manifest = loader.load_manifest()
        manifest["chunks"] = [{
            "index": 1,
            "filename": rel,
            "start_ms": int(df_bad.iloc[0]["open_time"]),
            "end_ms": int(df_bad.iloc[-1]["open_time"]),
            "count": len(df_bad),
            "sha256": loader.hash_file_sha256(path),
        }]
        loader.save_manifest(manifest)
        # verify_manifest should fail via schema/value check
        self.assertFalse(loader.verify_manifest(self.logger))


if __name__ == "__main__":
    unittest.main(verbosity=2)
