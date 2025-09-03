import os
import tempfile
import shutil
import unittest
from typing import List

import pandas as pd

try:
    import pyarrow  # noqa: F401
    HAS_PARQUET = True
except Exception:
    HAS_PARQUET = False

import binance_usdm_loader as loader


@unittest.skipUnless(HAS_PARQUET, "pyarrow is required for resource tests")
class ResourceAndParquetTest(unittest.TestCase):
    def setUp(self):
        # Temp workspace
        self.tempdir = tempfile.mkdtemp(prefix="loader_res_")
        self.data_root = os.path.join(self.tempdir, "data", loader.EXCHANGE_ID, "BTCUSDT", loader.TIMEFRAME)
        self.chunks_dir = os.path.join(self.data_root, "chunks")
        self.logs_dir = os.path.join(self.tempdir, "logs")
        os.makedirs(self.chunks_dir, exist_ok=True)
        os.makedirs(self.logs_dir, exist_ok=True)

        # Redirect paths in module
        loader.DATA_ROOT = self.data_root
        loader.CHUNKS_DIR = self.chunks_dir
        loader.MANIFEST_FILE = os.path.join(self.data_root, "manifest.json")
        loader.LOG_DIR = self.logs_dir
        loader.LOG_FILE = os.path.join(self.logs_dir, "loader.log")
        loader.COMBINED_DIR = os.path.join(self.data_root, "combined")
        loader.COMBINED_FILE = os.path.join(loader.COMBINED_DIR, "BTCUSDT_1m_all.parquet")

        self.logger = loader.setup_logging(verbose=False)

        # Avoid ccxt/network
        loader.create_exchange = lambda logger: (object(), "BTC/USDT")

    def tearDown(self):
        try:
            try:
                loader.shutdown_logging()
            except Exception:
                pass
            shutil.rmtree(self.tempdir)
        except Exception:
            pass

    def test_imports_and_dependencies(self):
        # pandas is required
        self.assertIsNotNone(pd)
        # pyarrow must be importable for our parquet ops
        self.assertTrue(HAS_PARQUET)
        # ccxt can be missing at import-time of module but create_exchange is patched in tests
        # Ensure helper functions exist
        self.assertTrue(callable(loader._atomic_write_parquet))
        self.assertTrue(callable(loader._read_parquet))

    def test_atomic_parquet_write_read_and_close(self):
        # Create minimal DataFrame matching schema
        rows: List[List[float]] = []
        t0 = 1577836800000  # 2020-01-01T00:00:00Z
        for i in range(3):
            t = t0 + i * 60_000
            rows.append([
                t, 100.0, 110.0, 90.0, 105.0, 1.0,
                t + 59_999, 100.0, 1, 0.5, 50.0, 0
            ])
        df = pd.DataFrame(rows, columns=loader.KLINE_COLUMNS)
        df = loader.ensure_numeric_schema(df, self.logger)

        # Write using atomic helper
        path = os.path.join(self.chunks_dir, "tmp_atomic.parquet")
        loader._atomic_write_parquet(df, path, self.logger)

        # No lingering .tmp
        self.assertFalse(os.path.exists(path + ".tmp"))
        self.assertTrue(os.path.exists(path))

        # Read back using helper and verify content
        df2 = loader._read_parquet(path)
        self.assertEqual(list(df2.columns), loader.KLINE_COLUMNS)
        self.assertEqual(len(df2), len(df))
        self.assertTrue((df2["open_time"].astype(int).tolist() == df["open_time"].astype(int).tolist()))

        # Ensure file is not locked: we should be able to replace/delete it immediately on Windows
        os.remove(path)
        self.assertFalse(os.path.exists(path))

    def test_combined_build_and_close_handles(self):
        # Prepare two small chunks and a manifest, then combine and ensure temp file removed and combined removable
        # Create two chunk files via write_chunk_parquet
        manifest = {
            "exchange": loader.EXCHANGE_ID,
            "symbol": "BTC/USDT",
            "timeframe": loader.TIMEFRAME,
            "chunk_size": 5,
            "format": "parquet",
            "columns": loader.KLINE_COLUMNS,
            "chunks": [],
            "last_candle_ms": None,
            "created_utc": "",
            "updated_utc": "",
        }
        os.makedirs(loader.COMBINED_DIR, exist_ok=True)

        def make_df(start_ms: int, n: int) -> pd.DataFrame:
            rows = []
            t = start_ms
            for _ in range(n):
                rows.append([
                    t, 100.0, 110.0, 90.0, 105.0, 1.0,
                    t + 59_999, 100.0, 1, 0.5, 50.0, 0
                ])
                t += 60_000
            d = pd.DataFrame(rows, columns=loader.KLINE_COLUMNS)
            return loader.ensure_numeric_schema(d, self.logger)

        df1 = make_df(1609459200000, 5)  # 2021-01-01 00:00Z
        p1, sha1 = loader.write_chunk_parquet(df1, 1, int(df1.iloc[0]["open_time"]), int(df1.iloc[-1]["open_time"]), self.logger)
        df2 = make_df(1609462500000, 3)  # +55 minutes, intentional gap but combine doesn't care about continuity
        p2, sha2 = loader.write_chunk_parquet(df2, 2, int(df2.iloc[0]["open_time"]), int(df2.iloc[-1]["open_time"]), self.logger)

        manifest["chunks"].append({
            "index": 1,
            "filename": os.path.relpath(p1, loader.DATA_ROOT),
            "start_ms": int(df1.iloc[0]["open_time"]),
            "end_ms": int(df1.iloc[-1]["open_time"]),
            "count": len(df1),
            "sha256": sha1,
        })
        manifest["chunks"].append({
            "index": 2,
            "filename": os.path.relpath(p2, loader.DATA_ROOT),
            "start_ms": int(df2.iloc[0]["open_time"]),
            "end_ms": int(df2.iloc[-1]["open_time"]),
            "count": len(df2),
            "sha256": sha2,
        })

        combined = loader.build_combined_parquet(manifest, self.logger)
        # Either None (if pyarrow missing) or a path
        if combined:
            self.assertTrue(os.path.exists(combined))
            # Ensure temp file is gone
            self.assertFalse(os.path.exists(combined.replace(".parquet", ".tmp.parquet")))
            # Read and then remove (no open handle)
            dfc = pd.read_parquet(combined)
            self.assertGreaterEqual(len(dfc), len(df1))
            os.remove(combined)
            self.assertFalse(os.path.exists(combined))


if __name__ == "__main__":
    unittest.main(verbosity=2)
