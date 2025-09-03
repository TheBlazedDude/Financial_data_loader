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


@unittest.skipUnless(HAS_PARQUET, "pyarrow is required for schema sanity tests")
class SchemaSanityTest(unittest.TestCase):
    def setUp(self):
        self.tempdir = tempfile.mkdtemp(prefix="loader_schema_")
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

    def test_negative_volume_flagged(self):
        t0 = loader.iso_to_ms("2020-01-04T00:00:00Z")
        df = self._make_df(t0, 3)
        df.loc[1, "volume"] = -1.0
        ok, msg = loader.verify_schema_and_values(df)
        self.assertFalse(ok)
        self.assertIn("negative", msg)

    def test_unsorted_rows_flagged(self):
        t0 = loader.iso_to_ms("2020-01-05T00:00:00Z")
        df = self._make_df(t0, 3)
        df = df.iloc[[1,0,2]].reset_index(drop=True)
        ok, msg = loader.verify_schema_and_values(df)
        self.assertFalse(ok)
        self.assertIn("sorted", msg)

    def test_close_time_equals_open_time_plus_59999(self):
        t0 = loader.iso_to_ms("2020-01-06T00:00:00Z")
        df = self._make_df(t0, 3)
        df.loc[2, "close_time"] = df.loc[2, "open_time"] + 123
        ok, msg = loader.verify_schema_and_values(df)
        self.assertFalse(ok)
        self.assertIn("close_time", msg)


if __name__ == "__main__":
    unittest.main(verbosity=2)
