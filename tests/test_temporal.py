import unittest
import pandas as pd
from datetime import date
from src.features.v1.engineering import build_feature_row

class MockDB:
    def __init__(self, data):
        self.data = data
        self.historical_data = self
        self.news_articles = self
        self.sector_indices = self
        self.pcr_data = self
        self.fii_dii_data = self
        self.prediction_history = self
        self.prediction_provenance = self

    def find(self, *args, **kwargs):
        return self.data.get("historical_data", [])

class MockClient:
    def __getitem__(self, item):
        return self

    def start_session(self):
        class MockSession:
            def __enter__(self): return self
            def __exit__(self, *args): pass
            def start_transaction(self): return self
        return MockSession()

    @property
    def pcr_data(self):
        class MockColl:
            def find(self, *args, **kwargs): return []
        return MockColl()

    @property
    def sector_indices(self):
        class MockColl:
            def find(self, *args, **kwargs): return []
        return MockColl()

    @property
    def fii_dii_data(self):
        class MockColl:
            def find(self, *args, **kwargs): return []
        return MockColl()

class TestTemporal(unittest.TestCase):
    def setUp(self):
        self.last_completed_session = date(2026, 8, 14)
        self.prediction_target_date = date(2026, 8, 17)
        self.ticker = "RELIANCE.NS"
        self.client = MockClient()
        
    def _create_db(self, row_override=None, missing_row=False):
        historical_data = []
        # Populate history
        for day in range(1, 15):
            dt = pd.Timestamp(f"2026-08-{day:02d}")
            if dt.weekday() < 5:  # skip weekends
                if missing_row and day == 14:
                    continue
                
                row = {
                    "ticker": self.ticker,
                    "date": dt,
                    "open": 100.0,
                    "high": 105.0,
                    "low": 95.0,
                    "close": 100.0,
                    "volume": 1000,
                }
                if day == 14 and row_override:
                    row.update(row_override)
                historical_data.append(row)
                
        return MockDB({"historical_data": historical_data})

    def test_complete_aug14_succeeds(self):
        db = self._create_db()
        try:
            # Note: _prepare_nifty_data and others will fail if we don't mock yfinance or have data,
            # but we can just check if ValueError is raised due to T-1 check.
            build_feature_row(self.ticker, self.client, db, self.last_completed_session, self.prediction_target_date)
        except ValueError as e:
            # We expect a failure down the line due to Nifty data fetching, but NOT from OHLCV integrity.
            self.assertNotIn("Missing last_completed_session", str(e))
            self.assertNotIn("Incomplete last_completed_session", str(e))
            self.assertNotIn("Invalid canonical close", str(e))

    def test_missing_aug14_row_fails(self):
        db = self._create_db(missing_row=True)
        with self.assertRaisesRegex(ValueError, "Missing last_completed_session"):
            build_feature_row(self.ticker, self.client, db, self.last_completed_session, self.prediction_target_date)

    def test_aug14_open_nan_fails(self):
        db = self._create_db(row_override={"open": pd.NA})
        with self.assertRaisesRegex(ValueError, "Incomplete last_completed_session .* missing \\['open'\\]"):
            build_feature_row(self.ticker, self.client, db, self.last_completed_session, self.prediction_target_date)

    def test_aug14_high_nan_fails(self):
        db = self._create_db(row_override={"high": pd.NA})
        with self.assertRaisesRegex(ValueError, "Incomplete last_completed_session .* missing \\['high'\\]"):
            build_feature_row(self.ticker, self.client, db, self.last_completed_session, self.prediction_target_date)

    def test_aug14_close_zero_fails(self):
        db = self._create_db(row_override={"close": 0.0})
        with self.assertRaisesRegex(ValueError, "Invalid canonical close"):
            build_feature_row(self.ticker, self.client, db, self.last_completed_session, self.prediction_target_date)

if __name__ == "__main__":
    unittest.main()
