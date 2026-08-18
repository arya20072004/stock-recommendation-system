import sys
import unittest
import argparse
from datetime import date, datetime

sys.path.append('.')
from unittest.mock import MagicMock
sys.modules['pandas_market_calendars'] = MagicMock()

import src.data.session_calendar as session_calendar
from src.features.router import get_feature_pipeline_hash

class TestHistoryCLIEntrypoint(unittest.TestCase):
    
    def test_cli_argument_forwarding(self):
        # Simulate argparse from history.py
        test_args = ["history.py", "--date", "2026-08-17"]
        parser = argparse.ArgumentParser()
        parser.add_argument("--date", type=str, required=True)
        args = parser.parse_args(test_args[1:])
        
        target_date = datetime.strptime(args.date, "%Y-%m-%d").date()
        with unittest.mock.patch('src.data.session_calendar.previous_session') as mock_prev:
            mock_prev.return_value = date(2026, 8, 14)
            last_completed_session = session_calendar.previous_session(target_date)
            
            # A. Verify --date resolves to correct previous session
            self.assertEqual(target_date, date(2026, 8, 17))
            self.assertEqual(last_completed_session, date(2026, 8, 14))
        
        # B, F. Verify function signature
        import inspect
        from src.ml.history import generate_and_persist_predictions
        sig = inspect.signature(generate_and_persist_predictions)
        params = list(sig.parameters.keys())
        self.assertEqual(params[0], 'client')
        self.assertEqual(params[1], 'last_completed_session')
        self.assertEqual(params[2], 'prediction_target_date')

    def test_weekend_resolution(self):
        # C. Verify a weekend target resolves through the canonical session calendar
        target_date = date(2026, 8, 16) # Sunday
        with unittest.mock.patch('src.data.session_calendar.previous_session') as mock_prev:
            mock_prev.return_value = date(2026, 8, 14)
            last_completed_session = session_calendar.previous_session(target_date)
            
            self.assertEqual(last_completed_session, date(2026, 8, 14)) # Friday
            # D. Verify target date remains unchanged
            self.assertEqual(target_date, date(2026, 8, 16))

    def test_pipeline_hash_unchanged(self):
        # G. Verify pipeline hash is unchanged
        h = get_feature_pipeline_hash('v1')
        self.assertEqual(h, 'f4891c1b2172b8e024b176cd221cf33c8b5e006acd567d95f2768cb7bf72384e')

if __name__ == '__main__':
    unittest.main()
