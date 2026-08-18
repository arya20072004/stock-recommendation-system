import os
import json
import unittest
from unittest.mock import patch, MagicMock
from datetime import datetime, timezone

from src.ml.model_registry import reconcile_all_manifests
from src.ml.history import _verify_production_readiness
from src.data.nifty50 import TICKERS
from src.features.router import get_feature_pipeline_hash

class TestProductionGateRegression(unittest.TestCase):
    def setUp(self):
        self.db = MagicMock()
        self.canonical_hash = get_feature_pipeline_hash("v1")
        
        # Mocks
        self.patches = [
            patch('src.ml.history.os.path.exists'),
            patch('builtins.open', new_callable=unittest.mock.mock_open),
            patch('src.ml.model_registry.read_active_manifest'),
            patch('src.ml.model_registry.validate_bundle'),
            patch('src.ml.model_registry.update_manifest_atomically'),
            patch('src.ml.model_registry.get_active_manifest_path')
        ]
        self.mocks = [p.start() for p in self.patches]
        self.mock_exists, self.mock_open, self.mock_read_manifest, self.mock_validate_bundle, self.mock_update_manifest, self.mock_get_path = self.mocks
        
        self.mock_validate_bundle.return_value = True
        self.mock_exists.return_value = True
        self.mock_get_path.return_value = "/tmp/fake.json"
        
        # Default: simulate successful lock
        self.db.model_locks.insert_one.return_value = MagicMock()
        self.db.model_locks.delete_one.return_value = MagicMock()
        
        # 51 ACTIVE records in MongoDB (all NEW)
        self.new_records = []
        for ticker in TICKERS:
            self.new_records.append({
                "ticker": ticker,
                "status": "ACTIVE",
                "version": "v3",
                "model_hash": "new_m_" + ticker,
                "feature_hash": "new_f_" + ticker,
                "feature_pipeline_version": "v1",
                "feature_pipeline_hash": self.canonical_hash
            })
            
        # Filesystem manifests (OLD)
        self.old_manifests = {}
        for ticker in TICKERS:
            self.old_manifests[ticker] = {
                "ticker": ticker,
                "model_version": "v2",
                "model_hash": "old_m_" + ticker,
                "feature_hash": "old_f_" + ticker,
                "feature_pipeline_version": "v1",
                "feature_pipeline_hash": self.canonical_hash
            }

        # Repaired manifests (NEW)
        self.repaired_manifests = {}
        for ticker in TICKERS:
            self.repaired_manifests[ticker] = {
                "ticker": ticker,
                "model_version": "v3",
                "model_hash": "new_m_" + ticker,
                "feature_hash": "new_f_" + ticker,
                "feature_pipeline_version": "v1",
                "feature_pipeline_hash": self.canonical_hash
            }
            
    def tearDown(self):
        for p in self.patches:
            p.stop()
            
    def test_production_gate_regression(self):
        # 1. Setup MongoDB NEW and FS OLD
        # when _verify_production_readiness iterates over active records:
        self.db.model_registry.find.return_value = self.new_records
        
        def mock_read_manifest(ticker):
            return self.old_manifests.get(ticker)
        self.mock_read_manifest.side_effect = mock_read_manifest
        
        with patch('json.load') as mock_json_load:
            # First, json.load returns OLD manifests (simulate STATE B)
            mock_json_load.side_effect = lambda f: list(self.old_manifests.values())[0] # it'll fail on first mismatch anyway
            
            # _verify_production_readiness should FAIL (State B)
            with self.assertRaises(RuntimeError) as context:
                _verify_production_readiness(self.db)
            self.assertIn("PRODUCTION INFERENCE BLOCKED", str(context.exception))
            
            # 2. Reconcile
            # db.model_registry.find for sync_manifest must return specific records
            # Since both functions call find, we need to handle it based on arguments
            def mock_find(*args, **kwargs):
                query = args[0]
                if "ticker" in query:
                    return [r for r in self.new_records if r["ticker"] == query["ticker"]]
                return self.new_records
            self.db.model_registry.find.side_effect = mock_find
            
            # When update_manifest_atomically is called, we want to simulate repair
            def mock_update(*args, **kwargs):
                self.mock_read_manifest.side_effect = lambda t: self.repaired_manifests.get(t)
            self.mock_update_manifest.side_effect = mock_update
            
            success = reconcile_all_manifests(self.db)
            self.assertTrue(success)
            self.assertEqual(self.mock_update_manifest.call_count, len(TICKERS))
            
            # 3. Verify gate again (now with repaired manifests)
            def smarter_json_load(f):
                nonlocal count
                res = self.repaired_manifests[TICKERS[count]]
                count += 1
                return res
            count = 0
            mock_json_load.side_effect = smarter_json_load
            
            # This should PASS
            _verify_production_readiness(self.db)
            
if __name__ == '__main__':
    unittest.main()
