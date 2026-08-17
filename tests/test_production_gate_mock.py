import unittest
from unittest.mock import MagicMock, patch
import os
import json

from src.ml.history import _verify_production_readiness
from src.features.router import get_feature_pipeline_hash

class TestProductionGate(unittest.TestCase):
    
    def setUp(self):
        self.current_version = "v1"
        self.current_hash = get_feature_pipeline_hash(self.current_version)
        self.mock_db = MagicMock()
        
        # Base expected 51 tickers
        from src.data.nifty50 import TICKERS
        self.tickers = TICKERS
        
        # Build 51 valid records
        self.valid_records = []
        for t in self.tickers:
            self.valid_records.append({
                "ticker": t,
                "status": "ACTIVE",
                "feature_pipeline_version": self.current_version,
                "feature_pipeline_hash": self.current_hash,
                "model_hash": "model_123",
                "feature_hash": "feat_123",
                "version": "v1.0.0"
            })
            
        self.mock_db.model_registry.find.return_value = self.valid_records
        
        # Mock filesystem
        self.patcher_exists = patch("os.path.exists")
        self.mock_exists = self.patcher_exists.start()
        self.mock_exists.return_value = True
        
        self.patcher_open = patch("builtins.open")
        self.mock_open = self.patcher_open.start()
        
        # We also need to mock get_feature_pipeline_hash to avoid builtins.open breaking it
        self.patcher_hash = patch("src.ml.history.get_feature_pipeline_hash")
        self.mock_hash = self.patcher_hash.start()
        self.mock_hash.return_value = self.current_hash
        
        self.valid_manifest = {
            "status": "ACTIVE",
            "feature_pipeline_version": self.current_version,
            "feature_pipeline_hash": self.current_hash,
            "ticker": "RELIANCE",
            "model_version": "v1.0.0",
            "model_hash": "model_123",
            "feature_hash": "feat_123"
        }
        
        # Mock json load
        self.patcher_json = patch("json.load")
        self.mock_json = self.patcher_json.start()
        
        def load_se(f):
            m = self.valid_records.pop(0)
            self.valid_records.append(m) # cycle it
            m = m.copy()
            m["model_version"] = m["version"]
            return m
        self.mock_json.side_effect = load_se

    def tearDown(self):
        self.patcher_exists.stop()
        self.patcher_open.stop()
        self.patcher_json.stop()
        self.patcher_hash.stop()

    def test_complete_state_pass(self):
        # Should pass
        _verify_production_readiness(self.mock_db)

        
    def test_old_hash_present(self):
        # 1 OLD_HASH
        records = list(self.valid_records)
        records[0]["feature_pipeline_hash"] = "OLD_HASH_VALUE"
        self.mock_db.model_registry.find.return_value = records
        with self.assertRaisesRegex(RuntimeError, "wrong pipeline hash in MongoDB"):
            _verify_production_readiness(self.mock_db)

    def test_missing_mongodb_record(self):
        # 50 records
        self.mock_db.model_registry.find.return_value = self.valid_records[:-1]
        with self.assertRaisesRegex(RuntimeError, "Expected 51 ACTIVE records"):
            _verify_production_readiness(self.mock_db)
            
    def test_duplicate_active_record(self):
        # 52 records
        records = list(self.valid_records)
        records.append(self.valid_records[0])
        self.mock_db.model_registry.find.return_value = records
        with self.assertRaisesRegex(RuntimeError, "Expected 51 ACTIVE records"):
            _verify_production_readiness(self.mock_db)
            
    def test_missing_filesystem_manifest(self):
        def exists_side_effect(path):
            if "RELIANCE_active.json" in path:
                return False
            return True
        self.mock_exists.side_effect = exists_side_effect
        with self.assertRaisesRegex(RuntimeError, "missing filesystem manifest"):
            _verify_production_readiness(self.mock_db)
            
    def test_unexpected_hash_filesystem(self):
        def dynamic_json_load(f):
            m = self.valid_manifest.copy()
            m["feature_pipeline_hash"] = "WRONG"
            return m
        self.mock_json.side_effect = dynamic_json_load
        with self.assertRaisesRegex(RuntimeError, "wrong pipeline hash in filesystem"):
            _verify_production_readiness(self.mock_db)

    def test_wrong_pipeline_version(self):
        records = list(self.valid_records)
        records[0]["feature_pipeline_version"] = "v2"
        self.mock_db.model_registry.find.return_value = records
        with self.assertRaisesRegex(RuntimeError, "wrong pipeline version in MongoDB"):
            _verify_production_readiness(self.mock_db)
            
    def test_mongodb_fs_mismatch(self):
        def dynamic_json_load(f):
            m = self.valid_manifest.copy()
            m["model_hash"] = "MISMATCH"
            return m
        self.mock_json.side_effect = dynamic_json_load
        with self.assertRaisesRegex(RuntimeError, "mismatch in model_hash field"):
            _verify_production_readiness(self.mock_db)
            
    def test_missing_model_artifact(self):
        def exists_side_effect(path):
            if "model_" in path:
                return False
            return True
        self.mock_exists.side_effect = exists_side_effect
        with self.assertRaisesRegex(RuntimeError, "missing model artifact"):
            _verify_production_readiness(self.mock_db)
            
    def test_missing_feature_artifact(self):
        def exists_side_effect(path):
            if "features_" in path:
                return False
            return True
        self.mock_exists.side_effect = exists_side_effect
        with self.assertRaisesRegex(RuntimeError, "missing feature artifact"):
            _verify_production_readiness(self.mock_db)

if __name__ == "__main__":
    unittest.main()
