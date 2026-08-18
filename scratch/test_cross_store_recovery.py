import os
import json
import unittest
from unittest.mock import patch, MagicMock
from datetime import datetime, timezone

from src.ml.model_registry import sync_manifest, reconcile_all_manifests
from src.features.router import get_feature_pipeline_hash

class TestCrossStoreRecovery(unittest.TestCase):
    def setUp(self):
        self.db = MagicMock()
        self.ticker = "TEST.NS"
        self.owner_id = "test-owner"
        self.canonical_hash = get_feature_pipeline_hash("v1")
        
        self.valid_active_record = {
            "ticker": self.ticker,
            "status": "ACTIVE",
            "version": "v3",
            "model_hash": "abcd1234abcd",
            "feature_hash": "f" * 64,
            "feature_pipeline_version": "v1",
            "feature_pipeline_hash": self.canonical_hash,
            "dataset_hash": "d" * 64,
            "metrics": {"f1_macro": 0.85},
            "promoted_at": datetime.now(timezone.utc).isoformat()
        }
        
        self.valid_manifest = {
            "ticker": self.ticker,
            "model_version": "v3",
            "model_hash": "abcd1234abcd",
            "feature_hash": "f" * 64,
            "feature_pipeline_version": "v1",
            "feature_pipeline_hash": self.canonical_hash
        }
        
        self.old_manifest = {
            "ticker": self.ticker,
            "model_version": "v2",
            "model_hash": "old_model",
            "feature_hash": "old_feat",
            "feature_pipeline_version": "v1",
            "feature_pipeline_hash": self.canonical_hash
        }

        self.patches = [
            patch('src.ml.model_registry.read_active_manifest'),
            patch('src.ml.model_registry.validate_bundle'),
            patch('src.ml.model_registry.update_manifest_atomically'),
            patch('src.ml.model_registry.get_active_manifest_path'),
            patch('os.path.exists'),
            patch('os.remove')
        ]
        self.mocks = [p.start() for p in self.patches]
        (self.mock_read_manifest, self.mock_validate_bundle, 
         self.mock_update_manifest, self.mock_get_path, 
         self.mock_exists, self.mock_remove) = self.mocks
         
        self.mock_validate_bundle.return_value = True
        self.mock_get_path.return_value = f"/tmp/{self.ticker}_active.json"
        
        # Default: simulate successful lock
        self.db.model_locks.insert_one.return_value = MagicMock()
        self.db.model_locks.delete_one.return_value = MagicMock()

    def tearDown(self):
        for p in self.patches:
            p.stop()

    def test_01_state_a(self):
        # State A: MongoDB OLD, FS OLD -> no-op because no active record matches?
        # Wait, if MongoDB is OLD ACTIVE and FS is OLD ACTIVE, they match perfectly!
        old_record = dict(self.valid_active_record, version="v2", model_hash="old_model", feature_hash="old_feat")
        self.db.model_registry.find.return_value = [old_record]
        self.mock_read_manifest.return_value = self.old_manifest
        
        res = sync_manifest(self.db, self.ticker, self.owner_id)
        self.assertTrue(res)
        self.mock_update_manifest.assert_not_called()

    def test_02_state_b(self):
        # State B: MongoDB NEW, FS OLD -> repair
        self.db.model_registry.find.return_value = [self.valid_active_record]
        
        # First read returns old manifest. Second read (post-repair verification) returns repaired manifest.
        self.mock_read_manifest.side_effect = [self.old_manifest, self.valid_manifest]
        
        res = sync_manifest(self.db, self.ticker, self.owner_id)
        self.assertTrue(res)
        self.mock_update_manifest.assert_called_once()
        args, _ = self.mock_update_manifest.call_args
        self.assertEqual(args[1]["model_version"], "v3")

    def test_03_state_c(self):
        # State C: MongoDB NEW, FS NEW -> no-op
        self.db.model_registry.find.return_value = [self.valid_active_record]
        self.mock_read_manifest.return_value = self.valid_manifest
        
        res = sync_manifest(self.db, self.ticker, self.owner_id)
        self.assertTrue(res)
        self.mock_update_manifest.assert_not_called()

    def test_04_missing_filesystem_manifest(self):
        # MongoDB NEW, FS missing -> repair
        self.db.model_registry.find.return_value = [self.valid_active_record]
        self.mock_read_manifest.side_effect = [None, self.valid_manifest]
        
        res = sync_manifest(self.db, self.ticker, self.owner_id)
        self.assertTrue(res)
        self.mock_update_manifest.assert_called_once()

    def test_05_corrupted_filesystem_manifest(self):
        # MongoDB NEW, FS corrupted (empty or missing fields) -> repair
        self.db.model_registry.find.return_value = [self.valid_active_record]
        self.mock_read_manifest.side_effect = [{"corrupted": True}, self.valid_manifest]
        
        res = sync_manifest(self.db, self.ticker, self.owner_id)
        self.assertTrue(res)
        self.mock_update_manifest.assert_called_once()

    def test_06_wrong_model_hash_in_fs(self):
        self.db.model_registry.find.return_value = [self.valid_active_record]
        wrong_manifest = dict(self.valid_manifest, model_hash="wrong")
        self.mock_read_manifest.side_effect = [wrong_manifest, self.valid_manifest]
        
        res = sync_manifest(self.db, self.ticker, self.owner_id)
        self.assertTrue(res)
        self.mock_update_manifest.assert_called_once()

    def test_07_wrong_feature_hash_in_fs(self):
        self.db.model_registry.find.return_value = [self.valid_active_record]
        wrong_manifest = dict(self.valid_manifest, feature_hash="wrong")
        self.mock_read_manifest.side_effect = [wrong_manifest, self.valid_manifest]
        
        res = sync_manifest(self.db, self.ticker, self.owner_id)
        self.assertTrue(res)
        self.mock_update_manifest.assert_called_once()

    def test_08_wrong_pipeline_hash_in_fs(self):
        self.db.model_registry.find.return_value = [self.valid_active_record]
        wrong_manifest = dict(self.valid_manifest, feature_pipeline_hash="wrong")
        self.mock_read_manifest.side_effect = [wrong_manifest, self.valid_manifest]
        
        res = sync_manifest(self.db, self.ticker, self.owner_id)
        self.assertTrue(res)
        self.mock_update_manifest.assert_called_once()

    def test_09_missing_mongodb_active_record(self):
        self.db.model_registry.find.return_value = [] # Missing
        self.mock_exists.return_value = True
        self.mock_read_manifest.return_value = self.valid_manifest
        
        res = sync_manifest(self.db, self.ticker, self.owner_id)
        self.assertTrue(res)
        self.mock_remove.assert_called_once()
        self.mock_update_manifest.assert_not_called()

    def test_10_duplicate_mongodb_active_records(self):
        self.db.model_registry.find.return_value = [self.valid_active_record, self.valid_active_record]
        self.mock_read_manifest.return_value = None
        
        res = sync_manifest(self.db, self.ticker, self.owner_id)
        self.assertFalse(res)
        self.mock_update_manifest.assert_not_called()

    def test_11_and_12_missing_or_invalid_model_feature_artifact(self):
        self.db.model_registry.find.return_value = [self.valid_active_record]
        self.mock_read_manifest.return_value = None
        self.mock_validate_bundle.return_value = False
        
        res = sync_manifest(self.db, self.ticker, self.owner_id)
        self.assertFalse(res)
        self.mock_update_manifest.assert_not_called()

    def test_14_wrong_pipeline_version(self):
        bad_record = dict(self.valid_active_record, feature_pipeline_version="v0")
        self.db.model_registry.find.return_value = [bad_record]
        self.mock_read_manifest.return_value = None
        
        res = sync_manifest(self.db, self.ticker, self.owner_id)
        self.assertFalse(res)
        self.mock_update_manifest.assert_not_called()

    def test_15_wrong_canonical_pipeline_hash(self):
        bad_record = dict(self.valid_active_record, feature_pipeline_hash="wrong")
        self.db.model_registry.find.return_value = [bad_record]
        self.mock_read_manifest.return_value = None
        
        res = sync_manifest(self.db, self.ticker, self.owner_id)
        self.assertFalse(res)
        self.mock_update_manifest.assert_not_called()

    def test_16_os_replace_failure(self):
        self.db.model_registry.find.return_value = [self.valid_active_record]
        self.mock_read_manifest.return_value = None
        self.mock_update_manifest.side_effect = Exception("os.replace failed")
        
        res = sync_manifest(self.db, self.ticker, self.owner_id)
        self.assertFalse(res)

    def test_17_repeated_reconciliation(self):
        # First call repairs
        self.db.model_registry.find.return_value = [self.valid_active_record]
        self.mock_read_manifest.side_effect = [None, self.valid_manifest]
        res1 = sync_manifest(self.db, self.ticker, self.owner_id)
        self.assertTrue(res1)
        self.assertEqual(self.mock_update_manifest.call_count, 1)
        
        # Second call does nothing
        self.mock_read_manifest.side_effect = [self.valid_manifest]
        res2 = sync_manifest(self.db, self.ticker, self.owner_id)
        self.assertTrue(res2)
        self.assertEqual(self.mock_update_manifest.call_count, 1)

if __name__ == '__main__':
    unittest.main()
