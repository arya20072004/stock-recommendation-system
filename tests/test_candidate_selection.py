import unittest
from unittest.mock import MagicMock
from src.ml.evaluate_candidate import select_current_candidate

class TestCandidateSelection(unittest.TestCase):
    def setUp(self):
        self.db = MagicMock()
        self.cutoff = "2025-08-07 00:00:00"
        self.db.model_registry.find.return_value = []

    def test_exactly_one_current_generation(self):
        self.db.model_registry.find.return_value = [
            {"ticker": "TICK", "status": "CANDIDATE", "dataset_date_end": self.cutoff, "model_hash": "hash1"}
        ]
        result = select_current_candidate(self.db, "TICK", self.cutoff)
        self.assertEqual(result, "hash1")

    def test_zero_current_generation(self):
        self.db.model_registry.find.return_value = []
        result = select_current_candidate(self.db, "TICK", self.cutoff)
        self.assertIsNone(result)

    def test_multiple_current_generation(self):
        self.db.model_registry.find.return_value = [
            {"ticker": "TICK", "status": "CANDIDATE", "dataset_date_end": self.cutoff, "model_hash": "hash1"},
            {"ticker": "TICK", "status": "CANDIDATE", "dataset_date_end": self.cutoff, "model_hash": "hash2"}
        ]
        with self.assertRaisesRegex(ValueError, "DUPLICATE CANDIDATES"):
            select_current_candidate(self.db, "TICK", self.cutoff)

    def test_historical_only_skip(self):
        self.db.model_registry.find.return_value = []
        result = select_current_candidate(self.db, "TICK", self.cutoff)
        self.assertIsNone(result)
        self.db.model_registry.find.assert_called_with({
            "ticker": "TICK", "status": "CANDIDATE", "dataset_date_end": self.cutoff
        })

    def test_historical_and_current_generation(self):
        self.db.model_registry.find.return_value = [
            {"ticker": "TICK", "status": "CANDIDATE", "dataset_date_end": self.cutoff, "model_hash": "curr_hash"}
        ]
        result = select_current_candidate(self.db, "TICK", self.cutoff)
        self.assertEqual(result, "curr_hash")

    def test_active_and_current_generation(self):
        self.db.model_registry.find.return_value = [
            {"ticker": "TICK", "status": "CANDIDATE", "dataset_date_end": self.cutoff, "model_hash": "cand_hash"}
        ]
        result = select_current_candidate(self.db, "TICK", self.cutoff)
        self.assertEqual(result, "cand_hash")

    def test_filesystem_mtime_ignored(self):
        self.db.model_registry.find.return_value = [
            {"ticker": "TICK", "status": "CANDIDATE", "dataset_date_end": self.cutoff, "model_hash": "reg_hash"}
        ]
        result = select_current_candidate(self.db, "TICK", self.cutoff)
        self.assertEqual(result, "reg_hash")

    def test_eternal_skip(self):
        self.db.model_registry.find.return_value = []
        result = select_current_candidate(self.db, "ETERNAL.NS", self.cutoff)
        self.assertIsNone(result)

    def test_jiofin_skip(self):
        self.db.model_registry.find.return_value = []
        result = select_current_candidate(self.db, "JIOFIN.NS", self.cutoff)
        self.assertIsNone(result)

    def test_maxhealth_skip(self):
        self.db.model_registry.find.return_value = []
        result = select_current_candidate(self.db, "MAXHEALTH.NS", self.cutoff)
        self.assertIsNone(result)

    def test_hash_returned_correctly(self):
        self.db.model_registry.find.return_value = [
            {"ticker": "TICK", "status": "CANDIDATE", "dataset_date_end": self.cutoff, "model_hash": "exact_hash_123"}
        ]
        result = select_current_candidate(self.db, "TICK", self.cutoff)
        self.assertEqual(result, "exact_hash_123")

if __name__ == '__main__':
    unittest.main()
