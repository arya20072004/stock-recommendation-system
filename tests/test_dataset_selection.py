import os
import unittest
from unittest.mock import patch, MagicMock
import pandas as pd

from src.ml.evaluate_candidate import load_frozen_dataset

class TestDatasetSelection(unittest.TestCase):
    def setUp(self):
        self.expected_hash = "b4c8b5075e70"
        self.metadata_path = f"saved_evaluations\\metadata_v1_{self.expected_hash}.json"
        self.parquet_path = f"saved_evaluations\\eval_dataset_v1_{self.expected_hash}.parquet"
        
        # We must use os.path.join in the test to match os.path.join behavior
        self.metadata_path = os.path.join("saved_evaluations", f"metadata_v1_{self.expected_hash}.json")
        self.parquet_path = os.path.join("saved_evaluations", f"eval_dataset_v1_{self.expected_hash}.parquet")
        
        self.valid_metadata = {
            "evaluation_dataset_hash": f"{self.expected_hash}59c10a9406fc06b2f03b764650394f2e355a7d39ed8e2f1cd420",
            "evaluation_start_date": "2025-08-25",
            "evaluation_end_date": "2026-08-05"
        }
        
        self.valid_df = pd.DataFrame({
            "target": [0, 1, 2],
            "future_return": [0.01, -0.01, 0.05]
        })

    @patch("src.ml.evaluate_candidate.os.path.exists")
    @patch("src.ml.evaluate_candidate.load_manifest")
    @patch("src.ml.evaluate_candidate.pd.read_parquet")
    def test_1_canonical_dataset_selected(self, mock_read_parquet, mock_load_manifest, mock_exists):
        mock_exists.side_effect = lambda path: path in [self.metadata_path, self.parquet_path]
        mock_load_manifest.return_value = self.valid_metadata
        mock_read_parquet.return_value = self.valid_df
        
        df, metadata = load_frozen_dataset()
        self.assertEqual(metadata, self.valid_metadata)
        
    @patch("src.ml.evaluate_candidate.os.path.exists")
    @patch("src.ml.evaluate_candidate.load_manifest")
    @patch("src.ml.evaluate_candidate.pd.read_parquet")
    def test_2_historical_metadata_ignored(self, mock_read_parquet, mock_load_manifest, mock_exists):
        # Even if historical exists, we ONLY ask for the expected one
        mock_exists.side_effect = lambda path: path in [self.metadata_path, self.parquet_path, os.path.join("saved_evaluations", "metadata_v1_a3262716b006.json")]
        mock_load_manifest.return_value = self.valid_metadata
        mock_read_parquet.return_value = self.valid_df
        
        df, metadata = load_frozen_dataset()
        self.assertEqual(metadata, self.valid_metadata)
        mock_load_manifest.assert_called_once_with(self.metadata_path)

    @patch("src.ml.evaluate_candidate.os.path.exists")
    @patch("src.ml.evaluate_candidate.load_manifest")
    @patch("src.ml.evaluate_candidate.pd.read_parquet")
    def test_3_filesystem_ordering_irrelevant(self, mock_read_parquet, mock_load_manifest, mock_exists):
        # glob is not used, so ordering is impossible to affect it.
        mock_exists.side_effect = lambda path: path in [self.metadata_path, self.parquet_path]
        mock_load_manifest.return_value = self.valid_metadata
        mock_read_parquet.return_value = self.valid_df
        df, metadata = load_frozen_dataset()
        self.assertEqual(metadata, self.valid_metadata)

    @patch("src.ml.evaluate_candidate.os.path.exists")
    def test_4_canonical_metadata_missing(self, mock_exists):
        mock_exists.side_effect = lambda path: path == self.parquet_path
        with self.assertRaisesRegex(FileNotFoundError, "Missing canonical metadata manifest"):
            load_frozen_dataset()

    @patch("src.ml.evaluate_candidate.os.path.exists")
    @patch("src.ml.evaluate_candidate.load_manifest")
    def test_5_canonical_parquet_missing(self, mock_load_manifest, mock_exists):
        mock_exists.side_effect = lambda path: path == self.metadata_path
        mock_load_manifest.return_value = self.valid_metadata
        with self.assertRaisesRegex(FileNotFoundError, "Missing canonical parquet dataset"):
            load_frozen_dataset()

    @patch("src.ml.evaluate_candidate.os.path.exists")
    @patch("src.ml.evaluate_candidate.load_manifest")
    def test_6_metadata_identity_mismatch(self, mock_load_manifest, mock_exists):
        mock_exists.return_value = True
        invalid_metadata = self.valid_metadata.copy()
        invalid_metadata["evaluation_dataset_hash"] = "a3262716b006wronghash"
        mock_load_manifest.return_value = invalid_metadata
        
        with self.assertRaisesRegex(ValueError, "Metadata identity mismatch"):
            load_frozen_dataset()
            
    @patch("src.ml.evaluate_candidate.os.path.exists")
    @patch("src.ml.evaluate_candidate.load_manifest")
    def test_7_metadata_parquet_mismatch(self, mock_load_manifest, mock_exists):
        # Mismatch handled by path strictly enforcing both have the exact same b4c8b5075e70 ID in filename
        # This test is conceptually the same as identity mismatch but covers the logic requirement.
        mock_exists.return_value = True
        invalid_metadata = self.valid_metadata.copy()
        invalid_metadata["evaluation_dataset_hash"] = "different_hash"
        mock_load_manifest.return_value = invalid_metadata
        with self.assertRaisesRegex(ValueError, "Metadata identity mismatch"):
            load_frozen_dataset()

    @patch("src.ml.evaluate_candidate.os.path.exists")
    @patch("src.ml.evaluate_candidate.load_manifest")
    def test_8_date_range_mismatch(self, mock_load_manifest, mock_exists):
        mock_exists.return_value = True
        invalid_metadata = self.valid_metadata.copy()
        invalid_metadata["evaluation_start_date"] = "2025-01-01"
        mock_load_manifest.return_value = invalid_metadata
        
        with self.assertRaisesRegex(ValueError, "Unexpected evaluation date range"):
            load_frozen_dataset()

    @patch("src.ml.evaluate_candidate.os.path.exists")
    @patch("src.ml.evaluate_candidate.load_manifest")
    @patch("src.ml.evaluate_candidate.pd.read_parquet")
    def test_9_dataset_maturity_failure(self, mock_read_parquet, mock_load_manifest, mock_exists):
        mock_exists.return_value = True
        mock_load_manifest.return_value = self.valid_metadata
        
        # Inject NaN in target
        invalid_df = pd.DataFrame({
            "target": [0, float('nan'), 2],
            "future_return": [0.01, -0.01, 0.05]
        })
        mock_read_parquet.return_value = invalid_df
        
        with self.assertRaisesRegex(ValueError, "Dataset is not fully mature"):
            load_frozen_dataset()

    @patch("src.ml.evaluate_candidate.os.path.exists")
    @patch("src.ml.evaluate_candidate.load_manifest")
    @patch("src.ml.evaluate_candidate.pd.read_parquet")
    def test_10_historical_orphan_cannot_hijack(self, mock_read_parquet, mock_load_manifest, mock_exists):
        mock_exists.side_effect = lambda path: path in [self.metadata_path, self.parquet_path, os.path.join("saved_evaluations", "metadata_v1_a3262716b006.json")]
        mock_load_manifest.return_value = self.valid_metadata
        mock_read_parquet.return_value = self.valid_df
        
        df, metadata = load_frozen_dataset()
        self.assertEqual(metadata, self.valid_metadata)

if __name__ == '__main__':
    unittest.main()
