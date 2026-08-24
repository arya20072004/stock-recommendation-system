import json
import os
import pytest
import tempfile
import src.ml.history as history_module
from unittest.mock import patch
from src.ml.history import load_active_bundle

# The old pipeline hash from August 11-12 training run (55/57 feature era)
OLD_PIPELINE_HASH = "685cb3dbe63d7923126e44c597914c93a7bcebc83c6f6e42017dd1101f7d2c68"


def test_old_pipeline_rejection():
    """
    Regression test: a manifest carrying the OLD pipeline hash must be rejected
    by load_active_bundle with a ValueError (fail-closed).

    The rejection proves:
      1. load_active_bundle reads the actual v1 pipeline hash (current = f4891c1b...)
      2. It compares against the OLD hash stored in the manifest (685cb3db...)
      3. The mismatch raises ValueError with "Failing closed."
      4. The rejection is due to pipeline identity incompatibility, NOT a missing file.
    """
    manifest = {
        "model_version": "9396a75b6365",
        "model_hash": "aabbccddeeff",
        "feature_hash": "a" * 64,
        "feature_pipeline_version": "v1",
        "feature_pipeline_hash": OLD_PIPELINE_HASH,   # <-- intentionally stale
        "dataset_hash": "dummy_dataset"
    }

    with tempfile.TemporaryDirectory() as tmpdir:
        # Write a real manifest file so builtins.open is NOT globally patched
        manifest_path = os.path.join(tmpdir, "RELIANCE.NS_active.json")
        with open(manifest_path, "w", encoding="utf-8") as fh:
            json.dump(manifest, fh)

        # Redirect history.py's MODELS_DIR to the temp dir
        with patch.object(history_module, "MODELS_DIR", tmpdir):
            with pytest.raises(ValueError, match="Feature pipeline hash mismatch.*Failing closed\\."):
                load_active_bundle("RELIANCE.NS")
