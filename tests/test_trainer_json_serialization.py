import json

import numpy as np

from src.ml.trainer import _to_json_safe


def test_to_json_safe_converts_numpy_values():
    payload = {
        "row_hash": np.uint64(42),
        "best_params": {"max_depth": np.int64(5)},
        "flag": np.bool_(True),
        "values": [np.int32(1), np.float64(0.5)],
    }

    safe_payload = _to_json_safe(payload)

    assert safe_payload["row_hash"] == 42
    assert safe_payload["best_params"]["max_depth"] == 5
    assert safe_payload["flag"] is True
    assert safe_payload["values"] == [1, 0.5]
    json.dumps(safe_payload)
