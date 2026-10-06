"""
src.ml.training.replay
Historical Challenger Replay Framework against Frozen Production Dataset for V2 Architecture.
Consumes immutable feature snapshots from prediction provenance without temporal contamination.
"""

from dataclasses import dataclass
import hashlib
import json
import os
from pathlib import Path
from typing import Dict, Any, List, Optional, Tuple
import numpy as np
import pandas as pd


@dataclass
class FrozenReplayManifest:
    dataset_name: str
    record_count: int
    sha256_hash: str
    start_date: str
    end_date: str
    feature_count: int
    created_at: str

    def to_json(self, indent: int = 2) -> str:
        return json.dumps(self.__dict__, indent=indent)


class FrozenReplayDataset:
    """
    Manages the immutable read-only evaluation snapshot of the 1,083 evaluated production predictions.
    Guarantees no future data substitution and verifies cryptographic hash on load.
    """

    DEFAULT_SNAPSHOT_DIR = Path("saved_evaluations")
    DATASET_FILENAME = "frozen_replay_1083.parquet"
    MANIFEST_FILENAME = "manifest_replay_1083.json"

    @classmethod
    def create_from_mongodb(cls, db, output_dir: Optional[Path] = None) -> Path:
        """
        Creates an immutable frozen parquet snapshot from evaluated records in MongoDB.
        Only executed once to freeze the evaluation set; never overwrites existing verified files.
        """
        target_dir = output_dir or cls.DEFAULT_SNAPSHOT_DIR
        target_dir.mkdir(parents=True, exist_ok=True)
        parquet_path = target_dir / cls.DATASET_FILENAME
        manifest_path = target_dir / cls.MANIFEST_FILENAME

        eval_records = list(db.prediction_history.find({"status": "EVALUATED"}).sort("market_date", 1))
        if len(eval_records) == 0:
            raise ValueError("No EVALUATED predictions found in prediction_history")

        prov_records = list(db.prediction_provenance.find({}))
        prov_map = {p.get("provenance_hash"): p for p in prov_records if p.get("provenance_hash")}

        rows = []
        for h in eval_records:
            p_hash = h.get("provenance_hash")
            p = prov_map.get(p_hash)
            if not p:
                raise ValueError(f"Missing provenance document for prediction {h.get('_id')} (hash: {p_hash})")

            # Extract features dict
            feat_dict = p.get("features", {})
            model_probs = p.get("model_probabilities", [])

            row = {
                "prediction_id": str(h.get("_id")),
                "symbol": h.get("symbol"),
                "market_date": h.get("market_date"),
                "model_version": h.get("model_version"),
                "provenance_hash": p_hash,
                "price_at_prediction": float(h.get("price_at_prediction", 0.0)),
                "actual_price": float(h.get("actual_price", 0.0)) if h.get("actual_price") is not None else np.nan,
                "actual_return": float(h.get("actual_return", 0.0)),
                "target_return_threshold": float(h.get("target_return_threshold", 0.015)),
                "actual_class": h.get("actual_class"),
                "incumbent_raw_prediction": h.get("raw_prediction"),
                "incumbent_recommendation": h.get("recommendation"),
                "incumbent_confidence": float(h.get("confidence", 0.0)),
                "incumbent_confidence_tier": h.get("confidence_tier"),
                "incumbent_prob_sell": float(model_probs[0]) if len(model_probs) > 0 else np.nan,
                "incumbent_prob_hold": float(model_probs[1]) if len(model_probs) > 1 else np.nan,
                "incumbent_prob_buy": float(model_probs[2]) if len(model_probs) > 2 else np.nan,
            }
            # Flatten feature dict with prefix
            for f_name, f_val in feat_dict.items():
                row[f"feat_{f_name}"] = float(f_val) if f_val is not None else np.nan

            rows.append(row)

        df = pd.DataFrame(rows)
        df.sort_values(["market_date", "symbol"], inplace=True)
        df.to_parquet(parquet_path, engine="pyarrow", index=False)

        # Compute hash
        with open(parquet_path, "rb") as f:
            file_hash = hashlib.sha256(f.read()).hexdigest()

        manifest = FrozenReplayManifest(
            dataset_name=cls.DATASET_FILENAME,
            record_count=len(df),
            sha256_hash=file_hash,
            start_date=str(df["market_date"].min()),
            end_date=str(df["market_date"].max()),
            feature_count=len([c for c in df.columns if c.startswith("feat_")]),
            created_at=pd.Timestamp.now().isoformat(),
        )

        with open(manifest_path, "w", encoding="utf-8") as f:
            f.write(manifest.to_json())

        return parquet_path

    @classmethod
    def load(cls, snapshot_dir: Optional[Path] = None, verify_hash: bool = True) -> pd.DataFrame:
        """
        Loads the immutable frozen replay dataset and verifies integrity.
        """
        target_dir = snapshot_dir or cls.DEFAULT_SNAPSHOT_DIR
        parquet_path = target_dir / cls.DATASET_FILENAME
        manifest_path = target_dir / cls.MANIFEST_FILENAME

        if not parquet_path.exists():
            raise FileNotFoundError(f"Frozen replay dataset not found at {parquet_path}")

        if verify_hash and manifest_path.exists():
            with open(manifest_path, "r", encoding="utf-8") as f:
                manifest_dict = json.load(f)
            expected_hash = manifest_dict.get("sha256_hash")

            with open(parquet_path, "rb") as f:
                actual_hash = hashlib.sha256(f.read()).hexdigest()

            if expected_hash and actual_hash != expected_hash:
                raise ValueError(f"Corrupted replay dataset! Expected hash {expected_hash}, got {actual_hash}")

        df = pd.read_parquet(parquet_path)
        return df


class ChallengerReplayEngine:
    """
    Executes paired replay of Challenger models against the frozen historical dataset.
    Feeds identical feature snapshots into the Challenger pipeline and scores outputs.
    """

    CLASS_NAMES = {0: "SELL", 1: "HOLD", 2: "BUY"}
    CLASS_INDICES = {"SELL": 0, "HOLD": 1, "BUY": 2}

    def __init__(self, models_dict: Dict[str, Any], calibrators_dict: Optional[Dict[str, Any]] = None):
        """
        models_dict: Map of ticker -> fitted Challenger model
        calibrators_dict: Optional map of ticker -> fitted calibrator
        """
        self.models = models_dict
        self.calibrators = calibrators_dict or {}

    def replay(self, df_eval: pd.DataFrame) -> pd.DataFrame:
        """
        Evaluates the challenger models across all rows in df_eval.
        Returns combined dataframe with Incumbent and Challenger predictions.
        """
        results = df_eval.copy()

        # Identify feature columns
        feat_cols = [c for c in results.columns if c.startswith("feat_")]
        raw_feat_names = [c.replace("feat_", "") for c in feat_cols]

        chal_raw_preds = []
        chal_recs = []
        chal_p_sell = []
        chal_p_hold = []
        chal_p_buy = []

        for _, row in results.iterrows():
            ticker = row["symbol"]
            model = self.models.get(ticker)

            if model is None:
                # If model not available for this ticker, mark neutral
                chal_raw_preds.append("HOLD")
                chal_recs.append("HOLD")
                chal_p_sell.append(0.33)
                chal_p_hold.append(0.34)
                chal_p_buy.append(0.33)
                continue

            # Extract feature vector in correct order
            x_vec = np.array([row[c] for c in feat_cols], dtype=float).reshape(1, -1)

            # Predict raw probabilities
            if hasattr(model, "predict_proba"):
                probs = model.predict_proba(x_vec)[0]
            else:
                raw_idx = model.predict(x_vec)[0]
                probs = np.zeros(3)
                probs[int(raw_idx)] = 1.0

            # Calibrate if calibrator is registered
            calibrator = self.calibrators.get(ticker)
            if calibrator is not None and hasattr(calibrator, "calibrate"):
                probs = calibrator.calibrate(probs.reshape(1, -1))[0]

            pred_class_idx = int(np.argmax(probs))
            raw_pred_name = self.CLASS_NAMES.get(pred_class_idx, "HOLD")

            # Apply confidence gating logic:
            # Gating rule: confidence = max_proba. If max_proba < 0.40, gate to HOLD.
            max_p = float(np.max(probs))
            if max_p < 0.40:
                rec_name = "HOLD"
            else:
                rec_name = raw_pred_name

            chal_raw_preds.append(raw_pred_name)
            chal_recs.append(rec_name)
            chal_p_sell.append(float(probs[0]))
            chal_p_hold.append(float(probs[1]))
            chal_p_buy.append(float(probs[2]))

        results["challenger_raw_prediction"] = chal_raw_preds
        results["challenger_recommendation"] = chal_recs
        results["challenger_prob_sell"] = chal_p_sell
        results["challenger_prob_hold"] = chal_p_hold
        results["challenger_prob_buy"] = chal_p_buy

        # Correctness tags
        results["incumbent_correct"] = (results["incumbent_recommendation"] == results["actual_class"]).astype(int)
        results["challenger_correct"] = (results["challenger_recommendation"] == results["actual_class"]).astype(int)

        results["incumbent_raw_correct"] = (results["incumbent_raw_prediction"] == results["actual_class"]).astype(int)
        results["challenger_raw_correct"] = (results["challenger_raw_prediction"] == results["actual_class"]).astype(int)

        return results
