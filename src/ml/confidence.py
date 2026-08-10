# confidence.py
"""
Confidence tier assignment for Nifty 50 ML predictions.

Tier logic uses three orthogonal signals:
  1. f1_macro    — historical model quality on test set
  2. max_proba   — model certainty on this specific prediction
  3. top2_margin — separation between top-2 class probabilities

Tiers: VERY_LOW → LOW → MEDIUM → HIGH → VERY_HIGH
Only MEDIUM and above are shown as actionable BUY/SELL in the UI.
VERY_LOW and LOW are shown as HOLD regardless of prediction.
"""

from __future__ import annotations
import json
import os

MODELS_DIR = "saved_models"

# F1-based stock quality thresholds (gate 1 — historical model quality)
F1_TIERS = [
    ("VERY_HIGH", 0.42),
    ("HIGH",      0.36),
    ("MEDIUM",    0.30),
    ("LOW",       0.25),
    ("VERY_LOW",  0.00),
]

# max_proba thresholds (gate 2 — prediction certainty)
PROBA_TIERS = [
    ("VERY_HIGH", 0.65),
    ("HIGH",      0.55),
    ("MEDIUM",    0.45),
    ("LOW",       0.38),
    ("VERY_LOW",  0.00),
]

# top-2 margin thresholds (gate 3 — class separation)
MARGIN_TIERS = [
    ("VERY_HIGH", 0.30),
    ("HIGH",      0.20),
    ("MEDIUM",    0.12),
    ("LOW",       0.06),
    ("VERY_LOW",  0.00),
]

TIER_RANK = {
    "VERY_LOW": 0,
    "LOW":      1,
    "MEDIUM":   2,
    "HIGH":     3,
    "VERY_HIGH":4,
}

TIER_LABELS = {v: k for k, v in TIER_RANK.items()}


def _classify(value: float, thresholds: list[tuple[str, float]]) -> str:
    for tier, cutoff in thresholds:
        if value >= cutoff:
            return tier
    return "VERY_LOW"


def compute_confidence_tier(
    ticker: str,
    max_proba: float,
    top2_margin: float,
    f1_macro: float = 0.0,
) -> dict:
    """
    Returns confidence tier and metadata for a single prediction.

    Parameters
    ----------
    ticker      : e.g. "AXISBANK.NS"
    max_proba   : highest class probability from model.predict_proba()
    top2_margin : difference between top-2 class probabilities
    f1_macro    : F1 macro score of the active model

    Returns
    -------
    dict with keys: tier, tier_rank, f1_macro, max_proba,
                    top2_margin, actionable
    """
    # Each signal independently assigns a tier rank
    f1_rank     = TIER_RANK[_classify(f1_macro,    F1_TIERS)]
    proba_rank  = TIER_RANK[_classify(max_proba,   PROBA_TIERS)]
    margin_rank = TIER_RANK[_classify(top2_margin, MARGIN_TIERS)]

    # Final tier = minimum of all three gates (weakest link)
    final_rank = min(f1_rank, proba_rank, margin_rank)
    final_tier = TIER_LABELS[final_rank]

    # Actionable = MEDIUM or above; below that force-display as HOLD
    actionable = final_rank >= TIER_RANK["MEDIUM"]

    return {
        "tier":        final_tier,
        "tier_rank":   final_rank,
        "f1_macro":    round(f1_macro, 4),
        "max_proba":   round(max_proba, 4),
        "top2_margin": round(top2_margin, 4),
        "actionable":  actionable,
    }

def get_display_signal(prediction: str, confidence: dict) -> str:
    """
    Returns the signal to display in the UI.
    Forces HOLD when confidence is not actionable.
    Preserves HOLD predictions regardless of tier.
    """
    if not confidence["actionable"]:
        return "HOLD"
    return prediction