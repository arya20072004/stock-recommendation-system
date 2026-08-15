"""
Frozen PCR Gate - Pre-registered for Temporal Validation.
Version: 1.0 (Frozen after T1-T3 development)
"""

def should_enable_pcr(
    ticker: str,
    cutoff_date: str,
    historical_information: dict,
    current_cv_information: dict
) -> tuple[bool, str, str]:
    """
    Evaluates the frozen pre-registered gate logic.
    
    Args:
        ticker: The stock ticker string.
        cutoff_date: The cutoff date string.
        historical_information: Dictionary containing 'obs_count', 'mean_delta_f1', 'collapse_rate'.
        current_cv_information: Dictionary containing 'delta_cv_score'.
        
    Returns:
        (gate_enabled, gate_reason, gate_version)
    """
    gate_version = "1.0_FROZEN"
    
    obs_count = historical_information.get("obs_count", 0)
    
    if obs_count == 0:
        # COLD START (T1)
        delta_cv = current_cv_information.get("delta_cv_score", 0.0)
        gate_enabled = delta_cv > 0.005
        gate_reason = f"COLD: cv_delta={delta_cv:.6f} > 0.005"
    else:
        # WARM START (T2+)
        mean_delta = historical_information.get("mean_delta_f1", 0.0)
        collapse_rate = historical_information.get("collapse_rate", 0.0)
        
        gate_enabled = (mean_delta > 0) and (collapse_rate == 0)
        gate_reason = f"WARM: mean_delta={mean_delta:.6f} > 0 AND collapse={collapse_rate:.2f} == 0"
        
    return gate_enabled, gate_reason, gate_version
