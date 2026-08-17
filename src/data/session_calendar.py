"""
Canonical NSE Session Authority
This module is the single repository-owned boundary for NSE trading-session identity.
It encapsulates pandas_market_calendars and provides explicit overrides for special
sessions (e.g. Muhurat trading) that the base mathematical calendar misses.
"""
import datetime
import pandas as pd

class CalendarResolutionError(Exception):
    """Raised for any systemic calendar resolution failure (e.g. missing dependency, out of bounds)."""
    pass

try:
    import pandas_market_calendars as mcal
    _mcal_nse = mcal.get_calendar("NSE")
except Exception as e:
    raise CalendarResolutionError(f"Failed to initialize NSE calendar from pandas_market_calendars: {e}")

# Version-controlled, explicit overrides for known NSE special sessions/closures.
_SPECIAL_SESSION_OVERRIDES = {
    datetime.date(2022, 10, 24): {
        "state": "SESSION",
        "reason": "Muhurat Trading",
        "source": "NSE Circular Ref. No: 104/2022"
    },
    datetime.date(2023, 11, 12): {
        "state": "SESSION",
        "reason": "Muhurat Trading",
        "source": "NSE Circular Ref. No: 130/2023"
    },
    datetime.date(2024, 3, 2): {
        "state": "SESSION",
        "reason": "Special Saturday Disaster Recovery Session",
        "source": "NSE Circular Ref. No: 42/2024"
    },
    datetime.date(2024, 11, 1): {
        "state": "SESSION",
        "reason": "Muhurat Trading",
        "source": "NSE Circular Ref. No: 121/2024"
    }
}

# Defensive boundary: fail fast if the override table is malformed.
for _date_key, _data in _SPECIAL_SESSION_OVERRIDES.items():
    if not isinstance(_date_key, datetime.date):
        raise CalendarResolutionError(f"Invalid override key type: {_date_key}")
    if _data.get("state") not in ("SESSION", "CLOSED"):
        raise CalendarResolutionError(f"Invalid override state for {_date_key}: {_data.get('state')}")
    if not _data.get("reason") or not _data.get("source"):
        raise CalendarResolutionError(f"Missing provenance for override: {_date_key}")


def _normalize_date(dt: datetime.date | datetime.datetime | pd.Timestamp) -> datetime.date:
    """Safely extracts a naive datetime.date from various timestamp types."""
    try:
        if isinstance(dt, pd.Timestamp):
            return dt.date()
        if isinstance(dt, datetime.datetime):
            return dt.date()
        if isinstance(dt, datetime.date):
            return dt
        raise ValueError("Unsupported date type")
    except Exception as e:
        raise CalendarResolutionError(f"Failed to normalize date {dt}: {e}")


def is_session(session_date: datetime.date | datetime.datetime | pd.Timestamp) -> bool:
    """
    Returns True if the given date is an NSE trading session.
    Evaluates explicit repository overrides first, then falls back to the base calendar.
    """
    normalized_date = _normalize_date(session_date)

    # 1. Check explicit repository overrides
    override = _SPECIAL_SESSION_OVERRIDES.get(normalized_date)
    if override is not None:
        return override["state"] == "SESSION"

    # 2. Check canonical base calendar
    try:
        # mcal valid_days is inclusive of both bounds
        days = _mcal_nse.valid_days(normalized_date, normalized_date)
        return len(days) > 0
    except Exception as e:
        raise CalendarResolutionError(f"Calendar query failed for {normalized_date}: {e}")


def next_session(session_date: datetime.date | datetime.datetime | pd.Timestamp) -> datetime.date:
    """
    Returns the first valid NSE session strictly AFTER the supplied date.
    """
    normalized_date = _normalize_date(session_date)
    
    # We iterate forward defensively. 
    # An upper bound of 30 days is chosen to prevent infinite loops 
    # while comfortably spanning any known exchange holiday cluster.
    MAX_FORWARD_DAYS = 30
    
    candidate = normalized_date + datetime.timedelta(days=1)
    for _ in range(MAX_FORWARD_DAYS):
        if is_session(candidate):
            return candidate
        candidate += datetime.timedelta(days=1)
        
    raise CalendarResolutionError(f"Could not resolve next session within {MAX_FORWARD_DAYS} days of {normalized_date}")


def previous_session(session_date: datetime.date | datetime.datetime | pd.Timestamp) -> datetime.date:
    """
    Returns the first valid NSE session strictly BEFORE the supplied date.
    """
    normalized_date = _normalize_date(session_date)
    
    MAX_BACKWARD_DAYS = 30
    
    candidate = normalized_date - datetime.timedelta(days=1)
    for _ in range(MAX_BACKWARD_DAYS):
        if is_session(candidate):
            return candidate
        candidate -= datetime.timedelta(days=1)
        
    raise CalendarResolutionError(f"Could not resolve previous session within {MAX_BACKWARD_DAYS} days of {normalized_date}")
