import logging
from enum import Enum
from typing import Optional, Dict, Any

logger = logging.getLogger("notifications")

class Severity(Enum):
    INFO = "INFO"
    WARNING = "WARNING"
    ACTION_REQUIRED = "ACTION_REQUIRED"
    CRITICAL = "CRITICAL"

def notify(event: Dict[str, Any]):
    """
    Abstract notification transport.
    Event expected schema:
    {
        "severity": Severity,
        "run_id": str,
        "market_date": str,
        "status": str,
        "reason": str,
        "message": str
    }
    """
    try:
        # Currently no external transport is configured.
        # This abstraction guarantees safe failure and structured event handling.
        severity = event.get("severity", Severity.INFO)
        msg = f"NOTIFICATION [{severity.name}]: {event.get('message')} | Status: {event.get('status')} | Reason: {event.get('reason')}"
        
        if severity in [Severity.CRITICAL, Severity.ACTION_REQUIRED]:
            logger.error(msg)
        elif severity == Severity.WARNING:
            logger.warning(msg)
        else:
            logger.info(msg)
            
        # Example webhook implementation could go here:
        # requests.post(WEBHOOK_URL, json=event, timeout=5)
        
    except Exception as e:
        # A notification failure must NEVER corrupt the pipeline execution
        logger.error(f"Failed to dispatch notification: {e}")
