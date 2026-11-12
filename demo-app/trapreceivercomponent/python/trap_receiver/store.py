"""
Trap Receiver - In-Memory Event Store
Thread-safe in-memory store. No external deps.
Zero impact: app.py calls record() only after successful response.
"""
import threading
from datetime import datetime, timezone
from collections import deque

MAX_EVENTS = 500
_lock     = threading.Lock()
_events   = deque(maxlen=MAX_EVENTS)
_counters = {}


def record(action, correlation_id, cloud_event):
    entry = {
        "received_at":       datetime.now(timezone.utc).isoformat(),
        "action":            action,
        "correlation_id":    correlation_id,
        "event_id":          cloud_event.get("id", ""),
        "source":            cloud_event.get("source", ""),
        "type":              cloud_event.get("type", ""),
        "time":              cloud_event.get("time", ""),
        "host":              _dig(cloud_event, "alarmedObject", "id"),
        "severity":          _dig(cloud_event, "perceivedSeverity"),
        "problem":           _dig(cloud_event, "specificProblem"),
        "state":             _dig(cloud_event, "state"),
        "recovery_event_id": cloud_event.get("recoveryeventid", ""),
        "user_id":           cloud_event.get("user_id", ""),
        "conversation_id":   cloud_event.get("conversation_id", ""),
        "conversation_name": cloud_event.get("conversation_name", ""),
    }
    with _lock:
        _events.appendleft(entry)
        _counters[action] = _counters.get(action, 0) + 1


def get_events(action=None, limit=100):
    with _lock:
        events = list(_events)
    if action:
        events = [e for e in events if e["action"] == action]
    return events[:limit]


def get_summary():
    with _lock:
        counters = dict(_counters)
        last     = _events[0]["received_at"] if _events else None
        total    = len(_events)
    return {"total": total, "counters": counters, "last_event": last}


def _dig(cloud_event, *keys):
    try:
        msgs      = cloud_event.get("data", {}).get("components", {}).get("messages", {})
        first_msg = list(msgs.values())[0]
        val       = first_msg.get("payload", {}).get("const", {})
        for k in keys:
            val = val.get(k, {})
        return val if isinstance(val, str) else ""
    except Exception:
        return ""
