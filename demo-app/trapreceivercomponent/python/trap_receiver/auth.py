"""
Trap Receiver - API Key Authentication Middleware
==================================================
Two keys loaded from environment (injected from Kubernetes Secret):

  TRAP_API_KEY       - Required for POST /api/v1/trap  (write/Zabbix key)
  DASHBOARD_API_KEY  - Required for GET /dashboard and GET /api/v1/events*

Rules:
  - GET /api/v1/health  → ALWAYS allowed (k8s liveness/readiness probe)
  - POST /api/v1/trap   → requires X-API-Key: <TRAP_API_KEY>
  - GET /dashboard      → requires X-API-Key: <DASHBOARD_API_KEY>
  - GET /api/v1/events* → requires X-API-Key: <DASHBOARD_API_KEY>
  - Anything else       → 404 (Flask handles normally)

Header: X-API-Key: <key>
"""

import os
import hmac
import logging
from flask import request, jsonify

logger = logging.getLogger("TrapReceiver.Auth")

# Loaded once at import time from env vars (set by k8s Secret envFrom)
_TRAP_KEY      = os.environ.get("TRAP_API_KEY", "").strip()
_DASHBOARD_KEY = os.environ.get("DASHBOARD_API_KEY", "").strip()

# Routes that need the TRAP key (write operations)
_TRAP_PATHS      = {"/api/v1/trap", "/api/v1/trap/alarm", "/api/v1/trap/clear"}

# Routes that need the DASHBOARD key (read operations)
_DASHBOARD_PATHS = {"/dashboard", "/api/v1/events", "/api/v1/events/summary"}

# Routes that are always open (k8s probes — called from inside cluster only)
_OPEN_PATHS      = {"/api/v1/health"}


def _safe_compare(provided: str, expected: str) -> bool:
    """Constant-time comparison to prevent timing attacks."""
    if not expected:
        logger.error("API key not configured in Secret — rejecting all requests!")
        return False
    return hmac.compare_digest(
        provided.encode("utf-8"),
        expected.encode("utf-8"),
    )


def check_auth():
    """
    Flask before_request hook.
    Returns a JSON error Response if auth fails, else None (allow).
    """
    path = request.path

    # ── Always open: k8s health probes ────────────────────────────────────────
    if path in _OPEN_PATHS:
        return None

    # ── Trap / write routes ───────────────────────────────────────────────────
    if path in _TRAP_PATHS:
        key = request.headers.get("X-API-Key", "").strip()
        if not key:
            logger.warning(f"[AUTH] Missing X-API-Key on {path} from {_client_ip()}")
            return _deny("missing_api_key", "X-API-Key header is required")
        if not _safe_compare(key, _TRAP_KEY):
            logger.warning(f"[AUTH] Invalid TRAP key on {path} from {_client_ip()}")
            return _deny("invalid_api_key", "Invalid API key")
        return None  # allow

    # ── Dashboard / read routes ───────────────────────────────────────────────
    # /api/v1/events and /api/v1/events/summary both start with /api/v1/events
    if path == "/dashboard" or path.startswith("/api/v1/events"):
        key = request.headers.get("X-API-Key", "").strip()
        if not key:
            logger.warning(f"[AUTH] Missing X-API-Key on {path} from {_client_ip()}")
            return _deny("missing_api_key", "X-API-Key header is required")
        if not _safe_compare(key, _DASHBOARD_KEY):
            logger.warning(f"[AUTH] Invalid DASHBOARD key on {path} from {_client_ip()}")
            return _deny("invalid_api_key", "Invalid API key")
        return None  # allow

    # All other paths — let Flask route normally (will 404 if not registered)
    return None


def _deny(code: str, message: str):
    return jsonify({"error": code, "message": message}), 401


def _client_ip() -> str:
    return (
        request.headers.get("X-Forwarded-For", "").split(",")[0].strip()
        or request.remote_addr
        or "unknown"
    )
