"""
Trap Receiver - API Key Authentication Middleware
==================================================
Two keys loaded from environment (injected from Kubernetes Secret):

  TRAP_API_KEY       - Required for POST /api/v1/trap  (write/Zabbix key)
  DASHBOARD_API_KEY  - Required for GET /api/v1/events*  (read/JSON key)

Route sets (which paths need which key) come from config.yaml's `auth`
section (auth.trap_paths / auth.dashboard_paths / auth.open_paths) -- not
hardcoded here, so adding a route doesn't require a code change.

Rules:
  - auth.open_paths      → ALWAYS allowed (health probe, the dashboard HTML
                            shell itself -- its own JS supplies the key when
                            it calls the JSON endpoints below)
  - auth.trap_paths      → requires X-API-Key: <TRAP_API_KEY>
  - auth.dashboard_paths  → requires X-API-Key: <DASHBOARD_API_KEY>
  - Anything else        → 404 (Flask handles normally)

Header: X-API-Key: <key>
"""

import hmac
import logging
import os

from flask import current_app, request, jsonify

logger = logging.getLogger("TrapReceiver.Auth")

# Loaded once at import time from env vars (set by k8s Secret envFrom)
_TRAP_KEY      = os.environ.get("TRAP_API_KEY", "").strip()
_DASHBOARD_KEY = os.environ.get("DASHBOARD_API_KEY", "").strip()


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
    path     = request.path
    auth_cfg = current_app.config["TRAP_CONFIG"]["auth"]

    if path in auth_cfg["open_paths"]:
        return None

    if path in auth_cfg["trap_paths"]:
        return _require_key(path, _TRAP_KEY)

    if path in auth_cfg["dashboard_paths"]:
        return _require_key(path, _DASHBOARD_KEY)

    # All other paths — let Flask route normally (will 404 if not registered)
    return None


def _require_key(path: str, expected_key: str):
    key = request.headers.get("X-API-Key", "").strip()
    if not key:
        logger.warning("[AUTH] Missing X-API-Key on %s from %s", path, _client_ip())
        return _deny("missing_api_key", "X-API-Key header is required")
    if not _safe_compare(key, expected_key):
        logger.warning("[AUTH] Invalid API key on %s from %s", path, _client_ip())
        return _deny("invalid_api_key", "Invalid API key")
    return None  # allow


def _deny(code: str, message: str):
    return jsonify({"error": code, "message": message}), 401


def _client_ip() -> str:
    return (
        request.headers.get("X-Forwarded-For", "").split(",")[0].strip()
        or request.remote_addr
        or "unknown"
    )
