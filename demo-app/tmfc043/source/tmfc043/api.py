"""TMF642 and TMF656 REST endpoints of TMFC043.

TMFC043 is a proxy for these two APIs: the alarm management agent calls them here and the
request is forwarded unchanged to the NetoAI digital twin (``TWIN_BASE_URL``), which owns
the alarm and service problem logic. The response comes back to the caller as the twin sent it.

When ``TWIN_BASE_URL`` is not set the endpoints answer 503, so a missing setting is visible
rather than silently swallowed.
"""

from __future__ import annotations

import logging
import os

import requests
from flask import Blueprint, Response, request

log = logging.getLogger("tmfc043.api")
bp = Blueprint("tmf_api", __name__)

# Request headers worth passing on. Hop-by-hop headers and Host are left to the HTTP library.
_FORWARD_HEADERS = ("Content-Type", "Accept", "Authorization", "X-Correlation-Id")


def _twin_base() -> str:
    return os.environ.get("TWIN_BASE_URL", "").rstrip("/")


def forward(path: str) -> Response:
    base = _twin_base()
    if not base:
        return Response('{"code":"503","reason":"TWIN_BASE_URL is not configured"}', 503, mimetype="application/json")
    try:
        upstream = requests.request(
            request.method,
            f"{base}/{path}",
            params=request.args,
            data=request.get_data(),
            headers={h: request.headers[h] for h in _FORWARD_HEADERS if h in request.headers},
            timeout=float(os.environ.get("TWIN_TIMEOUT_SECONDS", "20")),
        )
    except requests.RequestException as exc:
        log.warning("digital twin call failed: %s %s: %s", request.method, path, exc)
        return Response('{"code":"502","reason":"digital twin unreachable"}', 502, mimetype="application/json")
    log.info("proxied %s /%s -> %s", request.method, path, upstream.status_code)
    return Response(upstream.content, upstream.status_code, mimetype=upstream.headers.get("Content-Type", "application/json"))


@bp.route("/tmf-api/alarmManagement/v5/<path:rest>", methods=["GET", "POST", "PATCH"])
def alarm_management(rest: str):
    return forward(f"tmf-api/alarmManagement/v5/{rest}")


@bp.route("/tmf-api/serviceProblemManagement/v5/<path:rest>", methods=["GET", "POST", "PATCH"])
def service_problem_management(rest: str):
    return forward(f"tmf-api/serviceProblemManagement/v5/{rest}")
