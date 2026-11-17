"""Shared helpers for the Event Management Operator.

Mirrors the helpers used by the Component Management operator so behaviour is
consistent across the Canvas.
"""

from __future__ import annotations

import os
from collections.abc import Mapping
from typing import Any, Dict, Iterable, List, Optional

COMPONENTNAME_LABEL = os.environ.get(
    "COMPONENTNAME_LABEL", "oda.tmforum.org/componentName"
)


def safe_get(default_value: Any, dictionary: Optional[Dict], *paths: str) -> Any:
    """Navigate nested dicts without raising ``KeyError``/``TypeError``.

    ``safe_get(None, body, "spec", "eventBroker", "topic")``
    """
    result = dictionary
    for path in paths:
        if not isinstance(result, Mapping):
            return default_value
        result = result.get(path, default_value)
        if result is None:
            return default_value
    return result


def quick_get_comp_name(body: Dict) -> Optional[str]:
    """Component name from the standard Canvas label."""
    return safe_get(None, body, "metadata", "labels", COMPONENTNAME_LABEL)


def monitored_namespaces() -> List[str]:
    """Namespaces to watch, from ``COMPONENT_NAMESPACE`` (comma separated)."""
    raw = os.environ.get("COMPONENT_NAMESPACE", "components")
    return [ns.strip() for ns in raw.split(",") if ns.strip()]


def dns_safe(value: str, max_length: int = 63) -> str:
    """Coerce a string into a valid RFC 1123 DNS label."""
    cleaned = "".join(c if (c.isalnum() or c == "-") else "-" for c in value.lower())
    cleaned = cleaned.strip("-")[:max_length].strip("-")
    return cleaned or "unnamed"


def first_specification_url(spec: Dict) -> Optional[str]:
    """Return the first specification URL.

    ``spec.specification`` is a string in ``v1beta2``/``v1beta3`` and an array of
    ``{url, version}`` objects in ``v1beta4``/``v1``. Handle both so the operator
    keeps working against components that have not been converted yet.
    """
    specification = spec.get("specification")
    if not specification:
        return None
    if isinstance(specification, str):
        return specification
    if isinstance(specification, list) and specification:
        entry = specification[0]
        if isinstance(entry, dict):
            return entry.get("url")
        if isinstance(entry, str):
            return entry
    return None


def transport_mode(spec: Dict, default: str = "adapter") -> str:
    """Resolve the subscriber transport mode.

    UC017: when ``transport`` is absent, default to ``adapter`` for backward
    compatibility with components written before the field existed.
    """
    mode = (spec.get("transport") or default).strip().lower()
    if mode not in ("adapter", "native"):
        return default
    return mode


def callback_url(spec: Dict, namespace: str) -> Optional[str]:
    """Build the component's in-cluster callback URL from the CRD spec.

    ``spec.implementation`` is the component's Service name, ``spec.port`` the
    port and ``spec.callback`` the path.
    """
    implementation = spec.get("implementation")
    if not implementation:
        return None
    port = spec.get("port") or 80
    path = spec.get("callback") or "/"
    if not path.startswith("/"):
        path = "/" + path
    scheme = "https" if str(port) == "443" else "http"
    return f"{scheme}://{implementation}.{namespace}.svc.cluster.local:{port}{path}"


def hub_url(spec: Dict, namespace: str) -> Optional[str]:
    """Build the publisher's hub URL from the CRD spec."""
    implementation = spec.get("implementation")
    if not implementation:
        return None
    port = spec.get("port") or 80
    path = spec.get("hub") or spec.get("path") or "/"
    if not path.startswith("/"):
        path = "/" + path
    scheme = "https" if str(port) == "443" else "http"
    return f"{scheme}://{implementation}.{namespace}.svc.cluster.local:{port}{path}"


def any_reference_remains(
    resources: Iterable[Dict], topic: str, exclude_uid: Optional[str] = None
) -> bool:
    """True if any resource other than ``exclude_uid`` still references ``topic``.

    Used to resolve topic ownership before deletion (UC017): a topic is only ever
    removed when no other publisher or subscriber points at it.
    """
    for resource in resources:
        uid = safe_get(None, resource, "metadata", "uid")
        if exclude_uid and uid == exclude_uid:
            continue
        if safe_get(None, resource, "status", "eventManagement", "topic") == topic:
            return True
        if safe_get(None, resource, "spec", "topic") == topic:
            return True
    return False