"""Topic naming convention for the event-driven fault-management flow.

Pure Python on purpose: no Kafka or kopf imports, so it can be unit-tested
without a broker or a cluster.

Convention (letter case is PRESERVED, exactly as written in the spec)::

    <prefix>.<domain>[.<kind>].<eventName>

    oda.trapReceiver.event.alarmDetected
    oda.alarmManagement.command.createAlarm
    oda.tmf642.alarmCreateEvent

``domain`` is an API id (``tmf642``) or a functional area (``digitalTwin``).
``kind`` is optional and is ``event`` or ``command``.

Three ways a topic name is resolved, in this order (see :func:`resolve_topic`):

1. explicit ``spec.topic``                 -> used as written (validated only)
2. ``spec.topicDomain`` [+ ``topicKind``, ``topicEvent``]
                                           -> built by :func:`conventional_topic`
3. neither                                 -> legacy ``kafka_utils.topic_name`` (lower-cased,
   ``oda.<component>.<event>``) - unchanged, so existing demos keep their topics.

An explicit topic is used as written. :func:`check_convention` only reports a
warning when it does not follow the convention; it never rejects or rewrites it.

Kafka topic names are case-sensitive, so two parties that spell a topic with a
different letter case never meet. :func:`case_variants` lets the operator catch that.
"""

from __future__ import annotations

import re
from collections.abc import Mapping
from typing import Iterable, List, Optional

MAX_TOPIC_LENGTH = 249  # Kafka limit
ALLOWED_KINDS = ("event", "command")

_ILLEGAL = re.compile(r"[^A-Za-z0-9._-]")


def sanitize(raw: str) -> str:
    """Fold characters Kafka does not allow to ``-``. Letter case is not touched."""
    return _ILLEGAL.sub("-", raw.strip())


def validate_topic(name: str) -> str:
    """Return ``name`` unchanged if Kafka accepts it, otherwise raise ``ValueError``."""
    if not isinstance(name, str) or not name.strip():
        raise ValueError("topic name is empty")
    name = name.strip()
    if name in (".", ".."):
        raise ValueError(f"invalid Kafka topic name: {name!r}")
    if len(name) > MAX_TOPIC_LENGTH:
        raise ValueError(f"topic name is {len(name)} characters; Kafka allows {MAX_TOPIC_LENGTH}")
    bad = sorted(set(_ILLEGAL.findall(name)))
    if bad:
        raise ValueError(f"topic name {name!r} contains characters Kafka does not allow: {bad}")
    return name


def conventional_topic(
    prefix: str, domain: str, kind: Optional[str], event_name: str
) -> str:
    """Build ``<prefix>.<domain>[.<kind>].<eventName>``; letter case preserved.

    ``event_name`` is ``spec.topicEvent`` when the topic's last segment differs
    from the notification name (for example notification ``alarmCreateNotificationEvent``
    published on ``oda.tmf642.alarmCreateEvent``), otherwise ``spec.name``.
    """
    if not domain or not str(domain).strip():
        raise ValueError("topicDomain is required to build a conventional topic name")
    if not event_name or not str(event_name).strip():
        raise ValueError("an event name is required to build a conventional topic name")
    if kind and kind not in ALLOWED_KINDS:
        raise ValueError(f"topicKind must be one of {list(ALLOWED_KINDS)}, got {kind!r}")
    parts = [prefix, domain, kind, event_name]
    return validate_topic(".".join(sanitize(str(p)) for p in parts if p))


def check_convention(topic: str, prefix: str = "oda") -> Optional[str]:
    """Return a warning string if ``topic`` does not follow the convention, else None.

    Shape: ``<prefix>.<domain>[.event|.command].<eventName>`` where domain and
    eventName start with a letter and contain only letters and digits.
    """
    pattern = (
        r"^" + re.escape(prefix) + r"\.[A-Za-z][A-Za-z0-9]*"
        r"(\.(?:event|command))?\.[A-Za-z][A-Za-z0-9]*$"
    )
    if re.match(pattern, topic):
        return None
    return (
        f"topic {topic!r} does not follow {prefix}.<domain>[.event|.command].<eventName> "
        f"(letters and digits only in each segment)"
    )


def case_variants(topic: str, existing: Iterable[str]) -> List[str]:
    """Existing topics that equal ``topic`` ignoring case but not exactly."""
    wanted = topic.lower()
    return sorted(t for t in existing if t != topic and t.lower() == wanted)


def api_type_of(spec: dict) -> Optional[str]:
    """API type of a notification: ``spec.apiType`` if set, else the first
    ``spec.specification[]`` entry's ``apiType``.

    Notifications created by hand carry ``apiType`` at the top of ``spec``. Those
    created by the Component Operator are a copy of the component's event entry,
    where the Component CRD keeps ``apiType`` inside ``specification[]``.
    """
    if not isinstance(spec, Mapping):
        return None
    if spec.get("apiType"):
        return spec["apiType"]
    spec_list = spec.get("specification")
    if isinstance(spec_list, list):
        for entry in spec_list:
            if isinstance(entry, Mapping) and entry.get("apiType"):
                return entry["apiType"]
    return None


def resolve_topic(spec, component_name, event_name, prefix, legacy):
    """Resolve the Kafka topic for a notification. Returns ``(topic, warnings)``.

    ``legacy`` is a zero-argument callable returning the old derived name (the
    operator passes a lambda around ``kafka_utils.topic_name``). It is only
    called when neither an explicit topic nor a ``topicDomain`` is present.

    Raises ``ValueError`` for a name Kafka would reject; the operator turns that
    into a ``kopf.PermanentError`` because retrying cannot fix a bad spec.
    """
    broker = spec.get("eventBroker") if isinstance(spec.get("eventBroker"), Mapping) else {}

    explicit = spec.get("topic") or broker.get("topic")
    if explicit:
        topic = validate_topic(explicit)
        warning = check_convention(topic, prefix)
        return topic, ([warning] if warning else [])

    domain = spec.get("topicDomain") or broker.get("domain")
    if domain:
        kind = spec.get("topicKind") or broker.get("kind")
        event = spec.get("topicEvent") or broker.get("event") or event_name
        return conventional_topic(prefix, domain, kind, event), []

    return legacy(), []
