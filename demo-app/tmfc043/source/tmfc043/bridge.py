"""Pure message transforms for TMFC043 (no Kafka, no HTTP, so they are unit-testable).

Two transforms, one per kind of inbound event:

* ``alarm_to_a2a``    trap receiver event (asyncapi)   -> A2A command for the alarm agent
* ``problem_to_a2a``  service problem event (asyncapi) -> A2A message for the service problem agent

A2A over Kafka is a project convention, not an official A2A binding.
"""

from __future__ import annotations

import os
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, Tuple

CE_TYPE = os.environ.get("A2A_CE_TYPE", "com.jio.a2a.message.v1")
CE_SOURCE = os.environ.get("A2A_CE_SOURCE", "/components/tmfc043")
CE_DATASCHEMA = os.environ.get(
    "A2A_DATASCHEMA",
    "https://github.com/tmforum-oda/TMFOP006-Event-Management/blob/main/demo-app/tmfc043/README.md",
)

EVENT_TYPE = {"Alarm": "alarmCreateEvent", "ClearAlarm": "clearAlarmCreateEvent"}
RESOURCE_KEY = {"Alarm": "alarm", "ClearAlarm": "clearAlarm"}
# Fields the trap receiver adds that are not TMF642 fields.
NON_TMF_FIELDS = ("correlationId", "recoveryEventId")
ENVELOPE_EXTRAS = ("user_id", "conversation_id", "conversation_name", "recoveryeventid")


def _now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _envelope(correlationid: str, task_id: str, context_id: str, part: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "specversion": "1.0",
        "id": str(uuid.uuid4()),
        "source": CE_SOURCE,
        "type": CE_TYPE,
        "datacontenttype": "application/json",
        "time": _now(),
        "correlationid": correlationid,
        "payloadcategory": "a2a",
        "dataschema": CE_DATASCHEMA,
        "taskid": task_id,
        "contextid": context_id,
        "data": {
            "kind": "message",
            "messageId": str(uuid.uuid4()),
            "taskId": task_id,
            "contextId": context_id,
            "role": "user",
            "parts": [part],
        },
    }


def extract_alarm(hop1: Dict[str, Any]) -> Tuple[str, Dict[str, Any]]:
    """Return ``(tmf_type, resource)`` from a trap receiver CloudEvent.

    ``data`` is an AsyncAPI document; the TMF642 object is the ``const`` of the
    message payload. The plain ``{event: {alarm | clearAlarm}}`` shape is also accepted.
    """
    data = hop1.get("data") or {}
    messages = (data.get("components") or {}).get("messages") or {}
    for message in messages.values():
        const = (message.get("payload") or {}).get("const")
        if const:
            tmf_type = const.get("@type")
            if tmf_type not in EVENT_TYPE:
                raise ValueError(f"unsupported @type {tmf_type!r}; expected Alarm or ClearAlarm")
            return tmf_type, dict(const)
    event = data.get("event") or {}
    for key, tmf_type in (("alarm", "Alarm"), ("clearAlarm", "ClearAlarm")):
        if key in event:
            return tmf_type, dict(event[key])
    raise ValueError("event has no TMF642 alarm or clearAlarm in a known shape")


def _alarm_id(tmf_type: str, resource: Dict[str, Any], correlationid: str) -> str:
    if tmf_type == "Alarm":
        found = resource.get("externalAlarmId")
    else:
        # alarmPattern is a list of AlarmRef objects; some senders put the bare id string.
        pattern = resource.get("alarmPattern") or []
        first = pattern[0] if pattern else None
        found = first.get("id") if isinstance(first, dict) else first
        found = found or resource.get("externalAlarmId")
    return found or correlationid


def alarm_to_a2a(hop1: Dict[str, Any]) -> Dict[str, Any]:
    """Build the A2A CloudEvent for the alarm management agent.

    correlationid is copied unchanged; the task id is the same for the create and
    the clear of one alarm, so the agent can match them.
    """
    correlationid = hop1.get("correlationid")
    if not correlationid:
        raise ValueError("event has no correlationid")
    tmf_type, resource = extract_alarm(hop1)
    extras = {k: resource.pop(k) for k in NON_TMF_FIELDS if k in resource}
    extras.update({k: hop1[k] for k in ENVELOPE_EXTRAS if k in hop1})

    part: Dict[str, Any] = {
        "kind": "data",
        "data": {"eventType": EVENT_TYPE[tmf_type], "event": {RESOURCE_KEY[tmf_type]: resource}},
    }
    if extras:
        part["metadata"] = extras
    task_id = f"task_alarm_{_alarm_id(tmf_type, resource, correlationid)}"
    return _envelope(correlationid, task_id, f"ctx_{correlationid}", part)


def problem_to_a2a(event: Dict[str, Any]) -> Dict[str, Any]:
    """Wrap a TMF656 service problem event (asyncapi) as an A2A message.

    The service problem payload is carried unchanged in the data part.
    """
    correlationid = event.get("correlationid")
    if not correlationid:
        raise ValueError("event has no correlationid")
    data = event.get("data")
    if not isinstance(data, dict) or not data:
        raise ValueError("event has no data to forward")
    part: Dict[str, Any] = {"kind": "data", "data": data}
    task_id = event.get("taskid") or f"task_alarm_{correlationid}"
    context_id = event.get("contextid") or f"ctx_{correlationid}"
    return _envelope(correlationid, task_id, context_id, part)
