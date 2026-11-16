import copy
import json
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tmfc043"))
import bridge  # noqa: E402

CREATE = json.loads((Path(__file__).parent / "trap-alarm-create.json").read_text())


def test_create_alarm_to_a2a():
    out = bridge.alarm_to_a2a(CREATE)
    assert out["payloadcategory"] == "a2a"
    assert out["correlationid"] == "public-test-002"
    assert out["taskid"] == "task_alarm_public-test-002" == out["data"]["taskId"]
    assert out["contextid"] == "ctx_public-test-002" == out["data"]["contextId"]
    part = out["data"]["parts"][0]
    assert part["kind"] == "data"
    assert part["data"]["eventType"] == "alarmCreateEvent"
    alarm = part["data"]["event"]["alarm"]
    assert alarm["externalAlarmId"] == "public-test-002"
    assert "correlationId" not in alarm  # non-TMF field moved to metadata
    assert part["metadata"]["correlationId"] == "public-test-002"


def test_input_is_not_modified():
    original = copy.deepcopy(CREATE)
    bridge.alarm_to_a2a(CREATE)
    assert CREATE == original


def test_clear_uses_same_task_as_create():
    clear = copy.deepcopy(CREATE)
    msg = clear["data"]["components"]["messages"]["AlarmCreate"]["payload"]["const"]
    msg.clear()
    msg.update({"@type": "ClearAlarm", "alarmPattern": [{"id": "public-test-002"}], "clearUserId": "zabbix"})
    out = bridge.alarm_to_a2a(clear)
    assert out["taskid"] == bridge.alarm_to_a2a(CREATE)["taskid"]
    assert out["data"]["parts"][0]["data"]["eventType"] == "clearAlarmCreateEvent"


def test_plain_event_shape_accepted():
    event = {"correlationid": "c1", "data": {"event": {"alarm": {"@type": "Alarm", "externalAlarmId": "9"}}}}
    assert bridge.alarm_to_a2a(event)["taskid"] == "task_alarm_9"


@pytest.mark.parametrize("bad", [
    {"data": CREATE["data"]},  # no correlationid
    {"correlationid": "x", "data": {}},  # no alarm
    {"correlationid": "x", "data": {"components": {"messages": {"m": {"payload": {"const": {"@type": "Other"}}}}}}},
])
def test_bad_alarm_events_rejected(bad):
    with pytest.raises(ValueError):
        bridge.alarm_to_a2a(bad)


def test_problem_to_a2a_carries_payload_unchanged():
    sp = {"eventType": "serviceProblemCreateNotificationEvent", "event": {"serviceProblem": {"id": "sp-1"}}}
    out = bridge.problem_to_a2a({"correlationid": "c1", "data": sp, "taskid": "task_alarm_42"})
    assert out["data"]["parts"][0]["data"] == sp
    assert out["taskid"] == "task_alarm_42"
    assert out["payloadcategory"] == "a2a"


def test_problem_defaults_task_from_correlation():
    out = bridge.problem_to_a2a({"correlationid": "c1", "data": {"a": 1}})
    assert out["taskid"] == "task_alarm_c1" and out["contextid"] == "ctx_c1"


def test_problem_without_data_rejected():
    with pytest.raises(ValueError):
        bridge.problem_to_a2a({"correlationid": "c1"})


def _clear(pattern):
    ev = copy.deepcopy(CREATE)
    ev["data"]["components"]["messages"]["AlarmCreate"]["payload"]["const"] = {
        "@type": "ClearAlarm", "alarmPattern": pattern, "clearSystemId": "Zabbix", "clearUserId": "u"}
    return ev


def test_clear_alarm_pattern_as_bare_string():
    assert bridge.alarm_to_a2a(_clear(["ext-1"]))["taskid"] == "task_alarm_ext-1"


def test_clear_alarm_pattern_as_alarm_ref():
    assert bridge.alarm_to_a2a(_clear([{"@type": "AlarmRef", "id": "7"}]))["taskid"] == "task_alarm_7"


def test_clear_without_pattern_falls_back_to_correlation():
    assert bridge.alarm_to_a2a(_clear([]))["taskid"] == "task_alarm_public-test-002"
