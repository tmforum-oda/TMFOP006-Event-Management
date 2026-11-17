"""
Tests built around the real Zabbix create/clear samples used to validate
this component end-to-end on AWS EKS (correlation_id "32942", interface
"Gi1/0(): Link down"), plus the negative cases trap-receiver-component-
changes.md section E calls out: missing TMF642-required fields, a clear
with no alarmPattern, bad input types, and a swallowed Kafka failure.
"""
TRAP_HEADERS = {"X-API-Key": "test-trap-key", "Content-Type": "application/json"}
DASHBOARD_HEADERS = {"X-API-Key": "test-dashboard-key"}

CREATE_PAYLOAD = {
    "@type": "Alarm",
    "correlation_id": "32942",
    "externalAlarmId": "32942",
    "state": "raised",
    "alarmType": "communicationsAlarm",
    "severity": "Disaster",
    "probableCause": "lossOfSignal",
    "specificProblem": "Interface Gi1/0(): Link down",
    "alarmedObjectType": "Interface",
    "alarmedObject": {
        "@type": "AlarmedObjectRef", "id": "Gi1/0", "@referredType": "Host",
    },
    "sourceSystemId": "zabbix",
    "alarmRaisedTime": "2026-11-01T10:15:00Z",
    "alarmDetails": "Interface Gi1/0(): Link down",
}

CLEAR_PAYLOAD = {
    "@type": "ClearAlarm",
    "correlation_id": "32942",
    "recovery_event_id": "32943",
    "alarmClearedTime": "2026-11-01T10:20:00Z",
    "alarmPattern": [{"@type": "AlarmRef", "id": "32942"}],
    "clearSystemId": "zabbix",
    "clearUserId": "zabbix-system",
}


def test_health(client):
    resp = client.get("/api/v1/health")
    assert resp.status_code == 200
    assert resp.get_json()["status"] == "UP"


def test_create_success(client):
    resp = client.post("/api/v1/trap", json=CREATE_PAYLOAD, headers=TRAP_HEADERS)
    assert resp.status_code == 200
    ce = resp.get_json()
    assert ce["type"] == "org.tmforum.tmf642.alarm.create"
    msgs = ce["data"]["channels"]["alarmCreate"]
    assert msgs["address"] == "oda.trapReceiver.event.alarmDetected"
    alarm = ce["data"]["components"]["messages"]["AlarmCreate"]["payload"]["const"]
    assert alarm["perceivedSeverity"] == "critical"  # Disaster -> critical
    assert alarm["alarmedObject"]["id"] == "Gi1/0"


def test_clear_success_links_to_create(client):
    resp = client.post("/api/v1/trap", json=CLEAR_PAYLOAD, headers=TRAP_HEADERS)
    assert resp.status_code == 200
    ce = resp.get_json()
    assert ce["type"] == "org.tmforum.tmf642.alarm.clear"
    alarm = ce["data"]["components"]["messages"]["AlarmClear"]["payload"]["const"]
    # The clear's alarmPattern must reference the create's externalAlarmId
    # so a consumer can correlate the two.
    assert alarm["alarmPattern"][0]["id"] == CREATE_PAYLOAD["externalAlarmId"]
    assert alarm["clearSystemId"] == "zabbix"


def test_create_missing_required_field_returns_400(client):
    payload = dict(CREATE_PAYLOAD)
    del payload["probableCause"]
    resp = client.post("/api/v1/trap", json=payload, headers=TRAP_HEADERS)
    assert resp.status_code == 400
    assert "probableCause" in resp.get_json()["missing_fields"]


def test_create_no_longer_invents_values(client):
    """Historical bug: omitting probableCause used to silently return
    'lossOfSignal' instead of failing. Confirm it 400s instead."""
    payload = dict(CREATE_PAYLOAD)
    del payload["alarmedObjectType"]
    resp = client.post("/api/v1/trap", json=payload, headers=TRAP_HEADERS)
    assert resp.status_code == 400


def test_create_invalid_severity_returns_400(client):
    payload = dict(CREATE_PAYLOAD, severity="Catastrophic")
    resp = client.post("/api/v1/trap", json=payload, headers=TRAP_HEADERS)
    assert resp.status_code == 400
    assert resp.get_json()["error"] == "invalid_severity"


def test_clear_missing_alarm_pattern_returns_400(client):
    payload = dict(CLEAR_PAYLOAD)
    del payload["alarmPattern"]
    resp = client.post("/api/v1/trap", json=payload, headers=TRAP_HEADERS)
    assert resp.status_code == 400


def test_clear_empty_alarm_pattern_returns_400(client):
    payload = dict(CLEAR_PAYLOAD, alarmPattern=[])
    resp = client.post("/api/v1/trap", json=payload, headers=TRAP_HEADERS)
    assert resp.status_code == 400


def test_clear_missing_clear_system_id_returns_400(client):
    payload = dict(CLEAR_PAYLOAD)
    del payload["clearSystemId"]
    resp = client.post("/api/v1/trap", json=payload, headers=TRAP_HEADERS)
    assert resp.status_code == 400
    assert "clearSystemId" in resp.get_json()["missing_fields"]


def test_null_body_returns_400_not_500(client):
    resp = client.post(
        "/api/v1/trap", data="null", content_type="application/json",
        headers=TRAP_HEADERS,
    )
    assert resp.status_code == 400


def test_non_object_body_returns_400_not_500(client):
    resp = client.post(
        "/api/v1/trap", json=["not", "an", "object"], headers=TRAP_HEADERS,
    )
    assert resp.status_code == 400


def test_numeric_correlation_id_returns_400(client):
    payload = dict(CREATE_PAYLOAD, correlation_id=32942)
    resp = client.post("/api/v1/trap", json=payload, headers=TRAP_HEADERS)
    assert resp.status_code == 400


def test_non_string_action_returns_400_not_500(client):
    payload = dict(CREATE_PAYLOAD, action=12345)
    resp = client.post("/api/v1/trap", json=payload, headers=TRAP_HEADERS)
    assert resp.status_code == 400


def test_missing_api_key_returns_401(client):
    resp = client.post(
        "/api/v1/trap", json=CREATE_PAYLOAD,
        headers={"Content-Type": "application/json"},
    )
    assert resp.status_code == 401


def test_kafka_publish_failure_returns_502_not_200(client, monkeypatch):
    """Historical bug: a failed Kafka publish used to still return 200,
    so the sender believed the alarm was delivered when it was lost."""
    monkeypatch.setattr("trap_receiver.app.kafka_publish", lambda *a, **k: False)
    resp = client.post("/api/v1/trap", json=CREATE_PAYLOAD, headers=TRAP_HEADERS)
    assert resp.status_code == 502
    assert resp.get_json()["error"] == "kafka_publish_failed"


def test_dashboard_html_does_not_require_api_key(client):
    resp = client.get("/dashboard")
    assert resp.status_code == 200


def test_events_json_requires_api_key(client):
    resp = client.get("/api/v1/events")
    assert resp.status_code == 401


def test_events_json_with_key_succeeds(client):
    client.post("/api/v1/trap", json=CREATE_PAYLOAD, headers=TRAP_HEADERS)
    resp = client.get("/api/v1/events", headers=DASHBOARD_HEADERS)
    assert resp.status_code == 200
    assert isinstance(resp.get_json(), list)


def test_events_bad_limit_returns_400_not_500(client):
    resp = client.get("/api/v1/events?limit=not-a-number", headers=DASHBOARD_HEADERS)
    assert resp.status_code == 400
