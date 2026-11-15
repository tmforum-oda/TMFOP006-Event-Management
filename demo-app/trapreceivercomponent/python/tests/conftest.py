import pytest

from trap_receiver.app import create_app


def _test_config():
    return {
        "validation": {
            "common_required": ["correlation_id"],
            "action_required": {
                "create": [
                    "externalAlarmId", "alarmRaisedTime", "alarmType",
                    "probableCause", "alarmedObjectType", "alarmedObject",
                    "sourceSystemId", "state", "severity",
                ],
                "clear": [
                    "recovery_event_id", "alarmClearedTime", "alarmPattern",
                    "clearSystemId", "clearUserId",
                ],
            },
        },
        "type_action_map": {"Alarm": "create", "ClearAlarm": "clear"},
        "kafka": {
            "producer": {
                "acks": "all", "retries": 3, "linger_ms": 5,
                "request_timeout_ms": 10000, "max_block_ms": 10000,
            },
            "cloudevent_headers": [
                "id", "source", "type", "time", "specversion",
                "datacontenttype", "correlationid",
            ],
        },
        "severity_map": {
            "Disaster": "critical", "High": "major", "Average": "minor",
            "Warning": "warning", "Info": "indeterminate",
            "Not classified": "indeterminate",
        },
        "alarm_defaults": {
            "object_type": "Alarm",
            "alarm_details_prefix": "Operational data",
        },
        "cloudevent": {
            "spec_version": "1.0",
            "data_content_type": "application/json",
            "payload_category": "asyncapi",
            "id_template": "evt-{cid}-{action}",
            "source_template": "/zabbix/{source_type}/{cid}",
            "type_template": "org.tmforum.tmf642.alarm.{action}",
            "data_schema_template": "https://example.test/asyncapi/{action}.json#/components/messages/{MessageName}",
        },
        "asyncapi": {"version": "3.1.0", "info_version": "5.0.1"},
        "action_map": {
            "create": {
                "message_name": "AlarmCreate", "channel_name": "alarmCreate",
                "operation_name": "createAlarm", "source_type": "problem",
                "asyncapi_title": "TMF642 Alarm Create",
                "channel_address": "oda.trapReceiver.event.alarmDetected",
            },
            "clear": {
                "message_name": "AlarmClear", "channel_name": "alarmClear",
                "operation_name": "clearAlarm", "source_type": "recovery",
                "asyncapi_title": "TMF642 Alarm Clear",
                "channel_address": "oda.trapReceiver.event.alarmCleared",
            },
        },
        "auth": {
            "trap_paths": ["/api/v1/trap", "/api/v1/trap/alarm", "/api/v1/trap/clear"],
            "dashboard_paths": ["/api/v1/events", "/api/v1/events/summary"],
            "open_paths": ["/api/v1/health", "/dashboard"],
        },
    }


@pytest.fixture
def config():
    return _test_config()


@pytest.fixture
def app(config, monkeypatch):
    # Kafka publish always succeeds unless a test overrides this fixture's
    # monkeypatch -- no real broker needed for these tests.
    monkeypatch.setattr("trap_receiver.app.kafka_publish", lambda *a, **k: True)
    # auth.py reads TRAP_API_KEY/DASHBOARD_API_KEY from env once at import
    # time -- set the env first, then reload so its module-level constants
    # pick up these test values. app.py's `from .auth import check_auth`
    # reference shares the same module namespace reload() updates in place,
    # so it sees the new values too.
    monkeypatch.setenv("TRAP_API_KEY", "test-trap-key")
    monkeypatch.setenv("DASHBOARD_API_KEY", "test-dashboard-key")
    import importlib
    import trap_receiver.auth as auth_module
    importlib.reload(auth_module)
    flask_app = create_app(config)
    flask_app.config["TESTING"] = True
    return flask_app


@pytest.fixture
def client(app):
    return app.test_client()
