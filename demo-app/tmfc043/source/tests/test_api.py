import sys
from pathlib import Path

import pytest
import requests

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tmfc043"))
import app as c043  # noqa: E402


class _Resp:
    def __init__(self, status=201, body=b'{"id":"a1"}'):
        self.status_code, self.content, self.headers = status, body, {"Content-Type": "application/json"}


@pytest.fixture
def client():
    return c043.app.test_client()


def test_503_when_twin_not_configured(client, monkeypatch):
    monkeypatch.delenv("TWIN_BASE_URL", raising=False)
    assert client.post("/tmf-api/alarmManagement/v5/alarm", json={}).status_code == 503


def test_request_is_forwarded_and_response_returned(client, monkeypatch):
    monkeypatch.setenv("TWIN_BASE_URL", "http://twin:9000/")
    seen = {}

    def fake(method, url, **kw):
        seen.update(method=method, url=url, data=kw["data"], headers=kw["headers"])
        return _Resp()

    monkeypatch.setattr(requests, "request", fake)
    r = client.post("/tmf-api/alarmManagement/v5/clearAlarm", data=b'{"x":1}', headers={"Content-Type": "application/json", "Cookie": "no"})
    assert (r.status_code, r.get_json()) == (201, {"id": "a1"})
    assert seen["url"] == "http://twin:9000/tmf-api/alarmManagement/v5/clearAlarm"
    assert seen["data"] == b'{"x":1}' and "Cookie" not in seen["headers"]


def test_service_problem_patch_forwarded(client, monkeypatch):
    monkeypatch.setenv("TWIN_BASE_URL", "http://twin:9000")
    monkeypatch.setattr(requests, "request", lambda m, u, **kw: _Resp(200))
    assert client.patch("/tmf-api/serviceProblemManagement/v5/serviceProblem/sp-1", json={}).status_code == 200


def test_502_when_twin_down(client, monkeypatch):
    monkeypatch.setenv("TWIN_BASE_URL", "http://twin:9000")

    def boom(*a, **k):
        raise requests.ConnectionError("down")

    monkeypatch.setattr(requests, "request", boom)
    assert client.post("/tmf-api/alarmManagement/v5/alarm", json={}).status_code == 502
