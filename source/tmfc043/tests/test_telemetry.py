import io
import re
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tmfc043"))
import telemetry  # noqa: E402

TP = b"00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
LINE_RE = re.compile(
    r"^\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d\.\d{6}Z fm\.tmfc043\.kafka\.input "
    r"trace_id=[0-9a-f]{32} span_id=[0-9a-f]{16} topic=\S+ partition=\S+ offset=\S+ group=\S+ "
    r"correlation_id=\S+ cloudevent_id=\S+ outcome=\S+ duration_ms=\d+\.\d{3} error_type=\S+$"
)


def record(value=None, headers=None, topic="oda.trapReceiver.event.alarmDetected"):
    return SimpleNamespace(value=value or {"id": "evt-1", "correlationid": "c-1"}, headers=headers or [],
                           topic=topic, partition=0, offset=7)


@pytest.fixture
def stream(monkeypatch):
    buf = io.StringIO()
    monkeypatch.setattr(telemetry, "_tracer", None)
    monkeypatch.setattr(telemetry, "_provider", None)
    monkeypatch.setattr(telemetry, "_enabled", False)
    telemetry.configure(env={"OTEL_TRACES_ENABLED": "true", "OTEL_TRACES_SAMPLER_ARG": "1.0"}, stream=buf)
    yield buf
    telemetry.shutdown()


def test_one_line_per_record_in_expected_format(stream):
    with telemetry.input_record(record(), "grp") as span:
        telemetry.mark(span, "forwarded")
    lines = stream.getvalue().splitlines()
    assert len(lines) == 1
    assert LINE_RE.match(lines[0]), lines[0]
    assert "outcome=forwarded" in lines[0] and "correlation_id=c-1" in lines[0] and "error_type=none" in lines[0]


def test_skipped_and_error_outcomes(stream):
    with telemetry.input_record(record(), "grp") as span:
        telemetry.mark(span, "skipped", "InvalidInput")
    with pytest.raises(KeyError):
        with telemetry.input_record(record(), "grp") as span:
            raise KeyError("boom")
    out = stream.getvalue()
    assert "outcome=skipped" in out and "error_type=InvalidInput" in out
    assert "outcome=error" in out and "error_type=KeyError" in out
    assert "boom" not in out  # exception messages are never written


def test_valid_parent_is_continued(stream):
    with telemetry.input_record(record(headers=[("traceparent", TP)]), "grp") as span:
        telemetry.mark(span, "forwarded")
    assert "trace_id=4bf92f3577b34da6a3ce929d0e0e4736" in stream.getvalue()


@pytest.mark.parametrize("headers", [
    [("traceparent", TP), ("traceparent", TP)],  # duplicate: treated as absent
    [("traceparent", b"not-a-trace")],
    [("traceparent", None)],
    [("traceparent", "x" * 400)],
])
def test_bad_headers_do_not_break_and_are_ignored(stream, headers):
    with telemetry.input_record(record(headers=headers), "grp") as span:
        telemetry.mark(span, "forwarded")
    out = stream.getvalue()
    assert LINE_RE.match(out.strip()), out
    assert "trace_id=4bf92f3577b34da6a3ce929d0e0e4736" not in out


def test_unsafe_identifiers_are_written_as_absent(stream):
    rec = record(value={"id": "bad id\nwith newline", "correlationid": "x;rm -rf"})
    with telemetry.input_record(rec, "grp") as span:
        telemetry.mark(span, "forwarded")
    out = stream.getvalue()
    assert "correlation_id=absent" in out and "cloudevent_id=absent" in out
    assert out.count("\n") == 1


def test_disabled_writes_nothing(monkeypatch):
    buf = io.StringIO()
    monkeypatch.setattr(telemetry, "_tracer", None)
    monkeypatch.setattr(telemetry, "_provider", None)
    monkeypatch.setattr(telemetry, "_enabled", False)
    telemetry.configure(env={"OTEL_TRACES_ENABLED": "false"}, stream=buf)
    with telemetry.input_record(record(), "grp") as span:
        assert span is None
        telemetry.mark(span, "forwarded")
    assert buf.getvalue() == ""


@pytest.mark.parametrize("bad", ["2", "-0.1", "abc"])
def test_invalid_sampler_ratio_is_rejected(monkeypatch, bad):
    monkeypatch.setattr(telemetry, "_tracer", None)
    monkeypatch.setattr(telemetry, "_provider", None)
    with pytest.raises(ValueError):
        telemetry.configure(env={"OTEL_TRACES_ENABLED": "true", "OTEL_TRACES_SAMPLER_ARG": bad}, stream=io.StringIO())
    telemetry.shutdown()


def test_export_failure_does_not_raise(monkeypatch):
    class Broken(io.StringIO):
        def write(self, *_):
            raise OSError("pipe closed")

    monkeypatch.setattr(telemetry, "_tracer", None)
    monkeypatch.setattr(telemetry, "_provider", None)
    monkeypatch.setattr(telemetry, "_enabled", False)
    telemetry.configure(env={"OTEL_TRACES_ENABLED": "true"}, stream=Broken())
    try:
        with telemetry.input_record(record(), "grp") as span:
            telemetry.mark(span, "forwarded")  # processing continues; the failed write is swallowed
    finally:
        telemetry.shutdown()
