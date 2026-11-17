"""Input tracing for TMFC043: one CONSUMER span per Kafka record handled.

The span is written to stdout as one legacy-compatible line, which the trace collector reads from the pod
logs. It is not sent over OTLP. Consequences, stated so nobody over-claims:

* The collector rebuilds each span from the line, so its trace ID is a hash of ours, its duration is
  approximate and its status is always OK.
* Only the fields in ``LINE_FIELDS`` are written. No payloads, tokens, secret values or exception messages.

Set OTEL_TRACES_ENABLED=false to turn all of this off; then nothing is written.
"""

from __future__ import annotations

import logging
import os
import re
import sys
import threading
import time
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Any, Dict, Iterator, Optional, Sequence

from opentelemetry import trace
from opentelemetry.context import Context
from opentelemetry.propagators.textmap import Getter
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import ReadableSpan, TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor, SpanExporter, SpanExportResult
from opentelemetry.sdk.trace.sampling import ParentBased, TraceIdRatioBased
from opentelemetry.trace import SpanKind, Status, StatusCode
from opentelemetry.trace.propagation.tracecontext import TraceContextTextMapPropagator

log = logging.getLogger("tmfc043.telemetry")

SPAN_NAME = "fm.tmfc043.kafka.input"
MAX_LINE = 4096
MAX_HEADER = 256
LINE_FIELDS = (
    "trace_id", "span_id", "topic", "partition", "offset", "group", "correlation_id",
    "cloudevent_id", "outcome", "duration_ms", "error_type",
)
_CONTROL = re.compile(r"[\x00-\x1f\x7f]+")
_SAFE_ID = re.compile(r"^[A-Za-z0-9._:/\-]{1,128}$")

_provider: Optional[TracerProvider] = None
_tracer: Optional[trace.Tracer] = None
_enabled = False
_lock = threading.Lock()


def _clean(value: Any, limit: int = 256) -> str:
    """Make one bounded, single-line text value. Control characters become spaces."""
    text = _CONTROL.sub(" ", str(value)).strip()
    return text[:limit] if text else "none"


def _id_or_absent(value: Any) -> str:
    """Identifiers are only written when they look like identifiers; anything else is 'absent'."""
    text = "" if value is None else str(value).strip()
    return text if _SAFE_ID.match(text) else "absent"


def _ratio(raw: Optional[str]) -> float:
    try:
        ratio = float(raw) if raw not in (None, "") else 1.0
    except ValueError:
        raise ValueError(f"OTEL_TRACES_SAMPLER_ARG must be a number, got {raw!r}") from None
    if not 0.0 <= ratio <= 1.0:
        raise ValueError(f"OTEL_TRACES_SAMPLER_ARG must be within [0,1], got {ratio}")
    return ratio


class LegacyLineExporter(SpanExporter):
    """Writes one line per finished span to the given stream. Export failures never reach the caller."""

    def __init__(self, stream=None):
        self._stream = stream or sys.stdout
        self._lock = threading.Lock()

    def export(self, spans: Sequence[ReadableSpan]) -> SpanExportResult:
        try:
            for span in spans:
                if span.name != SPAN_NAME:
                    continue
                line = self.format(span)[:MAX_LINE]
                with self._lock:
                    self._stream.write(line + "\n")
                    self._stream.flush()
            return SpanExportResult.SUCCESS
        except Exception:  # noqa: BLE001 - tracing must never break record processing
            return SpanExportResult.FAILURE

    @staticmethod
    def format(span: ReadableSpan) -> str:
        ctx = span.context
        attrs = span.attributes or {}
        ts = datetime.fromtimestamp(span.end_time / 1e9, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")
        duration_ms = max(0.0, (span.end_time - span.start_time) / 1e6)
        fields = {
            "trace_id": format(ctx.trace_id, "032x"),
            "span_id": format(ctx.span_id, "016x"),
            "topic": _clean(attrs.get("messaging.destination.name", "none")),
            "partition": _clean(attrs.get("messaging.destination.partition.id", "none"), 16),
            "offset": _clean(attrs.get("messaging.kafka.offset", "none"), 24),
            "group": _clean(attrs.get("messaging.consumer.group.name", "none")),
            "correlation_id": _id_or_absent(attrs.get("oda.correlation_id")),
            "cloudevent_id": _id_or_absent(attrs.get("cloudevents.event_id")),
            "outcome": _clean(attrs.get("oda.input.outcome", "error"), 32),
            "duration_ms": f"{duration_ms:.3f}",
            "error_type": _clean(attrs.get("error.type", "none"), 64),
        }
        body = " ".join(f"{key}={fields[key]}" for key in LINE_FIELDS)
        return f"{ts} {SPAN_NAME} {body}"

    def shutdown(self) -> None:
        return None


def configure(env=os.environ, stream=None) -> None:
    """Set up the tracer once per process. Call before the route threads start."""
    global _provider, _tracer, _enabled
    with _lock:
        if _provider is not None or _tracer is not None:
            return
        _enabled = env.get("OTEL_TRACES_ENABLED", "true").strip().lower() in ("1", "true", "yes")
        if not _enabled:
            _tracer = trace.get_tracer("tmfc043")  # no processor attached: spans are created but not written
            return
        resource = Resource.create({
            "service.name": env.get("OTEL_SERVICE_NAME", "tmfc043"),
            "service.namespace": "oda-canvas",
            "oda.component.name": env.get("COMPONENT_NAME", "tmfc043"),
            "service.version": env.get("APP_VERSION", "unknown"),
            "deployment.environment": env.get("DEPLOYMENT_ENVIRONMENT", "unset"),
        })
        sampler = ParentBased(TraceIdRatioBased(_ratio(env.get("OTEL_TRACES_SAMPLER_ARG"))))
        _provider = TracerProvider(resource=resource, sampler=sampler)
        _provider.add_span_processor(SimpleSpanProcessor(LegacyLineExporter(stream)))
        _tracer = _provider.get_tracer("tmfc043")


def shutdown() -> None:
    """Flush and stop the provider once, at process end."""
    global _provider
    if _provider is not None:
        _provider.shutdown()
        _provider = None


class _HeaderGetter(Getter):
    """Reads only traceparent/tracestate. Duplicate or oversized values are treated as absent."""

    def get(self, carrier, key: str):
        found = []
        for name, value in carrier or []:
            if name == key:
                found.append(value)
        if len(found) != 1:
            return None
        raw = found[0]
        if raw is None or len(raw) > MAX_HEADER:
            return None
        try:
            return [raw.decode("ascii")]
        except (UnicodeDecodeError, AttributeError):
            return None

    def keys(self, carrier):
        return [name for name, _ in (carrier or [])]


def _parent_context(headers) -> Context:
    try:
        return TraceContextTextMapPropagator().extract(carrier=headers or [], getter=_HeaderGetter())
    except Exception:  # noqa: BLE001 - a broken header must not break the record
        return Context()


@contextmanager
def input_record(record, group: str) -> Iterator[Optional[trace.Span]]:
    """One CONSUMER span for one delivery of one record. Yields the span, or None when tracing is off."""
    if _tracer is None or not _enabled:
        yield None
        return
    headers = getattr(record, "headers", None)
    parent = _parent_context(headers)
    value = getattr(record, "value", None)
    cloudevent = value if isinstance(value, dict) else {}
    with _tracer.start_as_current_span(
        SPAN_NAME,
        context=parent,
        kind=SpanKind.CONSUMER,
        record_exception=False,
        set_status_on_exception=False,
    ) as span:
        span.set_attribute("messaging.system", "kafka")
        span.set_attribute("messaging.destination.name", getattr(record, "topic", "none"))
        span.set_attribute("messaging.destination.partition.id", str(getattr(record, "partition", "none")))
        span.set_attribute("messaging.kafka.offset", str(getattr(record, "offset", "none")))
        span.set_attribute("messaging.consumer.group.name", group)
        correlation = cloudevent.get("correlationid")
        if correlation is not None:
            span.set_attribute("oda.correlation_id", str(correlation))
        if cloudevent.get("id") is not None:
            span.set_attribute("cloudevents.event_id", str(cloudevent.get("id")))
        span.set_attribute("oda.input.outcome", "error")  # overwritten explicitly by the caller
        try:
            yield span
        except BaseException as exc:
            span.set_attribute("error.type", type(exc).__name__)
            span.set_attribute("oda.input.outcome", "error")
            span.set_status(Status(StatusCode.ERROR))
            raise


def mark(span: Optional[trace.Span], outcome: str, error_type: Optional[str] = None) -> None:
    """Set the outcome of an input span. Safe to call with None."""
    if span is None:
        return
    span.set_attribute("oda.input.outcome", outcome)
    if error_type:
        span.set_attribute("error.type", error_type)
        span.set_status(Status(StatusCode.ERROR))
