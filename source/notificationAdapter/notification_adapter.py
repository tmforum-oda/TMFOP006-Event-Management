# Copyright 2026 Reliance Jio Infocomm Limited (or successor entity)
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Kafka -> TMF REST notification adapter.

The subscriber-side half of the pub/sub path. One instance runs per
``SubscribedNotification`` in ``adapter`` transport mode. It consumes the Kafka
topic the publisher owns and POSTs each event to the component's TMF listener
callback, so the ODA Component needs no Kafka client, no broker credentials and
no raised privileges — the whole point of UC004/UC008.

Ships in its own image, built and versioned independently from the operator
(see source/notificationAdapter/Dockerfile). Keep ADAPTER_IMAGE in the operator's ConfigMap
pointed at whatever tag this was built into.

Delivery semantics
------------------
At-least-once. Offsets are committed only after the callback returns 2xx, so a
crash mid-delivery replays the event rather than dropping it. TMF listeners are
expected to be idempotent on ``event.id``.

A 4xx from the callback is treated as a poison message: retrying a malformed
payload forever would stall the partition, so it is logged and the offset is
committed. A 5xx or a connection error is retried with backoff and the offset is
*not* committed.

Environment
-----------
KAFKA_BOOTSTRAP_SERVERS   broker list                     (required)
KAFKA_TOPIC               topic to consume                (required)
KAFKA_GROUP_ID            consumer group                  (required)
CALLBACK_URL              component listener endpoint     (required)
KAFKA_SECURITY_PROTOCOL   PLAINTEXT | SSL | SASL_*        (default PLAINTEXT)
KAFKA_SASL_MECHANISM      e.g. SCRAM-SHA-512
KAFKA_SASL_USERNAME       from Secret
KAFKA_SASL_PASSWORD       from Secret
KAFKA_SSL_CAFILE          mounted CA bundle
EVENT_FILTER_QUERY        TMF-style filter, e.g. eventType=ProductOfferingCreateEvent
CALLBACK_TIMEOUT_SECONDS  default 10
CALLBACK_MAX_RETRIES      default 5
LISTEN_PORT               health server port, default 8080
LOGGING                   log level integer, default 20
"""

from __future__ import annotations

import json
import logging
import os
import signal
import ssl
import sys
import threading
import time
from http.server import BaseHTTPRequestHandler, HTTPServer
from typing import Any, Dict, Optional
from urllib.parse import parse_qsl

import requests
from kafka import KafkaConsumer
from kafka.errors import KafkaError

# --------------------------------------------------------------------------- #
# Configuration                                                               #
# --------------------------------------------------------------------------- #

LOG_LEVEL = int(os.environ.get("LOGGING", logging.INFO))
logging.basicConfig(
    level=LOG_LEVEL,
    format="%(asctime)s %(levelname)s [notification-adapter] %(message)s",
)
logger = logging.getLogger("notification-adapter")

BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "")
TOPIC = os.environ.get("KAFKA_TOPIC", "")
GROUP_ID = os.environ.get("KAFKA_GROUP_ID", "")
CALLBACK_URL = os.environ.get("CALLBACK_URL", "")

SECURITY_PROTOCOL = os.environ.get("KAFKA_SECURITY_PROTOCOL", "PLAINTEXT")
SASL_MECHANISM = os.environ.get("KAFKA_SASL_MECHANISM") or None
SASL_USERNAME = os.environ.get("KAFKA_SASL_USERNAME") or None
SASL_PASSWORD = os.environ.get("KAFKA_SASL_PASSWORD") or None
SSL_CAFILE = os.environ.get("KAFKA_SSL_CAFILE") or None

EVENT_FILTER_QUERY = os.environ.get("EVENT_FILTER_QUERY") or None
CALLBACK_TIMEOUT = float(os.environ.get("CALLBACK_TIMEOUT_SECONDS", "10"))
CALLBACK_MAX_RETRIES = int(os.environ.get("CALLBACK_MAX_RETRIES", "5"))
LISTEN_PORT = int(os.environ.get("LISTEN_PORT", "8080"))

#: Flipped to False by SIGTERM so the consume loop exits cleanly and commits.
_running = threading.Event()
_running.set()

#: Surfaced on /healthz so Kubernetes restarts a wedged adapter.
_state: Dict[str, Any] = {
    "consumerConnected": False,
    "eventsDelivered": 0,
    "eventsFiltered": 0,
    "eventsFailed": 0,
    "lastError": None,
    "lastDeliveryEpoch": None,
}


# --------------------------------------------------------------------------- #
# Event filtering                                                             #
# --------------------------------------------------------------------------- #


def parse_filter(query: Optional[str]) -> Dict[str, str]:
    """Parse ``spec.query`` into field->value pairs.

    The CRD documents ``query`` as "filter the events that will be sent to the
    callback endpoint", matching the TMF Hub/Listener style:
    ``eventType=ProductOfferingCreateEvent&priority=high``.
    """
    if not query:
        return {}
    return {k: v for k, v in parse_qsl(query.lstrip("?"), keep_blank_values=False)}


def _lookup(event: Dict[str, Any], field: str) -> Any:
    """Resolve a possibly-dotted field, checking the event envelope first.

    TMF events nest the payload under ``event``, so ``id`` should match either
    the envelope's ``id`` or the payload's. Dotted paths address the payload
    explicitly (``event.productOffering.id``).
    """
    if "." in field:
        cursor: Any = event
        for part in field.split("."):
            if not isinstance(cursor, dict):
                return None
            cursor = cursor.get(part)
        return cursor
    if field in event:
        return event[field]
    payload = event.get("event")
    if isinstance(payload, dict):
        return payload.get(field)
    return None


def event_matches(event: Dict[str, Any], criteria: Dict[str, str]) -> bool:
    """True when the event satisfies every filter criterion.

    An empty filter matches everything. A criterion naming a field the event
    does not carry does not match — filters narrow, they never widen.
    """
    for field, expected in criteria.items():
        actual = _lookup(event, field)
        if actual is None:
            return False
        if str(actual) != str(expected):
            return False
    return True


# --------------------------------------------------------------------------- #
# Callback delivery                                                           #
# --------------------------------------------------------------------------- #


class PoisonMessage(Exception):
    """Callback rejected the payload with a 4xx — retrying cannot help."""


def extract_headers(kafka_headers) -> Dict[str, str]:
    """Turn a Kafka ConsumerRecord's ``.headers`` (a list of (str, bytes)
    tuples, or None) into a plain string dict, forwarding only what's
    worth propagating to an HTTP callback -- correlationid, plus this
    message's own ``ce_id`` forwarded as ``ce-causationid``. From the
    receiver's perspective, "the id of the event I just received" *is*
    "the id that caused whatever I do next" -- that's exactly what
    causationid means one hop downstream. Silently skips anything that
    isn't valid UTF-8 rather than crashing the whole delivery over a
    malformed header.
    """
    result: Dict[str, str] = {}
    if not kafka_headers:
        return result
    for key, value in kafka_headers:
        if key not in ("correlationid", "ce_id"):
            continue
        try:
            decoded = value.decode("utf-8")
        except (UnicodeDecodeError, AttributeError):
            continue
        if key == "correlationid":
            result["ce-correlationid"] = decoded
        elif key == "ce_id":
            result["ce-causationid"] = decoded
    return result


def deliver(
    session: requests.Session,
    url: str,
    event: Dict[str, Any],
    extra_headers: Optional[Dict[str, str]] = None,
    max_retries: int = CALLBACK_MAX_RETRIES,
    timeout: float = CALLBACK_TIMEOUT,
    sleep=time.sleep,
) -> int:
    """POST one event to the component's listener.

    ``extra_headers`` carries forward whatever Kafka message headers the
    publisher set (e.g. ``correlationid``) so a correlation id set at the
    very start of a flow survives this hop too, not just the direct
    REST-to-Kafka leg. Content-Type always wins if a caller's extra_headers
    somehow collided with it.

    Returns the final status code on success. Raises :class:`PoisonMessage` on a
    4xx (caller commits the offset and moves on) and :class:`requests.RequestException`
    when retries are exhausted (caller must NOT commit).
    """
    headers = dict(extra_headers or {})
    headers["Content-Type"] = "application/json"

    last_exc: Optional[Exception] = None
    for attempt in range(1, max_retries + 1):
        try:
            response = session.post(
                url,
                json=event,
                headers=headers,
                timeout=timeout,
            )
            if 200 <= response.status_code < 300:
                return response.status_code
            if 400 <= response.status_code < 500:
                raise PoisonMessage(
                    f"callback rejected event with {response.status_code}: "
                    f"{response.text[:200]}"
                )
            last_exc = requests.HTTPError(
                f"callback returned {response.status_code}", response=response
            )
        except PoisonMessage:
            raise
        except requests.RequestException as exc:
            last_exc = exc

        if attempt < max_retries:
            backoff = min(2 ** (attempt - 1), 30)
            logger.warning(
                "callback attempt %d/%d failed (%s); retrying in %ss",
                attempt,
                max_retries,
                last_exc,
                backoff,
            )
            sleep(backoff)

    raise requests.RequestException(
        f"callback failed after {max_retries} attempts: {last_exc}"
    )


# --------------------------------------------------------------------------- #
# Health endpoint                                                             #
# --------------------------------------------------------------------------- #


class _HealthHandler(BaseHTTPRequestHandler):
    def do_GET(self):  # noqa: N802 - required by BaseHTTPRequestHandler
        if self.path not in ("/healthz", "/health", "/"):
            self.send_response(404)
            self.end_headers()
            return
        healthy = _state["consumerConnected"]
        body = json.dumps({"status": "ok" if healthy else "starting", **_state}).encode()
        self.send_response(200 if healthy else 503)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *args):
        """Silence per-request logging — the kubelet probes every 10s."""
        return


def start_health_server(port: int = LISTEN_PORT) -> HTTPServer:
    server = HTTPServer(("0.0.0.0", port), _HealthHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    logger.info("health endpoint listening on :%d/healthz", port)
    return server


# --------------------------------------------------------------------------- #
# Consumer                                                                    #
# --------------------------------------------------------------------------- #


def build_consumer() -> KafkaConsumer:
    kwargs: Dict[str, Any] = {
        "bootstrap_servers": [
            s.strip() for s in BOOTSTRAP_SERVERS.split(",") if s.strip()
        ],
        "group_id": GROUP_ID,
        "client_id": f"oda-adapter-{GROUP_ID}",
        "security_protocol": SECURITY_PROTOCOL,
        # Manual commit is what makes at-least-once possible.
        "enable_auto_commit": False,
        # A new subscriber starts at the earliest retained offset so events
        # published before the adapter came up are not silently skipped.
        "auto_offset_reset": "earliest",
        "value_deserializer": lambda v: v,
        "consumer_timeout_ms": 1000,
    }
    if SASL_MECHANISM:
        kwargs["sasl_mechanism"] = SASL_MECHANISM
        kwargs["sasl_plain_username"] = SASL_USERNAME
        kwargs["sasl_plain_password"] = SASL_PASSWORD
    if "SSL" in SECURITY_PROTOCOL:
        kwargs["ssl_context"] = ssl.create_default_context(cafile=SSL_CAFILE)
    return KafkaConsumer(TOPIC, **kwargs)


def decode(raw: bytes) -> Dict[str, Any]:
    """Decode a Kafka message value into an event dict.

    Non-JSON payloads are wrapped rather than dropped — a component publishing
    plain text is misconfigured, but silently discarding its events would be
    worse than forwarding them for the listener to reject.
    """
    try:
        decoded = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        return {"event": {"raw": raw.decode("utf-8", errors="replace")}}
    if isinstance(decoded, dict):
        return decoded
    return {"event": decoded}


def validate_config() -> None:
    missing = [
        name
        for name, value in (
            ("KAFKA_BOOTSTRAP_SERVERS", BOOTSTRAP_SERVERS),
            ("KAFKA_TOPIC", TOPIC),
            ("KAFKA_GROUP_ID", GROUP_ID),
            ("CALLBACK_URL", CALLBACK_URL),
        )
        if not value
    ]
    if missing:
        raise SystemExit(f"missing required environment variables: {', '.join(missing)}")


def _handle_sigterm(signum, frame):  # noqa: ARG001
    logger.info("received signal %s - draining", signum)
    _running.clear()


def run() -> None:
    validate_config()
    criteria = parse_filter(EVENT_FILTER_QUERY)
    logger.info(
        "starting: topic=%s group=%s callback=%s filter=%s",
        TOPIC,
        GROUP_ID,
        CALLBACK_URL,
        criteria or "none",
    )

    signal.signal(signal.SIGTERM, _handle_sigterm)
    signal.signal(signal.SIGINT, _handle_sigterm)
    start_health_server()

    session = requests.Session()
    consumer: Optional[KafkaConsumer] = None

    while _running.is_set():
        try:
            if consumer is None:
                consumer = build_consumer()
                _state["consumerConnected"] = True
                _state["lastError"] = None
                logger.info("subscribed to %s as %s", TOPIC, GROUP_ID)

            for message in consumer:
                if not _running.is_set():
                    break
                event = decode(message.value)
                forwarded_headers = extract_headers(message.headers)

                if not event_matches(event, criteria):
                    _state["eventsFiltered"] += 1
                    consumer.commit()
                    continue

                try:
                    status = deliver(session, CALLBACK_URL, event, forwarded_headers)
                except PoisonMessage as exc:
                    # Committed deliberately: a malformed event must not block
                    # the partition for every later event.
                    _state["eventsFailed"] += 1
                    _state["lastError"] = str(exc)
                    logger.error("dropping poison event at offset %s: %s", message.offset, exc)
                    consumer.commit()
                    continue
                except requests.RequestException as exc:
                    # Not committed: the event is replayed after reconnect.
                    _state["eventsFailed"] += 1
                    _state["lastError"] = str(exc)
                    logger.error(
                        "callback unavailable, offset %s will be retried: %s",
                        message.offset,
                        exc,
                    )
                    time.sleep(5)
                    continue

                consumer.commit()
                _state["eventsDelivered"] += 1
                _state["lastDeliveryEpoch"] = time.time()
                logger.debug(
                    "delivered offset=%s status=%s", message.offset, status
                )

        except KafkaError as exc:
            _state["consumerConnected"] = False
            _state["lastError"] = str(exc)
            logger.error("kafka error, reconnecting in 10s: %s", exc)
            try:
                if consumer is not None:
                    consumer.close()
            except Exception:  # noqa: BLE001 - close must not mask the real error
                logger.warning("error closing consumer during reconnect")
            consumer = None
            time.sleep(10)

    if consumer is not None:
        try:
            consumer.close()
        except Exception:  # noqa: BLE001
            logger.warning("error closing consumer on shutdown")
    logger.info(
        "stopped: delivered=%s filtered=%s failed=%s",
        _state["eventsDelivered"],
        _state["eventsFiltered"],
        _state["eventsFailed"],
    )


if __name__ == "__main__":
    sys.exit(run())