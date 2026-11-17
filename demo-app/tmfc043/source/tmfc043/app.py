"""TMFC043 fault management component: consumes asyncapi events, republishes them as A2A.

Routes (all topics come from the environment; the defaults are the create and clear
flows in the delivery diagrams):

  oda.trapReceiver.event.alarmDetected                        -> oda.tmf642.command.createAlarm
  oda.trapReceiver.event.alarmCleared                         -> oda.tmf642.command.clearAlarm
  oda.alarmManagement.serviceProblemCreateNotificationEvent   -> oda.tmf656.serviceProblemCreateNotificationEvent
  oda.alarmManagement.serviceProblemStateChangeNotificationEvent
                                                              -> oda.tmf656.serviceProblemStateChangeNotificationEvent

Each message is handled on its own: a bad message is logged and skipped, never retried
forever, and a failed publish stops the offset commit so the message is redelivered.
"""

from __future__ import annotations

import atexit
import json
import logging
import os
import threading
from typing import Callable, Dict, List

from flask import Flask
from kafka import KafkaConsumer, KafkaProducer

import api
import bridge
import kafka_conf
import telemetry

logging.basicConfig(level=int(os.environ.get("LOGGING", "20")), format="%(asctime)s %(levelname)s [tmfc043] %(message)s")
log = logging.getLogger("tmfc043")

GROUP_PREFIX = os.environ.get("CONSUMER_GROUP_PREFIX", "oda-tmfc043-tmfc043-")

# (consumer group suffix, inbound topic, outbound topic, transform)
ROUTES = [
    ("alarmcreateresourceevent", "TOPIC_IN_ALARM_DETECTED", "oda.trapReceiver.event.alarmDetected",
     "TOPIC_OUT_CREATE_ALARM", "oda.tmf642.command.createAlarm", bridge.alarm_to_a2a),
    ("alarmclearresourceevent", "TOPIC_IN_ALARM_CLEARED", "oda.trapReceiver.event.alarmCleared",
     "TOPIC_OUT_CLEAR_ALARM", "oda.tmf642.command.clearAlarm", bridge.alarm_to_a2a),
    ("serviceproblemcreateresourceevent", "TOPIC_IN_PROBLEM_CREATE", "oda.alarmManagement.serviceProblemCreateNotificationEvent",
     "TOPIC_OUT_PROBLEM_CREATE", "oda.tmf656.serviceProblemCreateNotificationEvent", bridge.problem_to_a2a),
    ("serviceproblemstatechangeresourceevent", "TOPIC_IN_PROBLEM_STATE", "oda.alarmManagement.serviceProblemStateChangeNotificationEvent",
     "TOPIC_OUT_PROBLEM_STATE", "oda.tmf656.serviceProblemStateChangeNotificationEvent", bridge.problem_to_a2a),
]

app = Flask(__name__)
app.register_blueprint(api.bp)
_state: Dict[str, int] = {"forwarded": 0, "skipped": 0}
_producer: KafkaProducer | None = None
_producer_lock = threading.Lock()


def _get_producer() -> KafkaProducer:
    global _producer
    with _producer_lock:
        if _producer is None:
            _producer = KafkaProducer(
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                key_serializer=lambda k: k.encode("utf-8"),
                acks="all",
                **kafka_conf.client_kwargs(),
            )
        return _producer


def forward(message: dict, out_topic: str, transform: Callable[[dict], dict]) -> bool:
    """Transform and publish one message. Returns False when the message is unusable."""
    try:
        out = transform(message)
    except ValueError as exc:
        _state["skipped"] += 1
        log.warning("skipped message id=%s: %s", message.get("id") if isinstance(message, dict) else None, exc)
        return False
    # Publish synchronously: if this raises, the caller does not commit the offset.
    _get_producer().send(out_topic, key=out["correlationid"], value=out).get(timeout=15)
    _state["forwarded"] += 1
    log.info("forwarded correlationid=%s taskid=%s -> %s", out["correlationid"], out["taskid"], out_topic)
    return True


def run_route(group_suffix: str, in_topic: str, out_topic: str, transform: Callable[[dict], dict]) -> None:
    consumer = KafkaConsumer(
        in_topic,
        group_id=GROUP_PREFIX + group_suffix,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        auto_offset_reset="latest",
        enable_auto_commit=False,
        **kafka_conf.client_kwargs(),
    )
    log.info("route started: %s -> %s (group %s)", in_topic, out_topic, GROUP_PREFIX + group_suffix)
    for record in consumer:
        with telemetry.input_record(record, GROUP_PREFIX + group_suffix) as span:
            try:
                forwarded = forward(record.value, out_topic, transform)
                # Set the outcome before the commit, so a commit failure is reported as an error.
                telemetry.mark(span, "forwarded" if forwarded else "skipped", None if forwarded else "InvalidInput")
                consumer.commit()
            except Exception as exc:  # noqa: BLE001 - keep the loop alive; the uncommitted message is redelivered
                telemetry.mark(span, "error", type(exc).__name__)
                log.exception("route %s failed, will retry the message", in_topic)


def routes_from_env(env=os.environ) -> List[tuple]:
    return [(g, env.get(in_var, in_default), env.get(out_var, out_default), fn)
            for g, in_var, in_default, out_var, out_default, fn in ROUTES]


@app.get("/healthz")
def healthz():
    return {"status": "ok", **_state}


def main() -> None:
    telemetry.configure()
    atexit.register(telemetry.shutdown)
    for group, tin, tout, fn in routes_from_env():
        threading.Thread(target=run_route, args=(group, tin, tout, fn), daemon=True).start()
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", "8080")))


if __name__ == "__main__":
    main()
