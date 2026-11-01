"""
Trap Receiver - Kafka Publisher
NO hardcoded values.

Config layers (12-factor / industry standard):
  Infrastructure (bootstrap, libs path, security protocol, SASL mechanism)
      -> env vars from kafka-config ConfigMap (envFrom)
  SASL Credentials (username, password)
      -> env vars from trap-receiver-kafka-creds Secret (envFrom)
  Application (topic names, producer tuning, header fields)
      -> cfg dict from config.yaml (trap-receiver-config CM)

Public API: publish(cloud_event, cfg) -> bool, never raises
"""
import json
import logging
import os
import sys

logger = logging.getLogger("TrapReceiver.KafkaPublisher")

# ── Infra config from env vars (kafka-config ConfigMap -> Deployment envFrom) ──
_BOOTSTRAP         = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "")
_KAFKA_LIBS        = os.environ.get("KAFKA_LIBS_PATH", "")
_SECURITY_PROTOCOL = os.environ.get("KAFKA_SECURITY_PROTOCOL", "PLAINTEXT")
_SASL_MECHANISM    = os.environ.get("KAFKA_SASL_MECHANISM", "")

# ── SASL Credentials from Secret (trap-receiver-kafka-creds -> Deployment envFrom) ──
_SASL_USERNAME = os.environ.get("KAFKA_SASL_USERNAME", "")
_SASL_PASSWORD = os.environ.get("KAFKA_SASL_PASSWORD", "")

# Add kafka-python install dir to sys.path (init container populates this)
if _KAFKA_LIBS and _KAFKA_LIBS not in sys.path:
    sys.path.insert(0, _KAFKA_LIBS)

_producer = None  # lazy singleton


def _build_sasl_config() -> dict:
    """Return SASL kwargs for KafkaProducer based on env vars.

    Supports three security modes:
      PLAINTEXT      -> no SASL kwargs added
      SASL_PLAINTEXT -> SCRAM-SHA-512 over plain TCP (port 9092)
      SASL_SSL       -> SCRAM-SHA-512 over TLS (port 9093) -- future use

    All values come from env vars injected by ConfigMap + Secret.
    Nothing is hardcoded.
    """
    if _SECURITY_PROTOCOL not in ("SASL_PLAINTEXT", "SASL_SSL"):
        return {}  # PLAINTEXT — no SASL needed

    if not _SASL_USERNAME or not _SASL_PASSWORD:
        logger.error(
            "  X SASL protocol=%r but KAFKA_SASL_USERNAME or KAFKA_SASL_PASSWORD "
            "is not set -- add trap-receiver-kafka-creds Secret to Deployment envFrom",
            _SECURITY_PROTOCOL,
        )
        return None  # None signals: cannot build producer

    if not _SASL_MECHANISM:
        logger.error(
            "  X KAFKA_SASL_MECHANISM is not set -- "
            "add KAFKA_SASL_MECHANISM=SCRAM-SHA-512 to kafka-config ConfigMap"
        )
        return None

    logger.info(
        "  OK SASL config: mechanism=%s  username=%s",
        _SASL_MECHANISM, _SASL_USERNAME,
    )
    return {
        "sasl_mechanism":      _SASL_MECHANISM,   # SCRAM-SHA-512
        "sasl_plain_username": _SASL_USERNAME,     # trap-receiver-producer
        "sasl_plain_password": _SASL_PASSWORD,     # from K8s Secret
    }


def _get_producer(prod_cfg: dict):
    """Create or return the KafkaProducer singleton.
    All tuning from cfg.kafka.producer (config.yaml).
    SASL credentials from env vars (Secret). Nothing hardcoded.
    """
    global _producer
    if _producer is not None:
        return _producer

    if not _BOOTSTRAP:
        logger.error(
            "  X KAFKA_BOOTSTRAP_SERVERS is not set -- "
            "add kafka-config ConfigMap to Deployment envFrom"
        )
        return None

    try:
        from kafka import KafkaProducer
    except ImportError:
        logger.error(
            "  X kafka-python not importable (KAFKA_LIBS_PATH=%r) -- "
            "check install-kafka-python init container completed", _KAFKA_LIBS
        )
        return None

    sasl_kwargs = _build_sasl_config()
    if sasl_kwargs is None:
        return None  # SASL misconfigured — logged inside _build_sasl_config

    try:
        _producer = KafkaProducer(
            bootstrap_servers=_BOOTSTRAP,
            security_protocol=_SECURITY_PROTOCOL,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            key_serializer=lambda k: k.encode("utf-8") if k else None,
            acks=prod_cfg.get("acks", "all"),
            retries=int(prod_cfg.get("retries", 3)),
            linger_ms=int(prod_cfg.get("linger_ms", 5)),
            request_timeout_ms=int(prod_cfg.get("request_timeout_ms", 10000)),
            max_block_ms=int(prod_cfg.get("max_block_ms", 10000)),
            **sasl_kwargs,    # empty dict for PLAINTEXT; SASL kwargs otherwise
        )
        logger.info(
            "  OK KafkaProducer ready  bootstrap=%s  protocol=%s  mechanism=%s  acks=%s",
            _BOOTSTRAP, _SECURITY_PROTOCOL,
            _SASL_MECHANISM if _SASL_MECHANISM else "none",
            prod_cfg.get("acks", "all"),
        )
        return _producer
    except Exception as exc:
        logger.error("  X KafkaProducer init failed: %s", exc)
        return None


def _resolve_topic(cloud_event: dict, action_map: dict) -> str:
    """Derive Kafka topic from CloudEvent type via config.yaml action_map.

    Example:
        CE type  'org.tmforum.tmf642.alarm.create'
        -> action suffix  'create'
        -> action_map['create']['channel_address']
        -> 'oda.tmf642-refImpl-alarmCreateResource'

    Zero hardcoded topic strings. Add a new action in config.yaml only.
    """
    ce_type = cloud_event.get("type", "")
    action  = ce_type.rsplit(".", 1)[-1] if "." in ce_type else ""
    topic   = action_map.get(action, {}).get("channel_address", "")
    if not topic:
        logger.warning(
            "  W No channel_address for action=%r (CE type=%r) -- "
            "add entry to config.yaml action_map", action, ce_type
        )
    return topic


def _build_headers(cloud_event: dict, header_fields: list) -> list:
    """Build Kafka record headers from CloudEvent fields listed in
    cfg.kafka.cloudevent_headers (config.yaml). Visible in Kafdrop.
    """
    return [
        ("ce_" + f, str(cloud_event[f]).encode("utf-8"))
        for f in header_fields
        if cloud_event.get(f) is not None
    ]


def publish(cloud_event: dict, cfg: dict) -> bool:
    """Publish a CloudEvent dict to its mapped Kafka topic.

    Parameters
    ----------
    cloud_event : full CloudEvent dict (becomes the Kafka message value)
    cfg         : loaded config.yaml dict, passed in from app.py

    Returns True on success, False on failure. Never raises.
    HTTP response is always returned to the caller regardless of Kafka state.
    """
    kafka_cfg  = cfg.get("kafka", {})
    prod_cfg   = kafka_cfg.get("producer", {})
    action_map = cfg.get("action_map", {})
    hdr_fields = kafka_cfg.get(
        "cloudevent_headers",
        ["id", "source", "type", "time", "specversion", "datacontenttype", "correlationid"],
    )

    topic = _resolve_topic(cloud_event, action_map)
    if not topic:
        return False

    producer = _get_producer(prod_cfg)
    if producer is None:
        logger.warning("  W Producer unavailable -- NOT sent to topic %r", topic)
        return False

    key     = cloud_event.get("correlationid", cloud_event.get("id", ""))
    headers = _build_headers(cloud_event, hdr_fields)
    timeout = int(prod_cfg.get("request_timeout_ms", 10000)) / 1000

    try:
        meta = producer.send(
            topic, value=cloud_event, key=key, headers=headers
        ).get(timeout=timeout)
        logger.info(
            "  OK Published  topic=%r  partition=%d  offset=%d  key=%r",
            meta.topic, meta.partition, meta.offset, key,
        )
        return True
    except Exception as exc:
        logger.error("  X Send failed topic=%r: %s", topic, exc)
        return False
