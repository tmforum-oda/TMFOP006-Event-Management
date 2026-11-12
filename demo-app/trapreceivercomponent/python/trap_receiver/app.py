"""
=============================================================
Trap Receiver - Unified Flask REST API  (Kafka-enabled)
=============================================================
NO hardcoded values.

Config layers (12-factor / industry standard):
  Application behaviour (validation rules, action map, Kafka topics)
      -> config.yaml loaded from trap-receiver-config ConfigMap
  Secrets (API keys)
      -> trap-receiver-auth Secret (envFrom in Deployment)
  Infrastructure (Kafka bootstrap, security protocol)
      -> kafka-config ConfigMap (envFrom in Deployment)

To add a new alarm action (e.g. acknowledge):
  1. Add entry in config.yaml action_map  (channel_address, message_name, ...)
  2. Add build_acknowledge_cloud_event()  in transformer.py
  Nothing else changes — no URL, no Python constant, no Deployment edit.
=============================================================
"""
import json
import logging
import os
import sys
from pathlib import Path

import yaml
from flask import Flask, request, jsonify

from .transformer import build_create_cloud_event, build_clear_cloud_event
from .kafka_publisher import publish as kafka_publish
from . import store
from .dashboard import dashboard_bp
from .auth import check_auth

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("TrapReceiver")

# Transformer registry: maps action name -> transformer function.
# Only actions that appear in BOTH this dict AND config.yaml action_map are
# accepted at runtime — config.yaml is the gating authority.
_TRANSFORMERS = {
    "create": build_create_cloud_event,
    "clear":  build_clear_cloud_event,
    # "acknowledge": build_acknowledge_cloud_event,   # register here when ready
}

# Required config.yaml keys per action_map entry — see _validate_config().
_ACTION_MAP_REQUIRED_KEYS = (
    "message_name", "channel_name", "operation_name",
    "source_type", "asyncapi_title", "channel_address",
)
_CLOUDEVENT_REQUIRED_KEYS = (
    "spec_version", "data_content_type", "payload_category",
    "id_template", "source_template", "type_template", "data_schema_template",
)
_KAFKA_PRODUCER_REQUIRED_KEYS = (
    "acks", "retries", "linger_ms", "request_timeout_ms", "max_block_ms",
)


def load_config(config_path: str) -> dict:
    path = Path(config_path)
    if not path.exists():
        logger.error("Config file not found: %s", config_path)
        sys.exit(1)
    with open(path) as f:
        return yaml.safe_load(f)


def _fail(msg: str, *args):
    """Log a startup config error and exit. Config must be the single
    source of truth — a missing key is a deployment error, not something to
    silently paper over with a Python-literal default."""
    logger.error(msg, *args)
    sys.exit(1)


def _validate_config(config: dict, allowed_actions: set) -> None:
    """Fail fast at startup if config.yaml is missing anything the app
    needs, instead of individual modules falling back to hardcoded
    defaults at request time."""
    if not config.get("type_action_map"):
        _fail("config.yaml: type_action_map is missing or empty")

    action_map = config.get("action_map", {})
    for action in sorted(allowed_actions):
        entry = action_map.get(action, {})
        missing = [k for k in _ACTION_MAP_REQUIRED_KEYS if not entry.get(k)]
        if missing:
            _fail("config.yaml: action_map.%s missing keys: %s", action, missing)

    ce_cfg = config.get("cloudevent", {})
    missing = [k for k in _CLOUDEVENT_REQUIRED_KEYS if not ce_cfg.get(k)]
    if missing:
        _fail("config.yaml: cloudevent missing keys: %s", missing)

    ac_cfg = config.get("asyncapi", {})
    if not ac_cfg.get("version") or not ac_cfg.get("info_version"):
        _fail("config.yaml: asyncapi.version / asyncapi.info_version missing")

    prod_cfg = config.get("kafka", {}).get("producer", {})
    missing = [k for k in _KAFKA_PRODUCER_REQUIRED_KEYS if k not in prod_cfg]
    if missing:
        _fail("config.yaml: kafka.producer missing keys: %s", missing)

    if not config.get("kafka", {}).get("cloudevent_headers"):
        _fail("config.yaml: kafka.cloudevent_headers is missing or empty")

    if "create" in allowed_actions and not config.get("severity_map"):
        _fail("config.yaml: severity_map is missing or empty (required for create)")

    auth_cfg = config.get("auth", {})
    if not auth_cfg.get("trap_paths") or not auth_cfg.get("dashboard_paths"):
        _fail("config.yaml: auth.trap_paths / auth.dashboard_paths missing or empty")


def create_app(config: dict) -> Flask:
    app = Flask(__name__)
    app.json.sort_keys = False
    app.config["TRAP_CONFIG"] = config
    app.register_blueprint(dashboard_bp)
    app.before_request(check_auth)

    # ── Validation rules from config.yaml — no Python constants ──────────────
    val_cfg        = config.get("validation", {})
    common_req     = val_cfg.get("common_required", [])
    action_req_map = val_cfg.get("action_required", {})

    # Allowed actions = intersection of config.yaml action_map and _TRANSFORMERS.
    # config.yaml is the authority; a transformer with no config entry is ignored.
    cfg_actions     = set(config.get("action_map", {}).keys())
    allowed_actions = cfg_actions & set(_TRANSFORMERS.keys())
    if not allowed_actions:
        logger.error(
            "No overlap between config.yaml action_map %s and known transformers %s — exiting",
            sorted(cfg_actions), sorted(_TRANSFORMERS.keys()),
        )
        sys.exit(1)

    _validate_config(config, allowed_actions)
    logger.info("  Supported actions: %s", sorted(allowed_actions))

    severity_map = config.get("severity_map", {})

    @app.route("/api/v1/health", methods=["GET"])
    def health():
        return jsonify({"status": "UP", "service": "trap-receiver"}), 200

    @app.route("/api/v1/trap", methods=["POST"])
    def receive_trap():
        if not request.is_json:
            return jsonify({
                "error":   "invalid_content_type",
                "message": "Content-Type must be application/json",
            }), 415

        logger.info("=" * 60)
        logger.info("  STAGE 4 | receive_trap()  [POST /api/v1/trap]")
        logger.info("=" * 60)
        try:
            payload = request.get_json()
        except Exception:
            return jsonify({"error": "invalid_json", "message": "Body is not valid JSON"}), 400

        if not isinstance(payload, dict):
            return jsonify({
                "error":   "invalid_body",
                "message": "Request body must be a JSON object",
            }), 400
        logger.info("  Payload:\n%s", json.dumps(payload, indent=4))

        # Common required fields — list from config.yaml validation.common_required
        missing = [f for f in common_req if f not in payload]
        if missing:
            logger.warning("  Missing common fields: %s", missing)
            return jsonify({"error": "missing_fields", "missing_fields": missing}), 400

        if not isinstance(payload.get("correlation_id"), str) or not payload["correlation_id"]:
            return jsonify({
                "error": "invalid_field_type",
                "field": "correlation_id",
                "message": "correlation_id must be a non-empty string",
            }), 400

        # Action inference: new payload uses @type field, not "action" key.
        # Backward-compatible: if explicit "action" field present, use it.
        _type_to_action = config["type_action_map"]
        if "action" in payload:
            if not isinstance(payload["action"], str):
                return jsonify({
                    "error": "invalid_field_type", "field": "action",
                    "message": "action must be a string",
                }), 400
            action = payload["action"].strip().lower()
            logger.info("  action (explicit): %s", action)
        else:
            raw_type = payload.get("@type", "")
            if not isinstance(raw_type, str):
                return jsonify({
                    "error": "invalid_field_type", "field": "@type",
                    "message": "@type must be a string",
                }), 400
            action = _type_to_action.get(raw_type, "")
            logger.info("  action (inferred from @type=%r): %s", raw_type, action)

        # Allowed actions gated by config.yaml action_map (not a Python constant)
        if action not in allowed_actions:
            logger.warning("  Unknown action: %r", action)
            return jsonify({
                "error":           "invalid_action",
                "received_action": action,
                "allowed_actions": sorted(allowed_actions),
            }), 400

        # Per-action required fields from config.yaml validation.action_required
        extra_req = action_req_map.get(action, [])
        missing2  = [f for f in extra_req if f not in payload]
        if missing2:
            logger.warning("  Missing fields for action %r: %s", action, missing2)
            return jsonify({
                "error":          "missing_fields",
                "action":         action,
                "missing_fields": missing2,
            }), 400

        # Type checks on fields the transformer treats as structured data —
        # a wrong type here would otherwise surface as a 500 inside the
        # transformer instead of a clean 400 here.
        if action == "create":
            if not isinstance(payload.get("alarmedObject"), dict):
                return jsonify({
                    "error": "invalid_field_type", "field": "alarmedObject",
                    "message": "alarmedObject must be a JSON object",
                }), 400
            if payload.get("severity") not in severity_map:
                return jsonify({
                    "error":            "invalid_severity",
                    "received":         payload.get("severity"),
                    "allowed_severity": sorted(severity_map.keys()),
                }), 400
            if "alarmDetails" in payload and not isinstance(payload["alarmDetails"], str):
                return jsonify({
                    "error": "invalid_field_type", "field": "alarmDetails",
                    "message": "alarmDetails must be a string",
                }), 400
        elif action == "clear":
            alarm_pattern = payload.get("alarmPattern")
            if not isinstance(alarm_pattern, list) or not alarm_pattern:
                return jsonify({
                    "error": "invalid_field_type", "field": "alarmPattern",
                    "message": "alarmPattern must be a non-empty array",
                }), 400

        # ── STAGE 5 | Transform ───────────────────────────────────────────────
        logger.info("  STAGE 5 | Transforming to CloudEvent (action=%s)", action)
        cloud_event = _TRANSFORMERS[action](payload, config)

        # ── STAGE 6 | Log CloudEvent ──────────────────────────────────────────
        logger.info("=" * 60)
        logger.info("  STAGE 6 | CloudEvent built")
        logger.info("  id=%s  type=%s", cloud_event.get("id"), cloud_event.get("type"))
        logger.info("  CloudEvent:\n%s", json.dumps(cloud_event, indent=4))
        logger.info("=" * 60)

        # ── STAGE 7 | Publish to Kafka ────────────────────────────────────────
        # Topic resolved inside kafka_publisher via config.yaml action_map.channel_address
        # No topic string appears here — config.yaml is the single source of truth
        logger.info("  STAGE 7 | Publishing CloudEvent to Kafka")
        ok = kafka_publish(cloud_event, config)
        if not ok:
            logger.error("  STAGE 7 | Kafka publish FAILED — alarm NOT delivered, returning 502")
            return jsonify({
                "error":      "kafka_publish_failed",
                "message":    "Alarm was validated but could not be published to Kafka",
                "cloudEvent": cloud_event,
            }), 502
        logger.info("  STAGE 7 | Kafka publish SUCCESS")
        logger.info("=" * 60)

        # Recorded (for the dashboard) only once the publish actually succeeded.
        store.record(action, payload["correlation_id"], cloud_event)

        return jsonify(cloud_event), 200

    # Backward-compat aliases — old Zabbix webhook URLs still work
    @app.route("/api/v1/trap/alarm", methods=["POST"])
    def receive_alarm_compat():
        logger.info("  [compat] /api/v1/trap/alarm -> /api/v1/trap")
        return receive_trap()

    @app.route("/api/v1/trap/clear", methods=["POST"])
    def receive_clear_compat():
        logger.info("  [compat] /api/v1/trap/clear -> /api/v1/trap")
        return receive_trap()

    return app


def main():
    # All config from env vars / mounted ConfigMaps — no hardcoded paths or addresses
    config_path = os.environ.get("CONFIG_PATH", "/app/config/config.yaml")
    port        = int(os.environ.get("PORT", 8080))
    host        = os.environ.get("HOST", "0.0.0.0")
    config      = load_config(config_path)
    app         = create_app(config)

    # KAFKA_BOOTSTRAP_SERVERS is set by kafka-config ConfigMap envFrom
    bootstrap = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "(not set — check kafka-config CM)")
    logger.info("=" * 60)
    logger.info("  TRAP RECEIVER — TMF642 Unified Alarm API + Kafka")
    logger.info("  Listen  : %s:%s", host, port)
    logger.info("  Config  : %s", config_path)
    logger.info("  Kafka   : %s", bootstrap)
    logger.info("=" * 60)
    app.run(host=host, port=port, debug=False)


if __name__ == "__main__":
    main()
