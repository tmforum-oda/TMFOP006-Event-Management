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
  Infrastructure (Kafka bootstrap, libs path, security protocol)
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


def load_config(config_path: str) -> dict:
    path = Path(config_path)
    if not path.exists():
        logger.error("Config file not found: %s", config_path)
        sys.exit(1)
    with open(path) as f:
        return yaml.safe_load(f)


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
    logger.info("  Supported actions: %s", sorted(allowed_actions))

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
        payload = request.get_json()
        logger.info("  Payload:\n%s", json.dumps(payload, indent=4))

        # Common required fields — list from config.yaml validation.common_required
        missing = [f for f in common_req if f not in payload]
        if missing:
            logger.warning("  Missing common fields: %s", missing)
            return jsonify({"error": "missing_fields", "missing_fields": missing}), 400

        # Action inference: new payload uses @type field, not "action" key.
        # Backward-compatible: if explicit "action" field present, use it.
        _type_to_action = config.get("type_action_map", {
            "Alarm":      "create",
            "ClearAlarm": "clear",
        })
        if "action" in payload:
            action = payload["action"].strip().lower()
            logger.info("  action (explicit): %s", action)
        else:
            raw_type = payload.get("@type", "")
            action   = _type_to_action.get(raw_type, "")
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

        # ── STAGE 5 | Transform ───────────────────────────────────────────────
        logger.info("  STAGE 5 | Transforming to CloudEvent (action=%s)", action)
        cloud_event = _TRANSFORMERS[action](payload, config)
        store.record(action, payload.get("correlation_id", ""), cloud_event)

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
        if ok:
            logger.info("  STAGE 7 | Kafka publish SUCCESS")
        else:
            logger.warning("  STAGE 7 | Kafka publish FAILED — HTTP response still returned")
        logger.info("=" * 60)

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
