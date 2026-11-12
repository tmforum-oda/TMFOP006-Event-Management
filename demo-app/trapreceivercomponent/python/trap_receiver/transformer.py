"""
=============================================================
Trap Receiver - TMF642 CloudEvent Transformer
=============================================================
Input payload is a pre-structured TMF642 alarm object produced
by the zabbix-alarm-sender Kafka consumer.  All TMF642-required
alarm fields are REQUIRED in the payload (see app.py's
validation.action_required and the per-action type checks) --
this module does not invent values for them. config.yaml
provides only structural defaults (e.g. the @type discriminator,
the alarmDetails prefix) and envelope/AsyncAPI templates.

CREATE input: correlation_id, @type=Alarm, externalAlarmId, state,
    alarmType, severity, probableCause, alarmedObjectType,
    alarmedObject, sourceSystemId, alarmRaisedTime, alarmDetails

CLEAR input: correlation_id, recovery_event_id, @type=ClearAlarm,
    alarmClearedTime, alarmPattern, clearSystemId, clearUserId
=============================================================
"""

import logging

logger = logging.getLogger("TrapReceiver.Transformer")


def _ce_base(payload: dict, cfg: dict, action: str, timestamp: str) -> dict:
    """Build CloudEvent envelope.  timestamp supplied by caller.

    Parameters
    ----------
    payload   : raw request body dict
    cfg       : loaded config.yaml dict
    action    : inferred action string ("create" | "clear")
    timestamp : ISO-8601 event time (alarmRaisedTime or alarmClearedTime)
    """
    cid    = payload["correlation_id"]
    ce_cfg = cfg["cloudevent"]

    action_entry = cfg["action_map"][action]
    message_name = action_entry["message_name"]
    schema_tpl   = ce_cfg["data_schema_template"]
    data_schema  = (schema_tpl.replace("{action}", action)
                               .replace("{MessageName}", message_name))

    source_type = action_entry["source_type"]
    source      = (ce_cfg["source_template"].replace("{source_type}", source_type)
                                             .replace("{cid}", cid))

    ce_type = ce_cfg["type_template"].replace("{action}", action)
    ce_id   = ce_cfg["id_template"].replace("{cid}", cid).replace("{action}", action)

    envelope = {
        "specversion":     ce_cfg["spec_version"],
        "id":              ce_id,
        "source":          source,
        "type":            ce_type,
        "time":            timestamp,
        "datacontenttype": ce_cfg["data_content_type"],
        "dataschema":      data_schema,
        "payloadcategory": ce_cfg["payload_category"],
        "correlationid":   cid,
    }
    for field in ("user_id", "conversation_id", "conversation_name"):
        if field in payload:
            envelope[field] = payload[field]
    return envelope


def _asyncapi_wrapper(alarm_const: dict, cfg: dict, action: str) -> dict:
    """Build AsyncAPI 3.x data block from config.  No hardcoding."""
    ac_cfg        = cfg["asyncapi"]
    action_entry  = cfg["action_map"][action]
    message_name  = action_entry["message_name"]
    channel_name  = action_entry["channel_name"]
    op_name       = action_entry["operation_name"]
    title         = action_entry["asyncapi_title"]
    address       = action_entry["channel_address"]
    return {
        "asyncapi": ac_cfg["version"],
        "info":     {"title": title, "version": ac_cfg["info_version"]},
        "channels": {
            channel_name: {
                "address":  address,
                "messages": {
                    channel_name: {"$ref": f"#/components/messages/{message_name}"}
                },
            }
        },
        "operations": {
            op_name: {
                "action":   "send",
                "channel":  {"$ref": f"#/channels/{channel_name}"},
                "messages": [{"$ref": f"#/channels/{channel_name}/messages/{channel_name}"}],
            }
        },
        "components": {
            "messages": {
                message_name: {
                    "name":        message_name,
                    "contentType": "application/json",
                    "payload":     {"const": alarm_const},
                }
            }
        },
    }


# ---------------------------------------------------------------------------
# CREATE transformer
# ---------------------------------------------------------------------------

def build_create_cloud_event(payload: dict, cfg: dict) -> dict:
    """TMF642 pre-structured Alarm payload -> CloudEvent with AsyncAPI wrapper.

    app.py has already validated: correlation_id, externalAlarmId,
    alarmRaisedTime, alarmType, probableCause, alarmedObjectType,
    alarmedObject (dict), sourceSystemId, state, severity (a severity_map
    key) are all present with the right types. Nothing here is invented.
    """
    logger.info("━" * 60)
    logger.info("  STAGE 5 │ transformer › build_create_cloud_event()")
    logger.info("  ACTION  │ TMF642 Alarm (pre-structured) → CloudEvent")
    logger.info("━" * 60)

    action = "create"
    cid    = payload["correlation_id"]
    defaults = cfg.get("alarm_defaults", {})

    # Timestamp: prefer alarmRaisedTime (canonical TMF642 field, required)
    timestamp = payload["alarmRaisedTime"]

    logger.info("  ✔ correlation_id  : %s", cid)
    logger.info("  ✔ alarmRaisedTime : %s", timestamp)

    perceived_severity = cfg["severity_map"][payload["severity"]]

    # alarmDetails: add prefix only if raw value not already prefixed
    # (alarm_details_prefix is a formatting default, not fabricated alarm
    # data -- the detail text itself always comes from the payload).
    details_prefix    = defaults.get("alarm_details_prefix", "")
    alarm_details_raw = payload.get("alarmDetails", "")
    if alarm_details_raw and details_prefix and not alarm_details_raw.startswith(details_prefix):
        alarm_details = f"{details_prefix}: {alarm_details_raw}"
    else:
        alarm_details = alarm_details_raw

    alarm_const = {
        # @type discriminator default: real traffic always has this set
        # already, since it's how app.py infers action in the first place.
        "@type":             payload.get("@type", defaults.get("object_type", "Alarm")),
        "externalAlarmId":   payload["externalAlarmId"],
        "state":             payload["state"],
        "alarmType":         payload["alarmType"],
        "perceivedSeverity": perceived_severity,
        "probableCause":     payload["probableCause"],
        "specificProblem":   payload.get("specificProblem", ""),
        "alarmedObjectType": payload["alarmedObjectType"],
        "alarmedObject":     payload["alarmedObject"],
        "sourceSystemId":    payload["sourceSystemId"],
        "alarmRaisedTime":   timestamp,
        "alarmDetails":      alarm_details,
        "correlationId":     cid,
    }

    logger.info("  ✔ externalAlarmId   : %s", alarm_const["externalAlarmId"])
    logger.info("  ✔ alarmType         : %s", alarm_const["alarmType"])
    logger.info("  ✔ perceivedSeverity : %s", alarm_const["perceivedSeverity"])
    logger.info("  ✔ specificProblem   : %s", alarm_const["specificProblem"])

    asyncapi_block = _asyncapi_wrapper(alarm_const, cfg, action)

    envelope = _ce_base(payload, cfg, action, timestamp)
    envelope["data"] = asyncapi_block
    logger.info("  ✔ CE id=%s  type=%s", envelope["id"], envelope["type"])
    logger.info("  ✔ AlarmCreate conversion complete")
    return envelope


# ---------------------------------------------------------------------------
# CLEAR transformer
# ---------------------------------------------------------------------------

def build_clear_cloud_event(payload: dict, cfg: dict) -> dict:
    """TMF642 pre-structured ClearAlarm payload -> CloudEvent with AsyncAPI wrapper.

    app.py has already validated: correlation_id, recovery_event_id,
    alarmClearedTime, alarmPattern (non-empty list), clearSystemId,
    clearUserId are all present. Nothing here is invented.
    """
    logger.info("━" * 60)
    logger.info("  STAGE 5 │ transformer › build_clear_cloud_event()")
    logger.info("  ACTION  │ TMF642 ClearAlarm (pre-structured) → CloudEvent")
    logger.info("━" * 60)

    action = "clear"
    cid    = payload["correlation_id"]
    rid    = payload["recovery_event_id"]

    # Timestamp: alarmClearedTime (canonical TMF642 clear field, required)
    timestamp = payload["alarmClearedTime"]

    logger.info("  ✔ correlation_id    : %s", cid)
    logger.info("  ✔ recovery_event_id : %s", rid)
    logger.info("  ✔ alarmClearedTime  : %s", timestamp)

    alarm_pattern = payload["alarmPattern"]

    alarm_const = {
        "@type":            payload.get("@type", "ClearAlarm"),
        "alarmClearedTime": timestamp,
        "alarmPattern":     alarm_pattern,
        "correlationId":    cid,
        "recoveryEventId":  rid,
        "clearSystemId":    payload["clearSystemId"],
        "clearUserId":      payload["clearUserId"],
    }

    logger.info("  ✔ alarmPattern: %s", alarm_pattern)

    asyncapi_block = _asyncapi_wrapper(alarm_const, cfg, action)

    envelope = _ce_base(payload, cfg, action, timestamp)
    envelope["recoveryeventid"] = rid
    envelope["data"] = asyncapi_block
    logger.info("  ✔ CE id=%s  type=%s", envelope["id"], envelope["type"])
    logger.info("  ✔ AlarmClear conversion complete")
    return envelope
