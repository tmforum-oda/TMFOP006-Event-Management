"""
=============================================================
Trap Receiver - TMF642 CloudEvent Transformer
=============================================================
Input payload is a pre-structured TMF642 alarm object produced
by the zabbix-alarm-sender Kafka consumer.  All alarm fields
are passed through directly from the payload.  config.yaml
provides envelope templates, AsyncAPI settings, and fallback
defaults only.

CREATE input (oda.tmf642-refImpl-alarmCreateResource):
    correlation_id, @type=Alarm, externalAlarmId, state,
    alarmType, perceivedSeverity, specificProblem, probableCause,
    alarmedObject, sourceSystemId, alarmRaisedTime, alarmDetails

CLEAR input (oda.tmf642-refImpl-alarmClearResource):
    correlation_id, recovery_event_id, @type=ClearAlarm,
    alarmClearedTime, alarmPattern
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
    cid      = payload.get("correlation_id", "unknown")
    ce_cfg   = cfg.get("cloudevent", {})
    spec_ver = ce_cfg.get("spec_version", "1.0")
    dct      = ce_cfg.get("data_content_type", "application/json")
    pcat     = ce_cfg.get("payload_category", "asyncapi")

    action_map   = cfg.get("action_map", {})
    message_name = action_map.get(action, {}).get(
                       "message_name", "Alarm" + action.capitalize())
    schema_tpl   = ce_cfg.get(
                       "data_schema_template",
                       "https://example.com/asyncapi/tmf642-alarm-{action}.json"
                       "#/components/messages/{MessageName}")
    data_schema  = (schema_tpl.replace("{action}", action)
                               .replace("{MessageName}", message_name))

    source_type = action_map.get(action, {}).get("source_type", "problem")
    source_tpl  = ce_cfg.get("source_template", "/zabbix/{source_type}/{cid}")
    source      = (source_tpl.replace("{source_type}", source_type)
                              .replace("{cid}", cid))

    type_tpl = ce_cfg.get("type_template", "org.tmforum.tmf642.alarm.{action}")
    ce_type  = type_tpl.replace("{action}", action)

    id_tpl = ce_cfg.get("id_template", "evt-{cid}-{action}")
    ce_id  = id_tpl.replace("{cid}", cid).replace("{action}", action)

    envelope = {
        "specversion":     spec_ver,
        "id":              ce_id,
        "source":          source,
        "type":            ce_type,
        "time":            timestamp,
        "datacontenttype": dct,
        "dataschema":      data_schema,
        "payloadcategory": pcat,
        "correlationid":   cid,
    }
    for field in ("user_id", "conversation_id", "conversation_name"):
        if field in payload:
            envelope[field] = payload[field]
    return envelope


def _asyncapi_wrapper(message_name: str, channel_name: str, op_name: str,
                      alarm_const: dict, cfg: dict, action: str) -> dict:
    """Build AsyncAPI 3.x data block from config.  No hardcoding."""
    ac_cfg     = cfg.get("asyncapi", {})
    version    = ac_cfg.get("version", "3.1.0")
    api_ver    = ac_cfg.get("info_version", "5.0.1")
    action_map = cfg.get("action_map", {})
    title      = action_map.get(action, {}).get(
                     "asyncapi_title", f"TMF642 Alarm {action.capitalize()}")
    address    = action_map.get(action, {}).get(
                     "channel_address",
                     f"oda.tmf642-refImpl-alarm{action.capitalize()}Resource")
    return {
        "asyncapi": version,
        "info":     {"title": title, "version": api_ver},
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

    The input payload already contains all TMF642 alarm fields produced by
    the zabbix-alarm-sender.  Fields are passed through directly into
    alarm_const (the payload.const block).  config.yaml alarm_defaults fills
    any field the sender omits.
    """
    logger.info("━" * 60)
    logger.info("  STAGE 5 │ transformer › build_create_cloud_event()")
    logger.info("  ACTION  │ TMF642 Alarm (pre-structured) → CloudEvent")
    logger.info("━" * 60)

    action = "create"
    cid    = payload.get("correlation_id", "unknown")

    # Timestamp: prefer alarmRaisedTime (canonical TMF642 field)
    timestamp = (payload.get("alarmRaisedTime")
                 or payload.get("timestamp")
                 or payload.get("timestap", ""))

    logger.info(f"  ✔ correlation_id  : {cid}")
    logger.info(f"  ✔ alarmRaisedTime : {timestamp}")

    # Defaults from config.yaml - used only when payload omits a field
    defaults         = cfg.get("alarm_defaults", {})
    source_system_id = defaults.get("source_system_id", "zabbix")
    alarm_type       = defaults.get("alarm_type", "communicationsAlarm")
    probable_cause   = defaults.get("probable_cause", "lossOfSignal")
    alarmed_obj_type = defaults.get("alarmed_object_type", "Interface")
    state            = defaults.get("state", "raised")
    object_type      = defaults.get("object_type", "Alarm")
    obj_ref_type     = defaults.get("alarmed_obj_ref_type", "AlarmedObjectRef")
    referred_type    = defaults.get("alarmed_referred_type", "Host")
    details_prefix   = defaults.get("alarm_details_prefix", "Operational data")

    # alarmedObject: pass through entire sub-object from payload if present
    alarmed_obj_raw = payload.get("alarmedObject", {})
    if alarmed_obj_raw:
        alarmed_object = alarmed_obj_raw
    else:
        alarmed_object = {
            "@type":         obj_ref_type,
            "id":            payload.get("alarmedObjectId", "unknown"),
            "@referredType": referred_type,
        }

    # alarmDetails: add prefix only if raw value not already prefixed
    alarm_details_raw = payload.get("alarmDetails", "")
    if alarm_details_raw and not alarm_details_raw.startswith(details_prefix):
        alarm_details = f"{details_prefix}: {alarm_details_raw}"
    else:
        alarm_details = alarm_details_raw

    alarm_const = {
        "@type":             payload.get("@type", object_type),
        "externalAlarmId":   payload.get("externalAlarmId", cid),
        "state":             payload.get("state", state),
        "alarmType":         payload.get("alarmType", alarm_type),
        "perceivedSeverity": payload.get("perceivedSeverity", "indeterminate"),
        "probableCause":     payload.get("probableCause", probable_cause),
        "specificProblem":   payload.get("specificProblem", ""),
        "alarmedObjectType": payload.get("alarmedObjectType", alarmed_obj_type),
        "alarmedObject":     alarmed_object,
        "sourceSystemId":    payload.get("sourceSystemId", source_system_id),
        "alarmRaisedTime":   timestamp,
        "alarmDetails":      alarm_details,
        "correlationId":     cid,
    }

    logger.info("  ✔ externalAlarmId  : %s", alarm_const["externalAlarmId"])
    logger.info("  ✔ alarmType        : %s", alarm_const["alarmType"])
    logger.info("  ✔ perceivedSeverity : %s", alarm_const["perceivedSeverity"])
    logger.info("  ✔ specificProblem   : %s", alarm_const["specificProblem"])

    asyncapi_block = _asyncapi_wrapper(
        "AlarmCreate", "alarmCreate", "createAlarm", alarm_const, cfg, action)

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

    Input payload contains alarmClearedTime, alarmPattern (list of AlarmRef),
    and recovery_event_id.  Fields are passed through directly.
    """
    logger.info("━" * 60)
    logger.info("  STAGE 5 │ transformer › build_clear_cloud_event()")
    logger.info("  ACTION  │ TMF642 ClearAlarm (pre-structured) → CloudEvent")
    logger.info("━" * 60)

    action = "clear"
    cid    = payload.get("correlation_id", "unknown")
    rid    = payload.get("recovery_event_id", "unknown")

    # Timestamp: prefer alarmClearedTime (canonical TMF642 clear field)
    timestamp = (payload.get("alarmClearedTime")
                 or payload.get("timestamp")
                 or payload.get("timestap", ""))

    logger.info("  ✔ correlation_id    : %s", cid)
    logger.info("  ✔ recovery_event_id : %s", rid)
    logger.info("  ✔ alarmClearedTime  : %s", timestamp)

    # alarmPattern: pass through from payload (list of AlarmRef objects)
    alarm_pattern = payload.get(
        "alarmPattern", [{"@type": "AlarmRef", "id": cid}])

    alarm_const = {
        "@type":            payload.get("@type", "ClearAlarm"),
        "alarmClearedTime": timestamp,
        "alarmPattern":     alarm_pattern,
        "correlationId":    cid,
        "recoveryEventId":  rid,
    }

    logger.info("  ✔ alarmPattern: %s", alarm_pattern)

    asyncapi_block = _asyncapi_wrapper(
        "AlarmClear", "alarmClear", "clearAlarm", alarm_const, cfg, action)

    envelope = _ce_base(payload, cfg, action, timestamp)
    envelope["recoveryeventid"] = rid
    envelope["data"] = asyncapi_block
    logger.info("  ✔ CE id=%s  type=%s", envelope["id"], envelope["type"])
    logger.info("  ✔ AlarmClear conversion complete")
    return envelope
