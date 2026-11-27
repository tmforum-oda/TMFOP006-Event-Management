"""Unit tests for operator/topic_naming.py (pure Python, no broker, no cluster).

Run from the repo root:   pytest tests/test_topic_naming.py
"""

import os
import sys
from collections.abc import Mapping

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "source", "eventOperator"))

import topic_naming as tn  # noqa: E402

# (domain, kind, event name, topic exactly as drawn in the flow diagram)
DIAGRAM_TOPICS = [
    ("trapReceiver", "event", "alarmDetected", "oda.trapReceiver.event.alarmDetected"),
    ("tmf642", None, "alarmCreateEvent", "oda.tmf642.alarmCreateEvent"),
    ("alarmManagement", "command", "createAlarm", "oda.alarmManagement.command.createAlarm"),
    ("digitalTwin", None, "createAlarmNotificationEvent", "oda.digitalTwin.createAlarmNotificationEvent"),
    (
        "digitalTwin",
        None,
        "createServiceProblemNotificationEvent",
        "oda.digitalTwin.createServiceProblemNotificationEvent",
    ),
    (
        "alarmManagement",
        None,
        "serviceProblemCreateNotificationEvent",
        "oda.alarmManagement.serviceProblemCreateNotificationEvent",
    ),
    (
        "tmf656",
        None,
        "serviceProblemCreateNotificationEvent",
        "oda.tmf656.serviceProblemCreateNotificationEvent",
    ),
    ("digitalTwin", "command", "createTroubleTicket", "oda.digitalTwin.command.createTroubleTicket"),
]


@pytest.mark.parametrize("domain,kind,event,expected", DIAGRAM_TOPICS)
def test_diagram_topics_are_built_exactly_as_drawn(domain, kind, event, expected):
    assert tn.conventional_topic("oda", domain, kind, event) == expected


def test_case_is_preserved():
    assert tn.conventional_topic("oda", "digitalTwin", "command", "createAlarm") == (
        "oda.digitalTwin.command.createAlarm"
    )


def test_the_two_service_problem_topics_differ():
    a = tn.conventional_topic("oda", "alarmManagement", None, "serviceProblemCreateNotificationEvent")
    b = tn.conventional_topic("oda", "tmf656", None, "serviceProblemCreateNotificationEvent")
    assert a != b


def test_illegal_characters_in_parts_are_folded_not_dropped():
    assert tn.conventional_topic("oda", "my domain", None, "evt/one") == "oda.my-domain.evt-one"


@pytest.mark.parametrize("kind", ["Event", "commands", "cmd", "query"])
def test_unknown_kind_is_rejected(kind):
    with pytest.raises(ValueError):
        tn.conventional_topic("oda", "tmf642", kind, "x")


@pytest.mark.parametrize("domain,event", [("", "x"), (None, "x"), ("tmf642", ""), ("tmf642", None), ("  ", "x")])
def test_missing_domain_or_event_is_rejected(domain, event):
    with pytest.raises(ValueError):
        tn.conventional_topic("oda", domain, None, event)


def test_explicit_topic_is_returned_unchanged():
    t = "oda.digitalTwin.command.createAlarm"
    assert tn.validate_topic(t) == t


def test_explicit_topic_is_trimmed_but_not_lowercased():
    assert tn.validate_topic("  oda.Mixed.Case  ") == "oda.Mixed.Case"


@pytest.mark.parametrize("bad", ["", "   ", ".", "..", "has space", "slash/topic", "a" * 250, None])
def test_invalid_explicit_topics_are_rejected(bad):
    with pytest.raises(ValueError):
        tn.validate_topic(bad)


def test_max_length_is_accepted():
    assert len(tn.validate_topic("a" * 249)) == 249


# --- case collisions --------------------------------------------------------------


def test_case_variants_finds_a_different_spelling():
    existing = ["oda.digitaltwin.command.createalarm", "oda.tmf642.alarmCreateEvent"]
    assert tn.case_variants("oda.digitalTwin.command.createAlarm", existing) == [
        "oda.digitaltwin.command.createalarm"
    ]


def test_case_variants_ignores_the_exact_topic():
    assert tn.case_variants("oda.tmf642.alarmCreateEvent", ["oda.tmf642.alarmCreateEvent"]) == []


def test_case_variants_ignores_unrelated_topics():
    assert tn.case_variants("oda.a.b", ["oda.a.c", "other"]) == []


# --- api_type_of ------------------------------------------------------------------


def test_api_type_from_top_level():
    assert tn.api_type_of({"apiType": "a2a", "specification": [{"apiType": "openapi"}]}) == "a2a"


def test_api_type_from_specification_entry():
    assert tn.api_type_of({"specification": [{"url": "x"}, {"apiType": "asyncapi"}]}) == "asyncapi"


@pytest.mark.parametrize("spec", [{}, {"specification": []}, {"specification": [{"url": "x"}]}, None, "x"])
def test_api_type_missing(spec):
    assert tn.api_type_of(spec) is None


# --- convention check ---------------------------------------------------------------


@pytest.mark.parametrize("domain,kind,event,expected", DIAGRAM_TOPICS)
def test_every_diagram_topic_follows_the_convention(domain, kind, event, expected):
    assert tn.check_convention(expected) is None


@pytest.mark.parametrize("legacy", ["oda.tmfc043.alarmcreateevent", "oda.ticketing.troubleticketcreateevent"])
def test_legacy_derived_topics_also_conform(legacy):
    assert tn.check_convention(legacy) is None


@pytest.mark.parametrize(
    "bad",
    [
        "tmf642.alarmCreateEvent",            # missing prefix
        "oda.alarmCreateEvent",               # no domain
        "oda.a.b.c.d",                        # too many segments
        "oda.digital-twin.createAlarm",       # hyphen in a segment
        "oda.tmf642.1stEvent",                # event starts with a digit
        "oda.tmf642.command.",                # empty event
    ],
)
def test_nonconforming_topics_get_a_warning(bad):
    assert tn.check_convention(bad) is not None


def test_convention_check_respects_a_custom_prefix():
    assert tn.check_convention("acme.tmf642.alarmCreateEvent", prefix="acme") is None
    assert tn.check_convention("oda.tmf642.alarmCreateEvent", prefix="acme") is not None


# --- resolve_topic --------------------------------------------------------------------


def _legacy():
    return "oda.tmfc043.alarmcreateevent"


def test_explicit_topic_wins_and_keeps_case():
    spec = {"topic": "oda.alarmManagement.command.createAlarm", "topicDomain": "ignored"}
    topic, warnings = tn.resolve_topic(spec, "c", "e", "oda", _legacy)
    assert topic == "oda.alarmManagement.command.createAlarm"
    assert warnings == []


def test_explicit_topic_from_eventBroker_block():
    spec = {"eventBroker": {"topic": "oda.tmf642.alarmCreateEvent"}}
    assert tn.resolve_topic(spec, "c", "e", "oda", _legacy)[0] == "oda.tmf642.alarmCreateEvent"


def test_explicit_nonconforming_topic_is_used_with_a_warning():
    topic, warnings = tn.resolve_topic({"topic": "my-old-topic"}, "c", "e", "oda", _legacy)
    assert topic == "my-old-topic"
    assert len(warnings) == 1 and "my-old-topic" in warnings[0]


def test_domain_builds_the_conventional_name():
    spec = {"topicDomain": "digitalTwin", "topicKind": "command"}
    topic, _ = tn.resolve_topic(spec, "c", "createTroubleTicket", "oda", _legacy)
    assert topic == "oda.digitalTwin.command.createTroubleTicket"


def test_topicEvent_overrides_the_notification_name():
    spec = {"topicDomain": "tmf642", "topicEvent": "alarmCreateEvent"}
    topic, _ = tn.resolve_topic(spec, "tmfc043", "alarmCreateNotificationEvent", "oda", _legacy)
    assert topic == "oda.tmf642.alarmCreateEvent"


def test_legacy_is_used_only_when_nothing_else_is_set():
    calls = []

    def legacy():
        calls.append(1)
        return "oda.tmfc043.alarmcreateevent"

    assert tn.resolve_topic({"name": "x"}, "tmfc043", "alarmCreateEvent", "oda", legacy)[0] == (
        "oda.tmfc043.alarmcreateevent"
    )
    assert calls == [1]
    tn.resolve_topic({"topic": "oda.a.b"}, "c", "e", "oda", legacy)
    tn.resolve_topic({"topicDomain": "a"}, "c", "e", "oda", legacy)
    assert calls == [1]  # not called again


@pytest.mark.parametrize("spec", [{"topic": "has space"}, {"topic": "a" * 250}, {"topicDomain": "a", "topicKind": "query"}])
def test_bad_specs_raise_value_error(spec):
    with pytest.raises(ValueError):
        tn.resolve_topic(spec, "c", "e", "oda", _legacy)


class _MappingSpec(Mapping):
    """Stands in for kopf's Spec, which is a Mapping but not a dict."""

    def __init__(self, data):
        self._d = data

    def __getitem__(self, key):
        return self._d[key]

    def __iter__(self):
        return iter(self._d)

    def __len__(self):
        return len(self._d)


def test_api_type_of_accepts_non_dict_mapping_top_level():
    assert tn.api_type_of(_MappingSpec({"apiType": "a2a"})) == "a2a"


def test_api_type_of_accepts_non_dict_mapping_in_specification():
    spec = _MappingSpec({"specification": [_MappingSpec({"apiType": "asyncapi"})]})
    assert tn.api_type_of(spec) == "asyncapi"


def test_resolve_topic_accepts_non_dict_mapping_spec():
    spec = _MappingSpec({"topic": "oda.digitalTwin.command.createAlarm"})
    topic, _ = tn.resolve_topic(spec, "c", "e", "oda", lambda: "legacy")
    assert topic == "oda.digitalTwin.command.createAlarm"
