import sys
from pathlib import Path

import pytest
from kubernetes.client.rest import ApiException

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "source" / "eventOperator"))
import kafka_user as ku  # noqa: E402

ON = ku.KafkaUserSettings(enabled=True)


class FakeApi:
    """Records calls; `store` maps KafkaUser name -> object."""

    def __init__(self, store=None):
        self.store, self.calls = dict(store or {}), []

    def get_namespaced_custom_object(self, g, v, ns, plural, name):
        if name not in self.store:
            raise ApiException(status=404)
        return self.store[name]

    def create_namespaced_custom_object(self, g, v, ns, plural, body):
        self.calls.append("create")
        self.store[body["metadata"]["name"]] = body

    def patch_namespaced_custom_object(self, g, v, ns, plural, name, body):
        self.calls.append("patch")
        self.store[name]["spec"] = body["spec"]
        self.store[name]["metadata"]["labels"] = body["metadata"]["labels"]

    def delete_namespaced_custom_object(self, g, v, ns, plural, name):
        self.calls.append("delete")
        del self.store[name]


def access(pub=(), sub=()):
    return ku.ComponentAccess(published=set(pub), subscribed=set(sub))


def test_rules_for_publisher_and_subscriber():
    rules = ku.build_acl_rules(access(pub=["t.out"], sub=[("t.in", "g1")]))
    assert {"resource": {"type": "topic", "name": "t.out", "patternType": "literal"}, "operations": ["Describe", "Write"]} in rules
    assert {"resource": {"type": "topic", "name": "t.in", "patternType": "literal"}, "operations": ["Describe", "Read"]} in rules
    assert {"resource": {"type": "group", "name": "g1", "patternType": "literal"}, "operations": ["Read"]} in rules


def test_topic_both_published_and_read_gets_all_operations():
    rules = ku.build_acl_rules(access(pub=["t"], sub=[("t", "g")]))
    topic = [r for r in rules if r["resource"]["type"] == "topic"][0]
    assert topic["operations"] == ["Describe", "Read", "Write"]


def test_rules_are_stable_regardless_of_order():
    a = ku.build_acl_rules(access(pub=["b", "a"], sub=[("z", "g2"), ("y", "g1")]))
    b = ku.build_acl_rules(access(pub=["a", "b"], sub=[("y", "g1"), ("z", "g2")]))
    assert a == b


def test_user_object_shape():
    obj = ku.build_kafka_user("tmfc043", access(pub=["t"]), ON)
    assert obj["metadata"]["name"] == "oda-tmfc043"
    assert obj["metadata"]["namespace"] == "kafka"
    assert obj["metadata"]["labels"]["strimzi.io/cluster"] == "oda-kafka"
    assert "authentication" not in obj["spec"]
    assert obj["spec"]["authorization"]["type"] == "simple"


def test_disabled_does_nothing():
    api = FakeApi()
    assert ku.KafkaUserManager(ku.KafkaUserSettings(enabled=False), api).sync("c", access(pub=["t"])) == "skipped"
    assert api.calls == []


def test_create_then_unchanged_then_update_then_delete():
    api = FakeApi()
    m = ku.KafkaUserManager(ON, api)
    assert m.sync("c", access(pub=["t"])) == "created"
    assert m.sync("c", access(pub=["t"])) == "unchanged"
    assert m.sync("c", access(pub=["t", "t2"])) == "updated"
    assert m.sync("c", access()) == "deleted"
    assert m.sync("c", access()) == "unchanged"
    assert api.calls == ["create", "patch", "delete"]


def test_invalid_component_name_rejected():
    with pytest.raises(ValueError):
        ku.KafkaUserManager(ON, FakeApi()).sync("Bad_Name", access(pub=["t"]))


def test_api_errors_other_than_404_propagate():
    class Boom(FakeApi):
        def get_namespaced_custom_object(self, *a):
            raise ApiException(status=500)

    with pytest.raises(ApiException):
        ku.KafkaUserManager(ON, Boom()).sync("c", access(pub=["t"]))


# ---- collect_access ------------------------------------------------------- #

def _item(name, comp, uid, **extra):
    return {"metadata": {"name": name, "uid": uid, "labels": {"c": comp}}, "spec": {"topic": f"topic.{name}"}, **extra}


KW = dict(
    owner_of=lambda i: i["metadata"]["labels"]["c"],
    published_topic=lambda i: i["spec"]["topic"],
    subscribed_topic_group=lambda i: (i["spec"]["topic"], "g-" + i["metadata"]["name"]),
)


def test_collect_only_this_component():
    pubs = [_item("p1", "a", "1"), _item("p2", "b", "2")]
    subs = [_item("s1", "a", "3"), _item("s2", "b", "4")]
    got = ku.collect_access("a", pubs, subs, **KW)
    assert got.published == {"topic.p1"} and got.subscribed == {("topic.s1", "g-s1")}


def test_collect_skips_pending_topic():
    subs = [_item("s1", "a", "3", status={"status": "PendingTopic"})]
    assert ku.collect_access("a", [], subs, **KW).is_empty()


def test_collect_excludes_object_being_deleted_and_adds_current():
    pubs = [_item("p1", "a", "1"), _item("p2", "a", "2")]
    got = ku.collect_access("a", pubs, [], exclude_uid="1", include=access(pub=["new"]), **KW)
    assert got.published == {"topic.p2", "new"}


def test_collect_survives_one_bad_object():
    bad = {"metadata": {"name": "x", "uid": "9"}, "spec": {}}  # owner_of raises KeyError
    got = ku.collect_access("a", [bad, _item("p1", "a", "1")], [], **KW)
    assert got.published == {"topic.p1"}


def test_collect_ignores_objects_already_being_deleted():
    pubs = [_item("p1", "a", "1"), _item("p2", "a", "2")]
    pubs[1]["metadata"]["deletionTimestamp"] = "2026-11-11T11:44:00Z"
    got = ku.collect_access("a", pubs, [], **KW)
    assert got.published == {"topic.p1"}
