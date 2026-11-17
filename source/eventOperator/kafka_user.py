"""Strimzi ``KafkaUser`` objects that keep a component's Kafka permissions.

Why this exists
---------------
On a Strimzi cluster the User Operator owns the broker's ACLs. On each periodic
reconcile it removes the ACLs of every principal that has no ``KafkaUser`` object. ACLs
the Event Management Operator creates directly through the Kafka admin API therefore
disappear after a while. A ``KafkaUser`` with the same ACLs makes them permanent.

One ``KafkaUser`` per component, named after the principal (``oda-<component>``). A
component has many notifications, so the object is always rebuilt from the whole set of
that component's notifications, never edited one event at a time.

The ``KafkaUser`` carries no ``authentication`` section: it only owns the ACLs. How the
client logs in (SCRAM, Keycloak OAuth, ...) is outside this module.

Pure functions (``build_acl_rules``, ``build_kafka_user``) have no Kubernetes dependency,
so they can be unit-tested. Disabled unless ``KAFKA_USER_MANAGEMENT=true``.
"""

from __future__ import annotations

import os
import re
from dataclasses import dataclass, field
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple

import kubernetes
from kubernetes.client.rest import ApiException

MANAGED_BY_LABEL = "app.kubernetes.io/managed-by"
MANAGER = "eventmanagement-operator"
COMPONENT_LABEL = "oda.tmforum.org/componentName"
USER_PREFIX = "oda-"

# A Kubernetes object name: lower-case alphanumerics, '-' and '.'.
_K8S_NAME = re.compile(r"^[a-z0-9]([-a-z0-9.]*[a-z0-9])?$")

PRODUCER_OPS = ("Write", "Describe")
CONSUMER_OPS = ("Read", "Describe")
GROUP_OPS = ("Read",)


@dataclass(frozen=True)
class KafkaUserSettings:
    enabled: bool = False
    namespace: str = "kafka"
    cluster: str = "oda-kafka"
    api_version: str = "kafka.strimzi.io/v1"

    @classmethod
    def from_env(cls, env=os.environ) -> "KafkaUserSettings":
        return cls(
            enabled=env.get("KAFKA_USER_MANAGEMENT", "false").strip().lower() in ("1", "true", "yes"),
            namespace=env.get("KAFKA_USER_NAMESPACE", "kafka"),
            cluster=env.get("KAFKA_CLUSTER_NAME", "oda-kafka"),
            api_version=env.get("KAFKA_USER_API_VERSION", "kafka.strimzi.io/v1"),
        )


@dataclass
class ComponentAccess:
    """What one component may do on Kafka: topics it publishes, topics+groups it reads."""

    published: Set[str] = field(default_factory=set)
    subscribed: Set[Tuple[str, str]] = field(default_factory=set)  # (topic, consumer group)

    def is_empty(self) -> bool:
        return not self.published and not self.subscribed


def user_name(component: str) -> str:
    return f"{USER_PREFIX}{component}"


def valid_user_name(name: str) -> bool:
    return bool(_K8S_NAME.match(name)) and len(name) <= 253


def build_acl_rules(access: ComponentAccess) -> List[Dict[str, Any]]:
    """Strimzi ACL rules for a component's access. Sorted, so the output is stable."""
    ops: Dict[Tuple[str, str], Set[str]] = {}

    def add(kind: str, name: str, operations: Iterable[str]) -> None:
        ops.setdefault((kind, name), set()).update(operations)

    for topic in access.published:
        add("topic", topic, PRODUCER_OPS)
    for topic, group in access.subscribed:
        add("topic", topic, CONSUMER_OPS)
        add("group", group, GROUP_OPS)

    return [
        {"resource": {"type": kind, "name": name, "patternType": "literal"}, "operations": sorted(operations)}
        for (kind, name), operations in sorted(ops.items())
    ]


def build_kafka_user(component: str, access: ComponentAccess, settings: KafkaUserSettings) -> Dict[str, Any]:
    return {
        "apiVersion": settings.api_version,
        "kind": "KafkaUser",
        "metadata": {
            "name": user_name(component),
            "namespace": settings.namespace,
            "labels": {
                "strimzi.io/cluster": settings.cluster,
                MANAGED_BY_LABEL: MANAGER,
                COMPONENT_LABEL: component,
            },
        },
        "spec": {"authorization": {"type": "simple", "acls": build_acl_rules(access)}},
    }


def _leaving(item: Dict[str, Any], exclude_uid: Optional[str]) -> bool:
    """True for an object that must not count: the one being deleted, or any object that
    already has a deletionTimestamp. Several objects of one component can be deleted at once;
    without the second test the last handler to run would still see its siblings and leave
    their ACLs behind.
    """
    meta = item.get("metadata", {}) or {}
    if exclude_uid and meta.get("uid") == exclude_uid:
        return True
    return bool(meta.get("deletionTimestamp"))


def collect_access(
    component: str,
    published: Iterable[Dict[str, Any]],
    subscribed: Iterable[Dict[str, Any]],
    *,
    owner_of,
    published_topic,
    subscribed_topic_group,
    exclude_uid: Optional[str] = None,
    include: Optional[ComponentAccess] = None,
) -> ComponentAccess:
    """Everything ``component`` may access, from all its notification objects.

    The three callables keep this free of operator internals:
    ``owner_of(item)`` -> component name, ``published_topic(item)`` -> topic,
    ``subscribed_topic_group(item)`` -> (topic, group). An item that cannot be resolved is
    skipped. A subscriber still waiting for its topic (``PendingTopic``) gets no access yet,
    matching what the operator grants. ``include`` adds the notification being reconciled
    right now; ``exclude_uid`` leaves out one that is being deleted.
    """
    access = ComponentAccess()
    for item in published:
        if _leaving(item, exclude_uid):
            continue
        try:
            if owner_of(item) == component:
                access.published.add(published_topic(item))
        except Exception:  # noqa: BLE001 - one bad object must not block the others
            continue
    for item in subscribed:
        if _leaving(item, exclude_uid):
            continue
        if (item.get("status", {}) or {}).get("status") == "PendingTopic":
            continue
        try:
            if owner_of(item) == component:
                access.subscribed.add(tuple(subscribed_topic_group(item)))
        except Exception:  # noqa: BLE001
            continue
    if include is not None:
        access.published |= include.published
        access.subscribed |= include.subscribed
    return access


class KafkaUserManager:
    """Creates, updates and deletes the ``KafkaUser`` of a component."""

    def __init__(self, settings: Optional[KafkaUserSettings] = None, api: Any = None):
        self.settings = settings or KafkaUserSettings.from_env()
        self._api = api

    @property
    def enabled(self) -> bool:
        return self.settings.enabled

    @property
    def api(self) -> Any:
        if self._api is None:
            self._api = kubernetes.client.CustomObjectsApi()
        return self._api

    def _coords(self) -> Tuple[str, str, str]:
        group, version = self.settings.api_version.split("/")
        return group, version, "kafkausers"

    def sync(self, component: str, access: ComponentAccess) -> str:
        """Make the cluster match ``access``. Returns created / updated / deleted / unchanged / skipped."""
        if not self.enabled:
            return "skipped"
        name = user_name(component)
        if not valid_user_name(name):
            raise ValueError(f"cannot create a KafkaUser named {name!r}: not a valid Kubernetes name")
        group, version, plural = self._coords()
        ns = self.settings.namespace

        try:
            existing = self.api.get_namespaced_custom_object(group, version, ns, plural, name)
        except ApiException as exc:
            if exc.status != 404:
                raise
            existing = None

        if access.is_empty():
            if existing is None:
                return "unchanged"
            self.api.delete_namespaced_custom_object(group, version, ns, plural, name)
            return "deleted"

        desired = build_kafka_user(component, access, self.settings)
        if existing is None:
            self.api.create_namespaced_custom_object(group, version, ns, plural, desired)
            return "created"

        same_acls = (existing.get("spec", {}).get("authorization", {}) or {}).get("acls") == desired["spec"]["authorization"]["acls"]
        labels = (existing.get("metadata", {}) or {}).get("labels", {}) or {}
        same_labels = all(labels.get(k) == v for k, v in desired["metadata"]["labels"].items())
        if same_acls and same_labels:
            return "unchanged"
        # JSON merge patch replaces the whole acls list, which is what we want.
        self.api.patch_namespaced_custom_object(
            group, version, ns, plural, name,
            {"metadata": {"labels": desired["metadata"]["labels"]}, "spec": desired["spec"]},
        )
        return "updated"
