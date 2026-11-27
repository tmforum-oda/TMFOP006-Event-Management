"""Kafka administration helpers for the ODA Canvas Event Management Operator.

Wraps ``kafka-python``'s ``KafkaAdminClient`` with the small set of idempotent
operations the operator needs:

* topic create / describe / delete
* producer and consumer ACL grant / revoke
* consumer-group ACL grant / revoke
* SASL/SCRAM credential provisioning is delegated to the broker admin API where
  available, otherwise credentials are read from a Kubernetes Secret.

Design notes
------------
* Every operation is **idempotent** — re-running it against an already-correct
  broker is a no-op that returns success. This matters because KOPF handlers are
  retried and are also invoked on ``resume`` after an operator restart.
* Nothing in this module deletes a topic implicitly. Topic deletion is only ever
  performed by an explicit :func:`delete_topic` call, which the operator makes
  only from the ``PublishedNotification`` delete path once ownership has been
  resolved (UC017 rule: a ``SubscribedNotification`` must never delete a topic).
* All broker coordinates come from the environment (see ``EventBrokerConfig``);
  nothing is hard-coded.
"""

from __future__ import annotations

import os
import ssl
import threading
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

import requests

from kafka import KafkaAdminClient

# NOTE: all of these come from `kafka.admin` directly. Do NOT import from
# `kafka.admin.acl_resource` — that private submodule existed in kafka-python
# 2.x and was removed in 3.x. Also note kafka-python < 3.0 does not import at
# all on Python 3.12 (its vendored `six` shim relies on import machinery that
# 3.12 removed), so requirements.txt pins >= 3.0.
from kafka.admin import (
    ACL,
    ACLFilter,
    ACLOperation,
    ACLPermissionType,
    ACLResourcePatternType,
    NewTopic,
    ResourcePattern,
    ResourcePatternFilter,
    ResourceType,
)
from kafka.errors import (
    KafkaError,
    SecurityDisabledError,
    TopicAlreadyExistsError,
    UnknownTopicOrPartitionError,
    UnsupportedVersionError,
)

from log_wrapper import LogWrapper

# --------------------------------------------------------------------------- #
# Configuration                                                               #
# --------------------------------------------------------------------------- #


def _env_bool(name: str, default: bool = False) -> bool:
    raw = os.environ.get(name)
    if raw is None:
        return default
    return raw.strip().lower() in ("1", "true", "yes", "on")


def _env_int(name: str, default: int) -> int:
    raw = os.environ.get(name)
    if raw is None or raw.strip() == "":
        return default
    return int(raw)


@dataclass
class EventBrokerConfig:
    """Broker coordinates, entirely environment driven.

    Defaults target the COMMON Services Kafka 3.8.0 cluster. Override every
    value through the operator ConfigMap / Secret — never hard-code a broker
    address in handler code.
    """

    bootstrap_servers: str = field(
        default_factory=lambda: os.environ.get(
            "KAFKA_BOOTSTRAP_SERVERS", "kafka-bootstrap.common-services.svc:9092"
        )
    )
    security_protocol: str = field(
        default_factory=lambda: os.environ.get("KAFKA_SECURITY_PROTOCOL", "PLAINTEXT")
    )
    sasl_mechanism: Optional[str] = field(
        default_factory=lambda: os.environ.get("KAFKA_SASL_MECHANISM") or None
    )
    sasl_username: Optional[str] = field(
        default_factory=lambda: os.environ.get("KAFKA_SASL_USERNAME") or None
    )
    sasl_password: Optional[str] = field(
        default_factory=lambda: os.environ.get("KAFKA_SASL_PASSWORD") or None
    )
    ssl_cafile: Optional[str] = field(
        default_factory=lambda: os.environ.get("KAFKA_SSL_CAFILE") or None
    )

    # Topic provisioning policy
    default_partitions: int = field(
        default_factory=lambda: _env_int("KAFKA_TOPIC_PARTITIONS", 3)
    )
    default_replication_factor: int = field(
        default_factory=lambda: _env_int("KAFKA_TOPIC_REPLICATION_FACTOR", 3)
    )
    default_retention_ms: int = field(
        default_factory=lambda: _env_int("KAFKA_TOPIC_RETENTION_MS", 604800000)
    )
    topic_prefix: str = field(
        default_factory=lambda: os.environ.get("KAFKA_TOPIC_PREFIX", "oda")
    )
    #: When False the operator never issues ``delete_topics`` — safest posture
    #: for a shared broker. Deletion then only marks the topic as orphaned.
    allow_topic_delete: bool = field(
        default_factory=lambda: _env_bool("KAFKA_ALLOW_TOPIC_DELETE", False)
    )

    # Schema Registry (external, HTTPS)
    schema_registry_url: Optional[str] = field(
        default_factory=lambda: os.environ.get("SCHEMA_REGISTRY_URL") or None
    )
    schema_registry_username: Optional[str] = field(
        default_factory=lambda: os.environ.get("SCHEMA_REGISTRY_USERNAME") or None
    )
    schema_registry_password: Optional[str] = field(
        default_factory=lambda: os.environ.get("SCHEMA_REGISTRY_PASSWORD") or None
    )
    schema_registry_verify_ssl: bool = field(
        default_factory=lambda: _env_bool("SCHEMA_REGISTRY_VERIFY_SSL", True)
    )

    request_timeout_ms: int = field(
        default_factory=lambda: _env_int("KAFKA_REQUEST_TIMEOUT_MS", 30000)
    )

    def admin_client_kwargs(self) -> Dict[str, Any]:
        kwargs: Dict[str, Any] = {
            "bootstrap_servers": [
                s.strip() for s in self.bootstrap_servers.split(",") if s.strip()
            ],
            "client_id": "oda-eventmanagement-operator",
            "security_protocol": self.security_protocol,
            "request_timeout_ms": self.request_timeout_ms,
        }
        if self.sasl_mechanism:
            kwargs["sasl_mechanism"] = self.sasl_mechanism
            kwargs["sasl_plain_username"] = self.sasl_username
            kwargs["sasl_plain_password"] = self.sasl_password
        if "SSL" in self.security_protocol:
            context = ssl.create_default_context(cafile=self.ssl_cafile)
            kwargs["ssl_context"] = context
        return kwargs


# --------------------------------------------------------------------------- #
# Naming                                                                      #
# --------------------------------------------------------------------------- #


def topic_name(config: EventBrokerConfig, component_name: str, event_name: str) -> str:
    """Deterministic topic name.

    ``<prefix>.<componentName>.<eventName>`` lower-cased with unsupported
    characters folded to ``-``. Determinism is what lets a subscriber resolve a
    publisher's topic without any shared state.
    """
    parts = [config.topic_prefix, component_name, event_name]
    raw = ".".join(p for p in parts if p)
    return "".join(
        c if (c.isalnum() or c in ("-", "_", ".")) else "-" for c in raw
    ).lower()


def consumer_group_id(component_name: str, subscription_name: str) -> str:
    """Consumer group per (component, subscription) pair."""
    raw = f"oda-{component_name}-{subscription_name}"
    return "".join(
        c if (c.isalnum() or c in ("-", "_", ".")) else "-" for c in raw
    ).lower()


def principal_for_component(component_name: str) -> str:
    """Kafka principal for a component's producer/consumer identity."""
    return f"User:oda-{component_name}"


# --------------------------------------------------------------------------- #
# Admin client (lazily created, thread-safe, reconnecting)                    #
# --------------------------------------------------------------------------- #


class KafkaAdmin:
    """Thin idempotent facade over ``KafkaAdminClient``.

    The client is created lazily and cached. Any :class:`KafkaError` invalidates
    the cache so the next call reconnects — brokers restart, and a stale admin
    client would otherwise wedge the operator until the pod is recycled.
    """

    def __init__(self, config: Optional[EventBrokerConfig] = None):
        self.config = config or EventBrokerConfig()
        self._client: Optional[KafkaAdminClient] = None
        self._lock = threading.Lock()
        self.logw = LogWrapper(handler_name="KafkaAdmin")

    # -- lifecycle ---------------------------------------------------------- #

    @property
    def client(self) -> KafkaAdminClient:
        with self._lock:
            if self._client is None:
                self._client = KafkaAdminClient(**self.config.admin_client_kwargs())
            return self._client

    def invalidate(self) -> None:
        with self._lock:
            if self._client is not None:
                try:
                    self._client.close()
                except Exception:  # noqa: BLE001 - close must never mask the real error
                    self.logw.warning("invalidate", "error closing stale admin client")
                self._client = None

    def health_check(self) -> str:
        """Used by the ``@kopf.on.probe`` liveness handler."""
        try:
            cluster = self.client.describe_cluster()
            return f"ok: cluster={cluster.get('cluster_id')} controller={cluster.get('controller_id')}"
        except KafkaError:
            self.invalidate()
            raise

    # -- topics ------------------------------------------------------------- #

    def topic_exists(self, topic: str) -> bool:
        try:
            return topic in self.client.list_topics()
        except KafkaError:
            self.invalidate()
            raise

    def describe_topic(self, topic: str) -> Optional[Dict[str, Any]]:
        try:
            described = self.client.describe_topics([topic])
        except UnknownTopicOrPartitionError:
            return None
        except KafkaError:
            self.invalidate()
            raise
        for entry in described:
            if entry.get("error_code", 0) == 0 and entry.get("topic") == topic:
                return entry
        return None

    def create_topic(
        self,
        topic: str,
        partitions: Optional[int] = None,
        replication_factor: Optional[int] = None,
        retention_ms: Optional[int] = None,
        extra_configs: Optional[Dict[str, str]] = None,
    ) -> Dict[str, Any]:
        """Create the topic if absent. Returns a description either way."""
        configs = {
            "retention.ms": str(
                retention_ms
                if retention_ms is not None
                else self.config.default_retention_ms
            ),
            "cleanup.policy": "delete",
        }
        if extra_configs:
            configs.update({k: str(v) for k, v in extra_configs.items()})

        new_topic = NewTopic(
            name=topic,
            num_partitions=partitions or self.config.default_partitions,
            replication_factor=(
                replication_factor or self.config.default_replication_factor
            ),
            topic_configs=configs,
        )
        try:
            self.client.create_topics([new_topic], validate_only=False)
            self.logw.info("create_topic", f"created topic {topic}")
        except TopicAlreadyExistsError:
            self.logw.info("create_topic", f"topic {topic} already exists - no action")
        except KafkaError:
            self.invalidate()
            raise
        return {
            "topic": topic,
            "partitions": new_topic.num_partitions,
            "replicationFactor": new_topic.replication_factor,
            "configs": configs,
        }

    def delete_topic(self, topic: str) -> bool:
        """Delete a topic. Guarded by ``KAFKA_ALLOW_TOPIC_DELETE``.

        Returns True if a delete was actually issued. Callers must have already
        confirmed no other PublishedNotification or SubscribedNotification
        references the topic.
        """
        if not self.config.allow_topic_delete:
            self.logw.warning(
                "delete_topic",
                f"topic delete disabled by policy - leaving {topic} in place",
            )
            return False
        try:
            self.client.delete_topics([topic])
            self.logw.info("delete_topic", f"deleted topic {topic}")
            return True
        except UnknownTopicOrPartitionError:
            return False
        except KafkaError:
            self.invalidate()
            raise

    # -- ACLs --------------------------------------------------------------- #

    @staticmethod
    def _topic_acl(principal: str, topic: str, operation: ACLOperation) -> ACL:
        return ACL(
            principal=principal,
            host="*",
            operation=operation,
            permission_type=ACLPermissionType.ALLOW,
            resource_pattern=ResourcePattern(
                ResourceType.TOPIC, topic, ACLResourcePatternType.LITERAL
            ),
        )

    @staticmethod
    def _group_acl(principal: str, group: str, operation: ACLOperation) -> ACL:
        return ACL(
            principal=principal,
            host="*",
            operation=operation,
            permission_type=ACLPermissionType.ALLOW,
            resource_pattern=ResourcePattern(
                ResourceType.GROUP, group, ACLResourcePatternType.LITERAL
            ),
        )

    #: Sentinel returned instead of an ACL list when the broker has no
    #: authorizer configured. Distinguishable from a real grant so status
    #: reflects reality rather than implying enforcement that is not happening.
    ACLS_UNSUPPORTED = "acls-not-supported-by-broker"

    def _create_acls(self, acls: List[ACL]) -> bool:
        """Create ACLs. Returns False when the broker does not support them.

        A Kafka broker started without ``authorizer.class.name`` answers
        ``CreateAcls`` with ``SecurityDisabledError`` (errno 54). That is a
        property of the broker's configuration, not a fault in the request — so
        treating it as a handler failure would leave every PublishedNotification
        stuck in ``Error`` on any cluster where ACLs are simply turned off
        (common for single-node dev brokers). Topic provisioning still succeeded
        and pub/sub still works; the operator records that ACLs were skipped and
        moves on.
        """
        try:
            result = self.client.create_acls(acls)
        except (SecurityDisabledError, UnsupportedVersionError) as exc:
            self.logw.warning(
                "_create_acls",
                f"broker does not support ACLs ({type(exc).__name__}) - "
                f"skipping {len(acls)} ACL binding(s). Topics are still "
                f"provisioned; enable an authorizer on the broker to enforce them.",
            )
            return False
        except KafkaError:
            self.invalidate()
            raise

        # An already-present ACL is reported as success by Kafka, so anything in
        # `failed` is a real problem worth surfacing.
        #
        # The shape of `failed` CHANGED between kafka-python majors:
        #   2.x -> [(acl, error_class), ...]
        #   3.x -> [error_class, ...]            (the ACL reference was dropped)
        # Normalise both so this survives a version bump either way.
        for entry in result.get("failed", []) or []:
            if isinstance(entry, tuple):
                acl, error = entry
            else:
                acl, error = None, entry
            unsupported = isinstance(error, type) and issubclass(
                error, (SecurityDisabledError, UnsupportedVersionError)
            )
            if unsupported:
                self.logw.warning(
                    "_create_acls",
                    "broker reported ACLs unsupported "
                    f"({getattr(error, '__name__', error)}) - skipping",
                )
                return False
            where = f" for {acl}" if acl is not None else ""
            raise KafkaError(f"failed to create ACL{where}: {error}")
        return True

    def grant_producer_acls(self, principal: str, topic: str) -> List[str]:
        """WRITE + DESCRIBE on the topic. Idempotent.

        Returns the granted bindings, or ``[ACLS_UNSUPPORTED]`` if the broker has
        no authorizer.
        """
        ops = [ACLOperation.WRITE, ACLOperation.DESCRIBE]
        granted = self._create_acls(
            [self._topic_acl(principal, topic, op) for op in ops]
        )
        if not granted:
            return [self.ACLS_UNSUPPORTED]
        return [f"{principal}:{op.name}:TOPIC:{topic}" for op in ops]

    def grant_consumer_acls(self, principal: str, topic: str, group: str) -> List[str]:
        """READ + DESCRIBE on the topic and READ on the consumer group.

        Returns the granted bindings, or ``[ACLS_UNSUPPORTED]`` if the broker has
        no authorizer.
        """
        topic_ops = [ACLOperation.READ, ACLOperation.DESCRIBE]
        acls = [self._topic_acl(principal, topic, op) for op in topic_ops]
        acls.append(self._group_acl(principal, group, ACLOperation.READ))
        if not self._create_acls(acls):
            return [self.ACLS_UNSUPPORTED]
        return [f"{principal}:{op.name}:TOPIC:{topic}" for op in topic_ops] + [
            f"{principal}:READ:GROUP:{group}"
        ]

    def revoke_acls(
        self,
        principal: str,
        topic: Optional[str] = None,
        group: Optional[str] = None,
    ) -> int:
        """Revoke this principal's ACLs. Returns the number of bindings removed.

        Called only *after* replacement ACLs are in place (update path) or after
        the adapter has been stopped (delete path) — never before.
        """
        filters: List[ACLFilter] = []
        if topic:
            filters.append(
                ACLFilter(
                    principal=principal,
                    host="*",
                    operation=ACLOperation.ANY,
                    permission_type=ACLPermissionType.ANY,
                    resource_pattern=ResourcePatternFilter(
                        ResourceType.TOPIC, topic, ACLResourcePatternType.LITERAL
                    ),
                )
            )
        if group:
            filters.append(
                ACLFilter(
                    principal=principal,
                    host="*",
                    operation=ACLOperation.ANY,
                    permission_type=ACLPermissionType.ANY,
                    resource_pattern=ResourcePatternFilter(
                        ResourceType.GROUP, group, ACLResourcePatternType.LITERAL
                    ),
                )
            )
        if not filters:
            return 0
        try:
            results = self.client.delete_acls(filters)
        except (SecurityDisabledError, UnsupportedVersionError) as exc:
            # Symmetric with _create_acls: nothing was granted, so nothing to
            # revoke. Must not block a delete handler and leave the finalizer on.
            self.logw.warning(
                "revoke_acls",
                f"broker does not support ACLs ({type(exc).__name__}) - "
                f"nothing to revoke",
            )
            return 0
        except KafkaError:
            self.invalidate()
            raise
        removed = 0
        for _acl_filter, matching, error in results:
            if error is not None and getattr(error, "errno", 0) != 0:
                self.logw.warning("revoke_acls", f"partial ACL revoke failure: {error}")
                continue
            removed += len(matching)
        return removed

    # -- schema registry ---------------------------------------------------- #

    def register_schema(self, subject: str, schema: str, schema_type: str = "AVRO"):
        """Register a schema for the topic's value subject.

        Returns the registry's schema id, or None when no registry is configured
        (a schema-less deployment is a legitimate configuration).
        """
        if not self.config.schema_registry_url:
            return None
        url = f"{self.config.schema_registry_url.rstrip('/')}/subjects/{subject}/versions"
        auth = None
        if self.config.schema_registry_username:
            auth = (
                self.config.schema_registry_username,
                self.config.schema_registry_password or "",
            )
        response = requests.post(
            url,
            json={"schema": schema, "schemaType": schema_type},
            headers={"Content-Type": "application/vnd.schemaregistry.v1+json"},
            auth=auth,
            verify=self.config.schema_registry_verify_ssl,
            timeout=self.config.request_timeout_ms / 1000.0,
        )
        response.raise_for_status()
        return response.json().get("id")

    def schema_registry_health(self) -> str:
        if not self.config.schema_registry_url:
            return "not-configured"
        auth = None
        if self.config.schema_registry_username:
            auth = (
                self.config.schema_registry_username,
                self.config.schema_registry_password or "",
            )
        response = requests.get(
            f"{self.config.schema_registry_url.rstrip('/')}/subjects",
            auth=auth,
            verify=self.config.schema_registry_verify_ssl,
            timeout=10,
        )
        response.raise_for_status()
        return "ok"