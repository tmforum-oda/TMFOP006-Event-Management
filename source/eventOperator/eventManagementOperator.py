"""ODA Canvas TMFOP006 Event Management Operator - Kafka implementation.

Watches the ``PublishedNotification`` and ``SubscribedNotification`` sub-resources
created by the Component Management operator and reconciles them against a Kafka
cluster (COMMON Services Kafka 3.8.0) plus an external Schema Registry.

Published side
    Create/reconcile the Kafka topic, register the schema, grant producer ACLs,
    and record the resolved topic + hub URL in ``status.eventManagement``.

Subscribed side
    Resolve the referenced topic, create the consumer group, grant consumer ACLs
    and — in ``adapter`` transport mode — deploy a Kafka->REST listener configured
    with the component's callback URL. In ``native`` mode no adapter is deployed.

UC017 invariants enforced here
    * A ``PublishedNotification`` owns its topic; a ``SubscribedNotification``
      only references it and never deletes it.
    * Updates provision new topic/ACLs/credentials *before* revoking old ones.
    * Deletion order is: stop adapter -> revoke ACLs/credentials -> resolve topic
      ownership -> remove finalizer.
    * A subscriber whose topic does not exist yet is ``PendingTopic``, which is a
      non-blocking state rather than an error.
    * Absent ``spec.transport`` defaults to ``adapter``.

Reference: source/operators/TMFOP006-Event-Management/README.md, UC004, UC008.
"""

from __future__ import annotations

import asyncio
import logging
import os
import traceback
from typing import Any, Dict, List, Optional

import kopf
import kubernetes
from kubernetes.client.rest import ApiException

import adapter_manager
import kafka_user
import topic_naming
from kafka_utils import (
    EventBrokerConfig,
    KafkaAdmin,
    consumer_group_id,
    principal_for_component,
    topic_name,
)
from log_wrapper import LogWrapper
from utils import (
    any_reference_remains,
    callback_url,
    first_specification_url,
    hub_url,
    quick_get_comp_name,
    safe_get,
    transport_mode,
)

# --------------------------------------------------------------------------- #
# Constants                                                                   #
# --------------------------------------------------------------------------- #

GROUP = "oda.tmforum.org"
VERSION = "v1"
PUBLISHED_PLURAL = "publishednotifications"
SUBSCRIBED_PLURAL = "subscribednotifications"
COMPONENTS_PLURAL = "components"

HTTP_CONFLICT = 409
HTTP_NOT_FOUND = 404

STATUS_KEY = "eventManagement"
FINALIZER = "eventmanagement.oda.tmforum.org/finalizer"

# Status values
STATUS_ACTIVE = "Active"
STATUS_PENDING_TOPIC = "PendingTopic"
STATUS_ERROR = "Error"

# --------------------------------------------------------------------------- #
# Logging                                                                     #
# --------------------------------------------------------------------------- #

logging_level = os.environ.get("LOGGING", logging.INFO)
logger = logging.getLogger("EventManagementOperator")
logger.setLevel(int(logging_level))
logger.info("Logging set to %s", logging_level)
LogWrapper.set_defaultLogger(logger)

kafka_admin = KafkaAdmin(EventBrokerConfig())
kafka_users = kafka_user.KafkaUserManager()


# --------------------------------------------------------------------------- #
# Startup                                                                     #
# --------------------------------------------------------------------------- #


@kopf.on.startup()
async def configure(settings: kopf.OperatorSettings, **_):
    """Operator-wide settings.

    ``watching.server_timeout`` recovers from broken watch connections. Peering
    priority 120 keeps this operator above the dependentapi operator (110) and
    well below secretsmanagement (200), which must always stay responsive for
    admission.
    """
    settings.watching.server_timeout = 1 * 60
    settings.peering.name = os.environ.get("KOPF_PEERING_NAME", "eventmanagement")
    settings.peering.priority = int(os.environ.get("KOPF_PEERING_PRIORITY", "120"))
    settings.persistence.finalizer = FINALIZER
    settings.execution.max_workers = int(os.environ.get("KOPF_MAX_WORKERS", "20"))

    logw = LogWrapper(handler_name="configure", function_name="configure")
    logw.info(
        "startup",
        f"bootstrap={kafka_admin.config.bootstrap_servers} "
        f"protocol={kafka_admin.config.security_protocol} "
        f"schemaRegistry={kafka_admin.config.schema_registry_url or 'not-configured'} "
        f"topicDeleteAllowed={kafka_admin.config.allow_topic_delete}",
    )


# --------------------------------------------------------------------------- #
# Health probe                                                                #
# --------------------------------------------------------------------------- #


@kopf.on.probe(id="kafka-check")
def check_kafka_connection(**_) -> str:
    """Liveness probe - verifies the broker is reachable."""
    return kafka_admin.health_check()


@kopf.on.probe(id="schema-registry-check")
def check_schema_registry(**_) -> str:
    return kafka_admin.schema_registry_health()


# --------------------------------------------------------------------------- #
# Helpers                                                                     #
# --------------------------------------------------------------------------- #


def _custom_objects_api() -> kubernetes.client.CustomObjectsApi:
    return kubernetes.client.CustomObjectsApi()


def _resolve_component_name(body: Dict, spec: Dict, name: str) -> str:
    """Component that owns this notification resource.

    Prefer the Canvas label set by the Component operator; fall back to the
    ownerReference name, then to the resource name itself.
    """
    component_name = quick_get_comp_name(body)
    if component_name:
        return component_name
    owner_refs = safe_get([], body, "metadata", "ownerReferences") or []
    for ref in owner_refs:
        if ref.get("kind") == "Component":
            return ref.get("name")
    return spec.get("component") or name


def _resolve_topic(spec: Dict, component_name: str, event_name: str) -> str:
    """Resolve the Kafka topic for a notification.

    Order: explicit ``spec.topic`` (used as written, letter case kept), then
    ``spec.topicDomain`` [+ ``topicKind``, ``topicEvent``] built as
    ``<prefix>.<domain>[.<kind>].<event>``, then the legacy derived name
    ``oda.<component>.<event>`` (lower-cased, unchanged).

    Determinism is what allows a subscriber to find a publisher's topic without
    any shared registry. A name Kafka would reject cannot be fixed by retrying,
    so it is a PermanentError. See topic_naming.py.
    """
    try:
        topic, warnings = topic_naming.resolve_topic(
            spec,
            component_name,
            event_name,
            kafka_admin.config.topic_prefix,
            lambda: topic_name(kafka_admin.config, component_name, event_name),
        )
    except ValueError as exc:
        raise kopf.PermanentError(str(exc)) from exc
    for warning in warnings:
        logger.warning(warning)
    return topic


def _publisher_component(spec: Dict, subscriber_component: str) -> str:
    """Which component publishes the topic this subscription wants.

    ``spec.publisher`` / ``spec.publishedBy`` name the producing component. If
    neither is set the subscription is assumed to be intra-component.
    """
    return (
        spec.get("publisher")
        or spec.get("publishedBy")
        or safe_get(None, spec, "eventBroker", "publisher")
        or subscriber_component
    )


async def _list_notifications(plural: str) -> List[Dict]:
    """All notification resources of one kind, cluster-wide.

    Used for topic-ownership resolution during deletion.
    """
    api = _custom_objects_api()
    try:
        response = await asyncio.to_thread(
            api.list_cluster_custom_object, GROUP, VERSION, plural
        )
    except ApiException as exc:
        raise kopf.TemporaryError(f"cannot list {plural}: {exc}", delay=30) from exc
    return response.get("items", [])


async def _patch_status(
    plural: str, namespace: str, name: str, status_payload: Dict[str, Any]
) -> None:
    """Patch the resource's status sub-resource.

    Kopf writes handler return values to ``status.<handler_id>`` automatically;
    this helper is for the flat ``status.uid`` / ``status.status`` / ``status.error``
    fields that the CRD's ``additionalPrinterColumns`` display.
    """
    api = _custom_objects_api()
    try:
        await asyncio.to_thread(
            api.patch_namespaced_custom_object_status,
            GROUP,
            VERSION,
            namespace,
            plural,
            name,
            {"status": status_payload},
        )
    except ApiException as exc:
        if exc.status == HTTP_NOT_FOUND:
            return
        raise kopf.TemporaryError(
            f"cannot patch status of {name}: {exc}", delay=30
        ) from exc


async def _sync_component_user(
    component: str,
    include: Optional["kafka_user.ComponentAccess"] = None,
    exclude_uid: Optional[str] = None,
) -> str:
    """Bring the component's Strimzi ``KafkaUser`` in line with all its notifications.

    Strimzi removes the ACLs of any principal without a ``KafkaUser``, so the ACLs granted
    through the admin API above are only lasting when this object exists. A no-op unless
    KAFKA_USER_MANAGEMENT is enabled.
    """
    if not kafka_users.enabled:
        return "skipped"
    published = await _list_notifications(PUBLISHED_PLURAL)
    subscribed = await _list_notifications(SUBSCRIBED_PLURAL)

    def owner_of(item: Dict) -> str:
        return _resolve_component_name(item, item.get("spec", {}) or {}, item["metadata"]["name"])

    def published_topic(item: Dict) -> str:
        spec = item.get("spec", {}) or {}
        return _resolve_topic(spec, owner_of(item), spec.get("name") or item["metadata"]["name"])

    def subscribed_topic_group(item: Dict):
        spec = item.get("spec", {}) or {}
        name = item["metadata"]["name"]
        mine = owner_of(item)
        topic = _resolve_topic(spec, _publisher_component(spec, mine), spec.get("name") or name)
        recorded = safe_get(None, item, "status", STATUS_KEY, "consumerGroup")
        return topic, recorded or consumer_group_id(mine, name)

    access = kafka_user.collect_access(
        component,
        published,
        subscribed,
        owner_of=owner_of,
        published_topic=published_topic,
        subscribed_topic_group=subscribed_topic_group,
        exclude_uid=exclude_uid,
        include=include,
    )
    try:
        return await asyncio.to_thread(kafka_users.sync, component, access)
    except (ApiException, ValueError) as exc:
        raise kopf.TemporaryError(f"cannot sync KafkaUser for {component}: {exc}", delay=30) from exc


# --------------------------------------------------------------------------- #
# PublishedNotification - create / resume / update                            #
# --------------------------------------------------------------------------- #


@kopf.on.resume(GROUP, VERSION, PUBLISHED_PLURAL, id=STATUS_KEY, retries=5)
@kopf.on.create(GROUP, VERSION, PUBLISHED_PLURAL, id=STATUS_KEY, retries=5)
@kopf.on.update(GROUP, VERSION, PUBLISHED_PLURAL, id=STATUS_KEY, retries=5)
async def publishedNotification(
    meta, spec, status, body, namespace, name, **kwargs
) -> Dict[str, Any]:
    """Reconcile a PublishedNotification into a Kafka topic + producer ACLs.

    Non-disruptive update: the new topic and ACLs are provisioned first; only
    once they are confirmed in place is a superseded topic's producer ACL revoked
    (the old topic itself is never deleted here).
    """
    component_name = _resolve_component_name(body, spec, name)
    event_name = spec.get("name") or name
    logw = LogWrapper(
        handler_name="publishedNotification", function_name="publishedNotification"
    )
    logw.set(component_name=component_name, resource_name=name)

    topic = _resolve_topic(spec, component_name, event_name)
    principal = principal_for_component(component_name)
    previous = safe_get({}, status, STATUS_KEY) or {}
    previous_topic = previous.get("topic")

    logw.info("reconcile", f"topic={topic} principal={principal}")

    try:
        # 1. Provision the new topic (idempotent).
        topic_info = await asyncio.to_thread(
            kafka_admin.create_topic,
            topic,
            spec.get("partitions"),
            spec.get("replicationFactor"),
            spec.get("retentionMs"),
            spec.get("topicConfigs"),
        )

        # 1b. Kafka topic names are case-sensitive. Warn (never fail) if a topic
        #     that differs only in letter case already exists.
        try:
            existing_topics = await asyncio.to_thread(kafka_admin.client.list_topics)
            clashes = topic_naming.case_variants(topic, existing_topics)
            if clashes:
                logw.warning(
                    "topic-case",
                    f"topic {topic} differs only in letter case from existing "
                    f"{clashes}; Kafka treats them as different topics",
                )
        except Exception as exc:  # noqa: BLE001 - advisory check, must never break reconcile
            logw.warning("topic-case", f"case check skipped: {exc}")

        # 2. Register the schema, if the component supplied one and a registry
        #    is configured. A missing registry is a valid configuration.
        schema_id = None
        inline_schema = spec.get("schema")
        if inline_schema:
            schema_id = await asyncio.to_thread(
                kafka_admin.register_schema,
                f"{topic}-value",
                inline_schema,
                spec.get("schemaType", "AVRO"),
            )

        # 3. Grant producer ACLs on the NEW topic before touching the old one.
        granted = await asyncio.to_thread(
            kafka_admin.grant_producer_acls, principal, topic
        )

        # 3b. Keep the permissions: a KafkaUser stops Strimzi from removing the ACLs.
        user_result = await _sync_component_user(
            component_name, include=kafka_user.ComponentAccess(published={topic})
        )
        logw.info("kafkauser", f"KafkaUser {kafka_user.user_name(component_name)}: {user_result}")

        # 4. Only now, revoke the producer ACL on a superseded topic.
        if previous_topic and previous_topic != topic:
            revoked = await asyncio.to_thread(
                kafka_admin.revoke_acls, principal, previous_topic, None
            )
            logw.info(
                "migrate",
                f"topic changed {previous_topic} -> {topic}; "
                f"revoked {revoked} stale producer ACL binding(s). "
                f"Old topic retained - deletion is a separate, policy-gated action.",
            )

    except Exception as exc:  # noqa: BLE001 - re-raised as a kopf error below
        logw.error("reconcile-failed", f"{exc}: {traceback.format_exc()}")
        await _patch_status(
            PUBLISHED_PLURAL,
            namespace,
            name,
            {"status": STATUS_ERROR, "error": str(exc)},
        )
        raise kopf.TemporaryError(
            f"cannot configure publisher {name}: {exc}", delay=60
        ) from exc

    result = {
        "topic": topic,
        "role": "publisher",
        "owner": True,
        "principal": principal,
        "acls": granted,
        "partitions": topic_info.get("partitions"),
        "replicationFactor": topic_info.get("replicationFactor"),
        "bootstrapServers": kafka_admin.config.bootstrap_servers,
        "schemaId": schema_id,
        "specificationURL": first_specification_url(spec),
        "hubURL": hub_url(spec, namespace),
        "apiType": topic_naming.api_type_of(spec),
        "state": STATUS_ACTIVE,
    }

    await _patch_status(
        PUBLISHED_PLURAL,
        namespace,
        name,
        {"uid": meta.get("uid"), "status": STATUS_ACTIVE, "error": ""},
    )
    logw.info("reconciled", f"publisher active on topic {topic}")
    return result


# --------------------------------------------------------------------------- #
# PublishedNotification - delete                                              #
# --------------------------------------------------------------------------- #


@kopf.on.delete(GROUP, VERSION, PUBLISHED_PLURAL, retries=5)
async def publishedNotificationDelete(meta, spec, status, body, namespace, name, **_):
    """Tear down a publisher.

    Order (UC017): revoke producer ACLs -> resolve topic ownership -> delete the
    topic only if nothing else references it *and* policy allows it -> let kopf
    remove the finalizer.
    """
    component_name = _resolve_component_name(body, spec, name)
    event_name = spec.get("name") or name
    logw = LogWrapper(
        handler_name="publishedNotificationDelete",
        function_name="publishedNotificationDelete",
    )
    logw.set(component_name=component_name, resource_name=name)

    topic = safe_get(None, status, STATUS_KEY, "topic") or _resolve_topic(
        spec, component_name, event_name
    )
    principal = principal_for_component(component_name)

    # 1. Revoke this publisher's producer ACLs.
    try:
        revoked = await asyncio.to_thread(kafka_admin.revoke_acls, principal, topic, None)
        logw.info("cleanup", f"revoked {revoked} producer ACL binding(s) on {topic}")
        user_result = await _sync_component_user(component_name, exclude_uid=meta.get("uid"))
        logw.info("cleanup", f"KafkaUser {kafka_user.user_name(component_name)}: {user_result}")
    except kopf.TemporaryError:
        raise
    except Exception as exc:  # noqa: BLE001
        logw.error("cleanup-failed", f"{exc}: {traceback.format_exc()}")
        raise kopf.TemporaryError(
            f"cannot revoke ACLs for {name}: {exc}", delay=30
        ) from exc

    # 2. Resolve topic ownership across BOTH kinds before considering deletion.
    my_uid = meta.get("uid")
    publishers = await _list_notifications(PUBLISHED_PLURAL)
    subscribers = await _list_notifications(SUBSCRIBED_PLURAL)

    still_referenced = any_reference_remains(
        publishers, topic, exclude_uid=my_uid
    ) or any_reference_remains(subscribers, topic, exclude_uid=my_uid)

    if still_referenced:
        logw.info(
            "cleanup",
            f"topic {topic} still referenced by another publisher/subscriber - retained",
        )
        return {"topic": topic, "topicDeleted": False, "reason": "StillReferenced"}

    # 3. Nothing else references it. Delete only if policy permits.
    deleted = await asyncio.to_thread(kafka_admin.delete_topic, topic)
    logw.info(
        "cleanup",
        f"topic {topic} unreferenced; deleted={deleted} "
        f"(policy allow_topic_delete={kafka_admin.config.allow_topic_delete})",
    )
    return {
        "topic": topic,
        "topicDeleted": deleted,
        "reason": "Unreferenced" if deleted else "DeleteDisabledByPolicy",
    }


# --------------------------------------------------------------------------- #
# SubscribedNotification - create / resume / update                           #
# --------------------------------------------------------------------------- #


@kopf.on.resume(GROUP, VERSION, SUBSCRIBED_PLURAL, id=STATUS_KEY, retries=5)
@kopf.on.create(GROUP, VERSION, SUBSCRIBED_PLURAL, id=STATUS_KEY, retries=5)
@kopf.on.update(GROUP, VERSION, SUBSCRIBED_PLURAL, id=STATUS_KEY, retries=5)
async def subscribedNotification(
    meta, spec, status, body, namespace, name, **kwargs
) -> Dict[str, Any]:
    """Reconcile a SubscribedNotification.

    Never creates or deletes the topic - a subscriber only ever references one.
    If the topic is absent the resource settles in ``PendingTopic``, which is a
    normal, non-blocking state: the timer handler re-checks periodically and the
    subscription activates as soon as the publisher appears.
    """
    subscriber_component = _resolve_component_name(body, spec, name)
    event_name = spec.get("name") or name
    publisher_component = _publisher_component(spec, subscriber_component)

    logw = LogWrapper(
        handler_name="subscribedNotification", function_name="subscribedNotification"
    )
    logw.set(component_name=subscriber_component, resource_name=name)

    topic = _resolve_topic(spec, publisher_component, event_name)
    group_id = consumer_group_id(subscriber_component, name)
    principal = principal_for_component(subscriber_component)
    mode = transport_mode(spec)
    target = callback_url(spec, namespace)

    previous = safe_get({}, status, STATUS_KEY) or {}
    previous_topic = previous.get("topic")
    previous_group = previous.get("consumerGroup")
    previous_mode = previous.get("transport")

    logw.info(
        "reconcile",
        f"topic={topic} group={group_id} transport={mode} callback={target}",
    )

    # PendingTopic: non-blocking, not an error.
    try:
        exists = await asyncio.to_thread(kafka_admin.topic_exists, topic)
    except Exception as exc:  # noqa: BLE001
        logw.error("broker-unreachable", f"{exc}: {traceback.format_exc()}")
        raise kopf.TemporaryError(f"broker unreachable: {exc}", delay=60) from exc

    if not exists:
        logw.info(
            "pending",
            f"topic {topic} does not exist yet - subscription parked in "
            f"{STATUS_PENDING_TOPIC}; the reconcile timer will retry",
        )
        await _patch_status(
            SUBSCRIBED_PLURAL,
            namespace,
            name,
            {"uid": meta.get("uid"), "status": STATUS_PENDING_TOPIC, "error": ""},
        )
        return {
            "topic": topic,
            "role": "subscriber",
            "owner": False,
            "consumerGroup": group_id,
            "transport": mode,
            "state": STATUS_PENDING_TOPIC,
        }

    if mode == "adapter" and not target:
        # Genuinely unrecoverable without a spec change - do not retry forever.
        raise kopf.PermanentError(
            "adapter transport requires spec.implementation (service name); "
            "set spec.transport=native if the component consumes Kafka directly"
        )

    adapter_info: Optional[Dict[str, Any]] = None
    try:
        # 1. Grant consumer ACLs on the NEW topic/group first.
        granted = await asyncio.to_thread(
            kafka_admin.grant_consumer_acls, principal, topic, group_id
        )

        # 1b. Keep the permissions: a KafkaUser stops Strimzi from removing the ACLs.
        user_result = await _sync_component_user(
            subscriber_component,
            include=kafka_user.ComponentAccess(subscribed={(topic, group_id)}),
        )
        logw.info("kafkauser", f"KafkaUser {kafka_user.user_name(subscriber_component)}: {user_result}")

        # 2. Deploy or update the adapter (adapter mode only).
        adapter_workload = adapter_manager.adapter_name(name)
        if mode == "adapter":
            deployment_body = adapter_manager.build_adapter_deployment(
                name=adapter_workload,
                namespace=namespace,
                component_name=subscriber_component,
                subscription_name=name,
                bootstrap_servers=kafka_admin.config.bootstrap_servers,
                topic=topic,
                group_id=group_id,
                target_url=target,
                query=spec.get("query"),
                security_protocol=kafka_admin.config.security_protocol,
                sasl_mechanism=kafka_admin.config.sasl_mechanism,
                schema_registry_url=kafka_admin.config.schema_registry_url,
            )
            service_body = adapter_manager.build_adapter_service(
                adapter_workload, namespace, subscriber_component
            )
            adapter_info = await asyncio.to_thread(
                adapter_manager.apply_adapter,
                logw,
                adapter_workload,
                namespace,
                deployment_body,
                service_body,
            )
        elif previous_mode == "adapter":
            # Transport switched adapter -> native: the new native path is
            # already authorised (step 1), so it is safe to stop the bridge.
            await asyncio.to_thread(
                adapter_manager.stop_adapter, logw, adapter_workload, namespace
            )
            logw.info("migrate", "transport switched to native; adapter removed")

        # 3. Revoke stale ACLs only after the new path is live.
        if previous_topic and (
            previous_topic != topic or previous_group != group_id
        ):
            revoked = await asyncio.to_thread(
                kafka_admin.revoke_acls, principal, previous_topic, previous_group
            )
            logw.info(
                "migrate",
                f"subscription moved {previous_topic}/{previous_group} -> "
                f"{topic}/{group_id}; revoked {revoked} stale binding(s). "
                f"Topic {previous_topic} untouched - subscribers never delete topics.",
            )

    except (kopf.PermanentError, kopf.TemporaryError):
        raise
    except Exception as exc:  # noqa: BLE001
        logw.error("reconcile-failed", f"{exc}: {traceback.format_exc()}")
        await _patch_status(
            SUBSCRIBED_PLURAL,
            namespace,
            name,
            {"status": STATUS_ERROR, "error": str(exc)},
        )
        raise kopf.TemporaryError(
            f"cannot configure subscriber {name}: {exc}", delay=60
        ) from exc

    await _patch_status(
        SUBSCRIBED_PLURAL,
        namespace,
        name,
        {"uid": meta.get("uid"), "status": STATUS_ACTIVE, "error": ""},
    )
    logw.info("reconciled", f"subscriber active on topic {topic} via {mode}")

    return {
        "topic": topic,
        "role": "subscriber",
        "owner": False,
        "principal": principal,
        "consumerGroup": group_id,
        "acls": granted,
        "transport": mode,
        "callbackURL": target,
        "adapter": adapter_info,
        "apiType": topic_naming.api_type_of(spec),
        "bootstrapServers": kafka_admin.config.bootstrap_servers,
        "specificationURL": first_specification_url(spec),
        "state": STATUS_ACTIVE,
    }


# --------------------------------------------------------------------------- #
# SubscribedNotification - delete                                             #
# --------------------------------------------------------------------------- #


@kopf.on.delete(GROUP, VERSION, SUBSCRIBED_PLURAL, retries=5)
async def subscribedNotificationDelete(meta, spec, status, body, namespace, name, **_):
    """Tear down a subscriber.

    Order (UC017): stop the adapter -> revoke ACLs/credentials -> finish. The
    topic is *never* deleted here, regardless of reference count.
    """
    subscriber_component = _resolve_component_name(body, spec, name)
    event_name = spec.get("name") or name
    publisher_component = _publisher_component(spec, subscriber_component)

    logw = LogWrapper(
        handler_name="subscribedNotificationDelete",
        function_name="subscribedNotificationDelete",
    )
    logw.set(component_name=subscriber_component, resource_name=name)

    topic = safe_get(None, status, STATUS_KEY, "topic") or _resolve_topic(
        spec, publisher_component, event_name
    )
    group_id = safe_get(None, status, STATUS_KEY, "consumerGroup") or consumer_group_id(
        subscriber_component, name
    )
    principal = principal_for_component(subscriber_component)
    adapter_workload = adapter_manager.adapter_name(name)

    # 1. Stop the adapter FIRST - revoking credentials from a live consumer
    #    would otherwise produce a burst of authorization failures.
    stopped = await asyncio.to_thread(
        adapter_manager.stop_adapter, logw, adapter_workload, namespace
    )
    logw.info("cleanup", f"adapter {adapter_workload} stopped={stopped}")

    # 2. Now revoke ACLs / credentials.
    try:
        revoked = await asyncio.to_thread(
            kafka_admin.revoke_acls, principal, topic, group_id
        )
        logw.info("cleanup", f"revoked {revoked} consumer ACL binding(s)")
        user_result = await _sync_component_user(
            subscriber_component, exclude_uid=meta.get("uid")
        )
        logw.info("cleanup", f"KafkaUser {kafka_user.user_name(subscriber_component)}: {user_result}")
    except kopf.TemporaryError:
        raise
    except Exception as exc:  # noqa: BLE001
        logw.error("cleanup-failed", f"{exc}: {traceback.format_exc()}")
        raise kopf.TemporaryError(
            f"cannot revoke ACLs for {name}: {exc}", delay=30
        ) from exc

    # 3. Explicitly do NOT delete the topic.
    logw.info(
        "cleanup",
        f"topic {topic} left in place - a SubscribedNotification never owns a topic",
    )
    return {
        "topic": topic,
        "topicDeleted": False,
        "adapterStopped": stopped,
        "aclsRevoked": revoked,
    }


# --------------------------------------------------------------------------- #
# Periodic reconciliation                                                     #
# --------------------------------------------------------------------------- #


@kopf.timer(
    GROUP,
    VERSION,
    SUBSCRIBED_PLURAL,
    interval=float(os.environ.get("RECONCILE_INTERVAL", "300")),
    idle=30.0,
)
async def reconcileSubscription(spec, status, body, namespace, name, **_):
    """Re-check ``PendingTopic`` subscriptions and adapter health.

    This is what turns a missing topic from an error into a wait: the publisher
    can be deployed minutes or hours after the subscriber, and the subscription
    activates on its own once the topic shows up.
    """
    subscriber_component = _resolve_component_name(body, spec, name)
    logw = LogWrapper(
        handler_name="reconcileSubscription", function_name="reconcileSubscription"
    )
    logw.set(component_name=subscriber_component, resource_name=name)

    current_state = safe_get(None, status, "status")
    recorded = safe_get({}, status, STATUS_KEY) or {}
    topic = recorded.get("topic")
    mode = recorded.get("transport") or transport_mode(spec)

    if current_state == STATUS_PENDING_TOPIC and topic:
        try:
            if await asyncio.to_thread(kafka_admin.topic_exists, topic):
                logw.info(
                    "reconcile",
                    f"topic {topic} has appeared - nudging resource to re-reconcile",
                )
                # Touching an annotation re-triggers the update handler, which
                # performs the full (idempotent) activation.
                api = _custom_objects_api()
                await asyncio.to_thread(
                    api.patch_namespaced_custom_object,
                    GROUP,
                    VERSION,
                    namespace,
                    SUBSCRIBED_PLURAL,
                    name,
                    {
                        "metadata": {
                            "annotations": {
                                "eventmanagement.oda.tmforum.org/topic-seen": topic
                            }
                        }
                    },
                )
                return {"pendingResolved": True, "topic": topic}
        except ApiException as exc:
            raise kopf.TemporaryError(f"cannot nudge {name}: {exc}", delay=30) from exc
        except Exception as exc:  # noqa: BLE001
            logw.warning("reconcile", f"topic existence check failed: {exc}")
            raise kopf.TemporaryError(str(exc), delay=60) from exc
        return {"pendingResolved": False, "topic": topic}

    if mode == "adapter" and current_state == STATUS_ACTIVE:
        adapter_workload = adapter_manager.adapter_name(name)
        healthy = await asyncio.to_thread(
            adapter_manager.adapter_ready, adapter_workload, namespace
        )
        if not healthy:
            logw.warning(
                "reconcile",
                f"adapter {adapter_workload} has no ready replica - "
                f"will be re-applied on the next update event",
            )
        return {"adapterReady": healthy, "topic": topic}

    return {"topic": topic, "transport": mode, "state": current_state}


# --------------------------------------------------------------------------- #
# Component status rollup                                                     #
# --------------------------------------------------------------------------- #


def _event_status_changed(status, **_) -> bool:
    """Filter: only act when the notification status actually moved."""
    return safe_get(None, status, "status") in (
        STATUS_ACTIVE,
        STATUS_PENDING_TOPIC,
        STATUS_ERROR,
    )


@kopf.on.field(
    GROUP,
    VERSION,
    PUBLISHED_PLURAL,
    field="status.status",
    id="eventStatusRollup",
    when=_event_status_changed,
    retries=5,
)
@kopf.on.field(
    GROUP,
    VERSION,
    SUBSCRIBED_PLURAL,
    field="status.status",
    id="eventStatusRollup",
    when=_event_status_changed,
    retries=5,
)
async def rollupEventStatus(body, status, namespace, name, new, **_):
    """Propagate notification status up to the parent Component.

    Writes ``status.eventManagement.<resourceName>`` on the Component so an
    operator can see the whole event posture of a component in one place, the
    same way the Component operator rolls up ExposedAPI status.
    """
    component_name = quick_get_comp_name(body)
    if not component_name:
        return

    logw = LogWrapper(handler_name="rollupEventStatus", function_name="rollupEventStatus")
    logw.set(component_name=component_name, resource_name=name)

    api = _custom_objects_api()
    patch = {
        "status": {
            STATUS_KEY: {
                name: {
                    "state": new,
                    "topic": safe_get(None, status, STATUS_KEY, "topic"),
                    "role": safe_get(None, status, STATUS_KEY, "role"),
                }
            }
        }
    }
    try:
        await asyncio.to_thread(
            api.patch_namespaced_custom_object_status,
            GROUP,
            VERSION,
            namespace,
            COMPONENTS_PLURAL,
            component_name,
            patch,
        )
        logw.info("rollup", f"component {component_name} updated with state {new}")
    except ApiException as exc:
        if exc.status == HTTP_NOT_FOUND:
            logw.info("rollup", f"component {component_name} not found - skipping")
            return
        raise kopf.TemporaryError(
            f"cannot roll up status to component {component_name}: {exc}", delay=30
        ) from exc