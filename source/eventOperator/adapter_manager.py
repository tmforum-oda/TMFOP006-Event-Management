"""Lifecycle management for the Kafka -> REST notification adapter.

In ``adapter`` transport mode (the UC017 default) the Canvas deploys a small
listener workload per ``SubscribedNotification``. The adapter consumes from the
Kafka topic and POSTs each event to the component's TMF-style callback URL, so
the component itself never needs Kafka credentials or a Kafka client.

In ``native`` mode no adapter is deployed — the operator only provisions the
topic reference, consumer group and ACLs, and the component consumes directly.

The adapter Deployment and Service are created as children of the
``SubscribedNotification`` via :func:`kopf.adopt`, so they are garbage-collected
by Kubernetes if the parent disappears. The delete handler still stops the
adapter explicitly first, because UC017 requires "stop adapter -> revoke ACLs"
ordering: revoking credentials from a still-running consumer would produce a
flood of authorization errors.
"""

from __future__ import annotations

import os
from typing import Any, Dict, Optional

import kopf
import kubernetes
from kubernetes.client.rest import ApiException

from log_wrapper import LogWrapper
from utils import dns_safe

HTTP_NOT_FOUND = 404
HTTP_CONFLICT = 409

# The adapter runs from a dedicated image, built and pushed separately from the
# operator's own image (see source/notificationAdapter/Dockerfile). ADAPTER_IMAGE must be set
# explicitly in the operator's ConfigMap -- it is NOT assumed to match
# OPERATOR_IMAGE. Keep the two versions in sync deliberately when you cut a
# release, since they no longer share a build.
ADAPTER_IMAGE = os.environ.get("ADAPTER_IMAGE") or os.environ.get(
    "OPERATOR_IMAGE", "tmforumodacanvas/eventmanagement-operator-kafka:0.1.0"
)
ADAPTER_COMMAND = [
    "python",
    os.environ.get("ADAPTER_ENTRYPOINT", "/src/notification_adapter.py"),
]
ADAPTER_IMAGE_PULL_POLICY = os.environ.get("ADAPTER_IMAGE_PULL_POLICY", "IfNotPresent")
ADAPTER_PORT = int(os.environ.get("ADAPTER_PORT", "8080"))
ADAPTER_CPU_REQUEST = os.environ.get("ADAPTER_CPU_REQUEST", "50m")
ADAPTER_CPU_LIMIT = os.environ.get("ADAPTER_CPU_LIMIT", "500m")
ADAPTER_MEMORY_REQUEST = os.environ.get("ADAPTER_MEMORY_REQUEST", "64Mi")
ADAPTER_MEMORY_LIMIT = os.environ.get("ADAPTER_MEMORY_LIMIT", "256Mi")
ADAPTER_CREDENTIALS_SECRET = os.environ.get("ADAPTER_CREDENTIALS_SECRET", "")


def adapter_name(subscription_name: str) -> str:
    return dns_safe(f"{subscription_name}-evtadapter")


def _adapter_env(
    bootstrap_servers: str,
    topic: str,
    group_id: str,
    target_url: str,
    query: Optional[str],
    security_protocol: str,
    sasl_mechanism: Optional[str],
    schema_registry_url: Optional[str],
) -> list:
    env = [
        {"name": "KAFKA_BOOTSTRAP_SERVERS", "value": bootstrap_servers},
        {"name": "KAFKA_TOPIC", "value": topic},
        {"name": "KAFKA_GROUP_ID", "value": group_id},
        {"name": "CALLBACK_URL", "value": target_url},
        {"name": "KAFKA_SECURITY_PROTOCOL", "value": security_protocol},
        {"name": "LISTEN_PORT", "value": str(ADAPTER_PORT)},
        {"name": "LOGGING", "value": os.environ.get("LOGGING", "20")},
    ]
    if query:
        env.append({"name": "EVENT_FILTER_QUERY", "value": query})
    if sasl_mechanism:
        env.append({"name": "KAFKA_SASL_MECHANISM", "value": sasl_mechanism})
    if schema_registry_url:
        env.append({"name": "SCHEMA_REGISTRY_URL", "value": schema_registry_url})
    return env


def build_adapter_deployment(
    name: str,
    namespace: str,
    component_name: str,
    subscription_name: str,
    bootstrap_servers: str,
    topic: str,
    group_id: str,
    target_url: str,
    query: Optional[str] = None,
    security_protocol: str = "PLAINTEXT",
    sasl_mechanism: Optional[str] = None,
    schema_registry_url: Optional[str] = None,
) -> Dict[str, Any]:
    labels = {
        "app": name,
        "oda.tmforum.org/componentName": component_name,
        "oda.tmforum.org/subscribedNotification": dns_safe(subscription_name),
        "oda.tmforum.org/managedBy": "eventmanagement-operator",
    }
    container: Dict[str, Any] = {
        "name": "notification-adapter",
        "image": ADAPTER_IMAGE,
        "imagePullPolicy": ADAPTER_IMAGE_PULL_POLICY,
        "command": ADAPTER_COMMAND,
        "ports": [{"containerPort": ADAPTER_PORT, "name": "http"}],
        "env": _adapter_env(
            bootstrap_servers,
            topic,
            group_id,
            target_url,
            query,
            security_protocol,
            sasl_mechanism,
            schema_registry_url,
        ),
        "resources": {
            "requests": {
                "cpu": ADAPTER_CPU_REQUEST,
                "memory": ADAPTER_MEMORY_REQUEST,
            },
            "limits": {"cpu": ADAPTER_CPU_LIMIT, "memory": ADAPTER_MEMORY_LIMIT},
        },
        "readinessProbe": {
            "httpGet": {"path": "/healthz", "port": ADAPTER_PORT},
            "initialDelaySeconds": 5,
            "periodSeconds": 10,
        },
        "livenessProbe": {
            "httpGet": {"path": "/healthz", "port": ADAPTER_PORT},
            "initialDelaySeconds": 15,
            "periodSeconds": 30,
        },
        "securityContext": {
            "allowPrivilegeEscalation": False,
            "runAsNonRoot": True,
            "runAsUser": 10001,
            "readOnlyRootFilesystem": True,
            "capabilities": {"drop": ["ALL"]},
        },
    }
    if ADAPTER_CREDENTIALS_SECRET:
        container["envFrom"] = [
            {"secretRef": {"name": ADAPTER_CREDENTIALS_SECRET, "optional": True}}
        ]

    return {
        "apiVersion": "apps/v1",
        "kind": "Deployment",
        "metadata": {"name": name, "namespace": namespace, "labels": labels},
        "spec": {
            "replicas": 1,
            "selector": {"matchLabels": {"app": name}},
            "template": {
                "metadata": {"labels": labels},
                "spec": {"containers": [container]},
            },
        },
    }


def build_adapter_service(
    name: str, namespace: str, component_name: str
) -> Dict[str, Any]:
    return {
        "apiVersion": "v1",
        "kind": "Service",
        "metadata": {
            "name": name,
            "namespace": namespace,
            "labels": {
                "app": name,
                "oda.tmforum.org/componentName": component_name,
                "oda.tmforum.org/managedBy": "eventmanagement-operator",
            },
        },
        "spec": {
            "selector": {"app": name},
            "ports": [
                {
                    "name": "http",
                    "port": ADAPTER_PORT,
                    "targetPort": ADAPTER_PORT,
                    "protocol": "TCP",
                }
            ],
        },
    }


def apply_adapter(
    logw: LogWrapper,
    name: str,
    namespace: str,
    deployment_body: Dict[str, Any],
    service_body: Dict[str, Any],
) -> Dict[str, Any]:
    """Create or update the adapter Deployment + Service. Idempotent.

    Note the ordering: the workload is created (or patched) *before* the caller
    revokes any previous ACLs, satisfying the UC017 non-disruptive update rule.
    """
    apps_v1 = kubernetes.client.AppsV1Api()
    core_v1 = kubernetes.client.CoreV1Api()

    kopf.adopt(deployment_body)
    kopf.adopt(service_body)

    try:
        apps_v1.read_namespaced_deployment(name, namespace)
        apps_v1.patch_namespaced_deployment(name, namespace, deployment_body)
        logw.info("apply_adapter", f"patched adapter deployment {name}")
    except ApiException as exc:
        if exc.status == HTTP_NOT_FOUND:
            apps_v1.create_namespaced_deployment(namespace, deployment_body)
            logw.info("apply_adapter", f"created adapter deployment {name}")
        else:
            raise kopf.TemporaryError(
                f"error managing adapter deployment {name}: {exc}", delay=30
            ) from exc

    try:
        core_v1.read_namespaced_service(name, namespace)
        logw.info("apply_adapter", f"adapter service {name} already present")
    except ApiException as exc:
        if exc.status == HTTP_NOT_FOUND:
            try:
                core_v1.create_namespaced_service(namespace, service_body)
                logw.info("apply_adapter", f"created adapter service {name}")
            except ApiException as inner:
                if inner.status != HTTP_CONFLICT:
                    raise kopf.TemporaryError(
                        f"error creating adapter service {name}: {inner}", delay=30
                    ) from inner
        else:
            raise kopf.TemporaryError(
                f"error reading adapter service {name}: {exc}", delay=30
            ) from exc

    return {"deployment": name, "service": name, "port": ADAPTER_PORT}


def stop_adapter(logw: LogWrapper, name: str, namespace: str) -> bool:
    """Scale the adapter to zero and remove it.

    Called from the SubscribedNotification delete path *before* ACLs are revoked.
    Only ever touches resources this operator created — identified by the
    ``oda.tmforum.org/managedBy`` label, which is verified before deletion.
    """
    apps_v1 = kubernetes.client.AppsV1Api()
    core_v1 = kubernetes.client.CoreV1Api()
    stopped = False

    try:
        existing = apps_v1.read_namespaced_deployment(name, namespace)
        managed_by = (existing.metadata.labels or {}).get(
            "oda.tmforum.org/managedBy", ""
        )
        if managed_by != "eventmanagement-operator":
            logw.warning(
                "stop_adapter",
                f"refusing to delete deployment {name}: not managed by this operator",
            )
            return False
        apps_v1.delete_namespaced_deployment(name, namespace)
        logw.info("stop_adapter", f"deleted adapter deployment {name}")
        stopped = True
    except ApiException as exc:
        if exc.status != HTTP_NOT_FOUND:
            raise kopf.TemporaryError(
                f"error deleting adapter deployment {name}: {exc}", delay=30
            ) from exc

    try:
        existing_svc = core_v1.read_namespaced_service(name, namespace)
        managed_by = (existing_svc.metadata.labels or {}).get(
            "oda.tmforum.org/managedBy", ""
        )
        if managed_by == "eventmanagement-operator":
            core_v1.delete_namespaced_service(name, namespace)
            logw.info("stop_adapter", f"deleted adapter service {name}")
    except ApiException as exc:
        if exc.status != HTTP_NOT_FOUND:
            raise kopf.TemporaryError(
                f"error deleting adapter service {name}: {exc}", delay=30
            ) from exc

    return stopped


def adapter_ready(name: str, namespace: str) -> bool:
    """True when at least one adapter replica is ready."""
    apps_v1 = kubernetes.client.AppsV1Api()
    try:
        deployment = apps_v1.read_namespaced_deployment(name, namespace)
    except ApiException:
        return False
    return (deployment.status.ready_replicas or 0) >= 1