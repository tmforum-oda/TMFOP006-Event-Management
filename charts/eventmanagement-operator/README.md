# eventmanagement-operator

Helm chart for the TMFOP006 Event Management Operator: watches
`oda.tmforum.org/v1` `PublishedNotification` and `SubscribedNotification`
custom resources and reconciles them into real Kafka topics, producer /
consumer ACLs, and (in adapter transport mode) notification-adapter
Deployments.

Source code: [`../../source/eventOperator/`](../../source/eventOperator/).
CRDs this operator requires: `PublishedNotification` and `SubscribedNotification`, as installed by the Canvas `oda-crds` chart.

## Prerequisites

1. A reachable Kafka cluster (bootstrap servers configurable via
   `kafka.bootstrapServers`).
2. The `PublishedNotification` and `SubscribedNotification` CRDs installed on
   the cluster by the Canvas `oda-crds` chart. This repository does not ship its
   own copies. The `apiType` values `asyncapi`, `a2a` and `mcp` are not in the
   upstream CRDs yet; they need a change to `oda-canvas` (`charts/oda-crds`)
   before the demo manifests apply cleanly on a stock Canvas.
3. (Optional) A pre-existing Secret with SASL credentials for the
   operator's own Kafka admin principal, if the broker requires
   authentication. Not created by this chart.

## Build the images

Both images are built from this repository and pushed to a public registry.
Build for the architecture of your cluster nodes (`linux/amd64` shown):

```sh
# operator
docker build --platform linux/amd64 -t <registry>/eventmanagement-operator:0.2.1 \
  --build-arg GIT_COMMIT_SHA=$(git rev-parse --short HEAD) ../../source/eventOperator
# notification adapter (used only by SubscribedNotifications in adapter transport mode)
docker build --platform linux/amd64 -t <registry>/eventmanagement-notification-adapter:0.2.0 \
  ../../source/notificationAdapter
docker push <registry>/eventmanagement-operator:0.2.1
docker push <registry>/eventmanagement-notification-adapter:0.2.0
```

Then install with `--set operator.image=<registry>/eventmanagement-operator:0.2.1
--set adapter.image=<registry>/eventmanagement-notification-adapter:0.2.0`.

## Install

```sh
helm install eventmgmt-op . -n canvas --create-namespace
```

Override any value with `--set` or `-f values.yaml`, for example to point at
a different Kafka broker or a different Component namespace:

```sh
helm install eventmgmt-op . -n canvas --create-namespace \
  --set kafka.bootstrapServers=my-kafka-bootstrap.kafka.svc.cluster.local:9092 \
  --set operator.componentNamespace=my-components-namespace
```

## Upgrade

```sh
helm upgrade eventmgmt-op . -n canvas
```

## What this chart creates

- `ServiceAccount` -- `templates/serviceaccount.yaml`
- `ClusterRole` + `ClusterRoleBinding` -- `templates/rbac.yaml` (cluster-wide,
  since the operator watches ODA resources across Component namespaces, not
  just its own)
- `ConfigMap` (Kafka connection settings, adapter defaults, kopf tuning) --
  `templates/configmap.yaml`
- `Deployment` running the operator under `kopf run` -- `templates/deployment.yaml`

It does **not** create the Secret referenced by `kafka.existingSecretName`
(if set) -- that must already exist in the operator's namespace.

## Values

| Key | Default | Description |
|---|---|---|
| `operator.image` | `lekhamaru/eventmanagement-operator:0.2.1` | Operator container image |
| `operator.namespace` | `canvas` | Namespace the operator itself runs in |
| `operator.componentNamespace` | `components` | Namespace of ODA Components whose notifications this operator watches |
| `kafka.bootstrapServers` | `oda-kafka-kafka-bootstrap.kafka.svc.cluster.local:9092` | Kafka bootstrap servers |
| `kafka.allowTopicDelete` | `false` | Whether the operator is allowed to actually delete a Kafka topic on cleanup (policy-gated; superseded topics are otherwise retained) |
| `adapter.image` | `lekhamaru/eventmanagement-notification-adapter:0.2.0` | Image used for per-SubscribedNotification adapter Deployments |
| `kopf.maxWorkers` | `20` | kopf's internal worker pool size |

This chart packages the operator exactly as already deployed in production
(see the plain manifests this was generated from, under the parent repo's
`manifests/` folder) -- no behavior change, just proper Helm packaging.
