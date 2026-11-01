# trapreceivercomponent

ODA component registration for the trapreceiveroct workload: it receives
Zabbix alarm/clear traps over HTTP and republishes them as TMF642-style
CloudEvents onto two Kafka topics, one per action.

It is **publish-only**: `coreFunction.exposedAPIs` is empty. Its sole
integration point with the rest of the Canvas is
`eventNotification.publishedEvents`, so the Event Management Operator can
provision the Kafka topics the workload publishes to.

## Quick start (run it locally, no Kubernetes needed)

Everything needed to run this is in this one folder. No cloud account, no
Kubernetes cluster -- just Docker.

```sh
git clone https://github.com/tmforum-oda/TMFOP006-Event-Management.git
cd TMFOP006-Event-Management/demo-app/trapreceivercomponent
docker compose up --build
```

This starts a local single-node Kafka broker and the component together,
pre-wired to talk to each other. Once it's up (`trapreceiver-demo-app`
logs "Running on all addresses"), send it a test alarm in a second
terminal:

```sh
curl -X POST http://localhost:8080/api/v1/trap \
  -H "Content-Type: application/json" \
  -H "X-API-Key: demo-trap-key" \
  -d '{"@type":"Alarm","correlation_id":"demo-0001","externalAlarmId":"demo-0001","alarmRaisedTime":"2026-10-31T19:30:00Z"}'
```

The response is the full TMF642 `AlarmCreate` CloudEvent it built and
published. To see the actual message sitting on the Kafka topic (not just
the HTTP response):

```sh
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic oda.trapReceiver.event.alarmDetected \
  --from-beginning --max-messages 1
```

Stop everything with `docker compose down -v`.

This exact sequence was verified working end-to-end (auth, validation,
transform, and the real Kafka write all confirmed) as part of preparing
this reference implementation.

### Running it for real (on a Kubernetes cluster)

The sections below cover deploying this as an actual ODA component via
Helm, with real Kafka topic governance through the Event Management
Operator's `PublishedNotification` mechanism -- the `docker compose` setup
above is for trying it out quickly, not a substitute for that.

## Topics

Two topics, one per TMF642 action (not one merged topic), named to match
what Zabbix is configured against:

| Action | Topic |
|---|---|
| AlarmCreate | `oda.trapReceiver.event.alarmDetected` |
| AlarmClear | `oda.trapReceiver.event.alarmCleared` |

These replace the original legacy topics
(`oda.tmf642-refImpl-alarmCreateResource` / `alarmClearResource`). An
intermediate iteration briefly tried an `alarmCreated`/`alarmCleared` naming
(matching the create/clear convention used elsewhere on this cluster by
`tmf642-reflmpl` and `tmfc043-refimpl`) before reverting the create-side name
back to `alarmDetected`.

## Folder layout

Everything for this component lives in this one folder:

- `python/`, `Dockerfile`, `docker-compose.yml`, `VERSION` -- the actual
  application: a Flask service, its dependencies, and two ways to run its
  container (plain `docker build`, or the full local stack via compose).
- `Chart.yaml`, `values.yaml`, `templates/` -- a standard TM Forum ODA
  Component Helm chart (structured the same way as
  [tmforum-oda/reference-example-components](https://github.com/tmforum-oda/reference-example-components)).
  `helm install <release> trapreceivercomponent -n components` installs it
  on a real Kubernetes cluster.
- `k8s/` -- plain (non-templated) manifests, including the hand-authored
  `Component` and `PublishedNotification` CRs that pin the exact topic
  names (`05a`, `05b`, `05c`).

## What the Helm chart creates

- `Component` (`oda.tmforum.org/v1`) -- `templates/component-trapreceivercomponent.yaml`
- `Deployment` + `Service` for trapreceiveroct -- `templates/deployment-trapreceiveroct.yaml`
- A public `LoadBalancer` Service for Zabbix, opt-in via
  `trapreceiveroct.externalAccess.enabled` (default `true`) --
  `templates/service-external.yaml`
- Two `ConfigMap`s (app config, Kafka infra env vars) -- `templates/configmap-*.yaml`

It does **not** create the `kafka.existingSecretName` / `auth.existingSecretName`
Secrets referenced in `values.yaml` -- those must already exist in the target
namespace (same pattern the SASL credentials already use today).

## Installing this on your own cluster: what gets created vs. what you must supply

| | Behavior |
|---|---|
| Public endpoint (`externalAccess`) | **Created fresh on install.** `type: LoadBalancer` is cloud-agnostic -- your cluster provisions its own load balancer (an AWS ELB on EKS, a GCP Load Balancer on GKE, an Azure LB on AKS), in your own account. Nothing connects back to any other cluster. On a cluster with no cloud load-balancer controller (local kind/minikube), it stays `<pending>` -- use `kubectl port-forward` or set `externalAccess.enabled=false` instead. |
| Kafka (`kafka.bootstrapServers`) | **Not created -- you must supply your own.** The default value is an internal DNS name specific to the cluster this chart was built against; it will not resolve anywhere else. Point it at your own Kafka cluster with `--set kafka.bootstrapServers=<your-broker>:9092` (or your own values file) before installing. |
| Kafka/API credentials (`kafka.existingSecretName`, `auth.existingSecretName`) | **Not created -- you must supply your own** Secrets with those names in the target namespace. |

## Topic-name caveat

`values.yaml`'s `eventNotification.create.topic` / `eventNotification.clear.topic`
are what the app's own config is told to publish to (`channel_address` in
`configmap-trapreceiveroct.yaml`). They are **not** guaranteed to be the
literal topic names a Component Operator derives from this chart's
`Component` resource: the installed Component CRD on the cluster this was
built against has no `topic` override field under
`eventNotification.publishedEvents[].specification[]` (confirmed via
`kubectl explain component.spec.eventNotification.publishedEvents --recursive
--api-version=oda.tmforum.org/v1`), so that naming is entirely up to
whatever Component Operator reconciles this chart's `Component` object --
and that operator's source isn't in this repo to audit.

This was confirmed as a real problem, not just a theoretical one: installing
an earlier, single-topic version of this chart's `Component` caused the live
Component Operator to auto-create a second `PublishedNotification`
(`trapreceiver1-trapreceivercomponent-alarmdetectedevent`) with its own
different, unused topic
(`oda.trapreceiver1-trapreceivercomponent.alarmdetectedevent`) -- deleted as
part of this change.

The path that's *confirmed* to produce the exact literal names is the
hand-authored standalone `PublishedNotification` pair, applied directly (not
via this chart):
`k8s/05b-trapreceivercomponent-publishednotification-detected.yaml` and
`k8s/05c-trapreceivercomponent-publishednotification-clear.yaml` (each sets
`spec.topic` explicitly, which the Event Management Operator honors ahead of
any derived name). Before relying on this chart's `Component` for topic
creation in place of those standalone CRs, confirm the Component Operator's
derived names match -- then retire the standalone CRs so there's one
`PublishedNotification` (and one topic) per action.

## Values

| Key | Default | Description |
|---|---|---|
| `component.name` | `trapreceivercomponent` | ODA component name |
| `eventNotification.create.topic` | `oda.trapReceiver.event.alarmDetected` | Kafka topic for AlarmCreate |
| `eventNotification.clear.topic` | `oda.trapReceiver.event.alarmCleared` | Kafka topic for AlarmClear |
| `trapreceiveroct.image` | `jiotmraj/trapreceivercomponent:1.0.0` | Container image (multi-arch: amd64 + arm64) |
| `trapreceiveroct.externalAccess.enabled` | `true` | Create the public LoadBalancer Service for Zabbix |
| `kafka.existingSecretName` | `trapreceiver2-kafka-creds` | Pre-existing Secret: `KAFKA_SASL_USERNAME` / `KAFKA_SASL_PASSWORD` |
| `auth.existingSecretName` | `trapreceiver2-auth` | Pre-existing Secret: `TRAP_API_KEY` / `DASHBOARD_API_KEY` |

## Install

```sh
helm install trapreceiver1 trapreceivercomponent -n components
```

The `components` namespace matches the convention most ODA Canvas
deployments use for where real components run, as opposed to a dedicated
namespace per component.
