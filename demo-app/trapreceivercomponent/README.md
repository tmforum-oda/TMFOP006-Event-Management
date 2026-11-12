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
terminal. Every field below is required -- TMF642's required Alarm fields
(see "Required payload fields" below) are enforced with a 400, not
defaulted, so a minimal payload missing any of them will be rejected:

```sh
curl -X POST http://localhost:8080/api/v1/trap \
  -H "Content-Type: application/json" \
  -H "X-API-Key: demo-trap-key" \
  -d '{
    "@type": "Alarm",
    "correlation_id": "demo-0001",
    "externalAlarmId": "demo-0001",
    "state": "raised",
    "alarmType": "communicationsAlarm",
    "severity": "Disaster",
    "probableCause": "lossOfSignal",
    "alarmedObjectType": "Interface",
    "alarmedObject": {"@type": "AlarmedObjectRef", "id": "Gi1/0", "@referredType": "Host"},
    "sourceSystemId": "zabbix",
    "alarmRaisedTime": "2026-10-31T19:30:00Z",
    "alarmDetails": "Interface Gi1/0(): Link down"
  }'
```

The response is the full TMF642 `AlarmCreate` CloudEvent it built and
published (HTTP 200). If Kafka itself is unreachable, the response is a 502
with the built CloudEvent included in the error body -- the alarm is never
silently reported as delivered when it wasn't. To see the actual message
sitting on the Kafka topic (not just the HTTP response):

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

## Required payload fields

The app validates TMF642's required Alarm/ClearAlarm fields itself and
returns 400 for anything missing or the wrong type -- it never invents a
plausible-looking default in their place (an earlier version did this for
`perceivedSeverity`, `alarmedObject`, `probableCause`, etc., which hid real
sender bugs).

| Action | Required fields |
|---|---|
| create (`@type: Alarm`) | `correlation_id`, `externalAlarmId`, `state`, `alarmType`, `severity` (one of `severity_map`'s keys below -- mapped to TMF642's `perceivedSeverity`), `probableCause`, `alarmedObjectType`, `alarmedObject` (object), `sourceSystemId`, `alarmRaisedTime` |
| clear (`@type: ClearAlarm`) | `correlation_id`, `recovery_event_id`, `alarmClearedTime`, `alarmPattern` (non-empty array, should reference the create's `externalAlarmId` so the two can be correlated downstream), `clearSystemId`, `clearUserId` |

`severity` (Zabbix's own vocabulary) maps to TMF642's `perceivedSeverity`
via `values.yaml`'s `alarm.severityMap`: `Disaster`→`critical`,
`High`→`major`, `Average`→`minor`, `Warning`→`warning`,
`Info`/`Not classified`→`indeterminate`.

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
  application: a Flask service (served by gunicorn in the container),
  its dependencies, a test suite (`python/tests/`), and two ways to run
  its container (plain `docker build`, or the full local stack via compose).
- `Chart.yaml`, `values.yaml`, `templates/` -- a standard TM Forum ODA
  Component Helm chart (structured the same way as
  [tmforum-oda/reference-example-components](https://github.com/tmforum-oda/reference-example-components)).
  `helm install <release> trapreceivercomponent -n components` installs it
  on a real Kubernetes cluster.
- `asyncapi/` -- the real AsyncAPI 3.1.0 documents describing the two
  published events (replacing earlier `example.com` placeholder URLs).
- `k8s/` -- plain (non-templated) manifests for standalone `kubectl apply`
  use: namespace-agnostic Secret template (`02`), app config (`03`), and the
  hand-authored standalone `PublishedNotification` CRs that pin the exact
  topic names (`05b`, `05c` -- see "Topic-name caveat" below for why these,
  not the chart's `Component`, are what actually registers the topics).

## What the Helm chart creates

- `Component` (`oda.tmforum.org/v1`) -- `templates/component-trapreceivercomponent.yaml`.
  Deliberately declares no `eventNotification.publishedEvents` (see
  "Topic-name caveat" below).
- `Deployment` + `Service` for trapreceiveroct -- `templates/deployment-trapreceiveroct.yaml`.
  Runs as non-root (`runAsUser: 10001`), read-only root filesystem, all
  capabilities dropped, `seccompProfile: RuntimeDefault`.
- A public `LoadBalancer` Service for Zabbix, opt-in via
  `trapreceiveroct.externalAccess.enabled` (default `true`) --
  `templates/service-external.yaml`
- Two `ConfigMap`s (app config, Kafka infra env vars) -- `templates/configmap-*.yaml`

It does **not** create the `kafka.existingSecretName` / `auth.existingSecretName`
Secrets referenced in `values.yaml` -- those must already exist in the target
namespace. `k8s/02-secrets.yaml` is a redacted template for them, not real
credentials -- fill in your own base64-encoded values before applying it;
never commit real values.

## Installing this on your own cluster: what gets created vs. what you must supply

| | Behavior |
|---|---|
| Public endpoint (`externalAccess`) | **Created fresh on install.** `type: LoadBalancer` is cloud-agnostic -- your cluster provisions its own load balancer (an AWS ELB on EKS, a GCP Load Balancer on GKE, an Azure LB on AKS), in your own account. Nothing connects back to any other cluster. On a cluster with no cloud load-balancer controller (local kind/minikube), it stays `<pending>` -- use `kubectl port-forward` or set `externalAccess.enabled=false` instead. |
| Kafka (`kafka.bootstrapServers`) | **Not created -- you must supply your own.** The default value is an internal DNS name specific to the cluster this chart was built against; it will not resolve anywhere else. Point it at your own Kafka cluster with `--set kafka.bootstrapServers=<your-broker>:9092` (or your own values file) before installing. |
| Kafka/API credentials (`kafka.existingSecretName`, `auth.existingSecretName`) | **Not created -- you must supply your own** Secrets with those names in the target namespace. |

## Topic-name caveat

`values.yaml`'s `eventNotification.create.topic` / `eventNotification.clear.topic`
are what the app's own config is told to publish to (`channel_address` in
`configmap-trapreceiveroct.yaml`). They are **not** the literal topic names
a Component Operator would derive from this chart's `Component` resource if
it declared `eventNotification.publishedEvents`: the installed Component
CRD has no `topic` override field under
`eventNotification.publishedEvents[].specification[]` (confirmed via
`kubectl explain component.spec.eventNotification.publishedEvents --recursive
--api-version=oda.tmforum.org/v1`), so that naming would be entirely up to
whatever Component Operator reconciles this chart's `Component` object --
and that operator's source isn't in this repo to audit.

This was confirmed as a real problem, not just a theoretical one: installing
an earlier, single-topic version of this chart's `Component` (which did
declare `publishedEvents`) caused the live Component Operator to auto-create
a second `PublishedNotification` (`trapreceiver1-trapreceivercomponent-alarmdetectedevent`)
with its own different, unused topic
(`oda.trapreceiver1-trapreceivercomponent.alarmdetectedevent`). That object
was deleted, and the fix is now permanent: this chart's `Component` declares
`eventNotification.publishedEvents: []` (see
`templates/component-trapreceivercomponent.yaml`), not a stopgap pending
confirmation.

The single source of truth for topic registration is the hand-authored
standalone `PublishedNotification` pair, applied directly (not via this
chart, and not auto-created by it):
`k8s/05b-trapreceivercomponent-publishednotification-detected.yaml` and
`k8s/05c-trapreceivercomponent-publishednotification-clear.yaml` (each sets
`spec.topic` explicitly, which the Event Management Operator honors). Revisit
this only once the Component CRD itself gains a `topic` override field
upstream in `oda-canvas` -- not expected soon, and not required for this
component to work correctly today.

## Values

| Key | Default | Description |
|---|---|---|
| `component.name` | `trapreceivercomponent` | ODA component name |
| `eventNotification.create.topic` | `oda.trapReceiver.event.alarmDetected` | Kafka topic for AlarmCreate |
| `eventNotification.clear.topic` | `oda.trapReceiver.event.alarmCleared` | Kafka topic for AlarmClear |
| `trapreceiveroct.image` | `jiotmraj/trapreceivercomponent:1.1.0` | Container image (multi-arch: amd64 + arm64) |
| `trapreceiveroct.imagePullPolicy` | `IfNotPresent` | Tags are immutable (never overwritten), so no need to re-pull |
| `trapreceiveroct.externalAccess.enabled` | `true` | Create the public LoadBalancer Service for Zabbix |
| `kafka.existingSecretName` | `trapreceivercomponent-kafka-creds` | Pre-existing Secret: `KAFKA_SASL_USERNAME` / `KAFKA_SASL_PASSWORD` |
| `auth.existingSecretName` | `trapreceivercomponent-auth` | Pre-existing Secret: `TRAP_API_KEY` / `DASHBOARD_API_KEY` |

## Dashboard

`GET /dashboard` itself needs no API key -- its own JavaScript prompts for
the dashboard key once (stored in the browser's `localStorage`) and attaches
it as `X-API-Key` when it calls the two JSON endpoints
(`/api/v1/events`, `/api/v1/events/summary`), which do still require it.

## Testing

```sh
cd python
pip install -r requirements.txt -r requirements-dev.txt
pytest -q
```

Covers both the happy path (using the same real Zabbix create/clear sample
this component was validated against end-to-end on AWS EKS) and the
negative cases: each TMF642-required field missing, an invalid `severity`,
a clear with no `alarmPattern`, malformed input types, and a Kafka publish
failure returning 502 rather than silently reporting success.

## Install

```sh
helm install trapreceiver1 trapreceivercomponent -n components
```

The `components` namespace matches the convention most ODA Canvas
deployments use for where real components run, as opposed to a dedicated
namespace per component.
