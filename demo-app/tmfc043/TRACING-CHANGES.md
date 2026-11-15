# TMFC043 input tracing: what changed, and how to undo it

Status: deployed to the AWS EKS dev cluster (`ihpp-canvas-dev-01`) on 15 November 2026.
Branch: `feature/tmfc043-input-tracing` (Azure DevOps). Application image `lekhamaru/tmfc043:0.2.0`.

## What tracing does, in one paragraph
Each Kafka record that TMFC043 handles gets one span. The span is written to the pod log as one line. The trace
collector (`tmfc043-trace`) reads that line and sends a reconstructed trace event to the in-cluster Langfuse. The
Langfuse session ID is the business correlation ID, so one fault can be followed across the components. The events are
approximate: the collector rebuilds the trace ID, shows a duration of about 1 ms and always reports OK. Do not present
latency or error status from this view as accurate.

## Changes, in the order they were made

| Step | Helm revision | What changed | Cluster effect |
|---|---|---|---|
| 0 | 1 | Starting point: `tmfc043` `0.1.0` (installed 6 Nov 2026) | Working, create and clear tested |
| 1 | 2 | Upgrade to `0.2.0` failed on a field-ownership conflict (`kubectl` client-side apply owned `containers[].image`) | None. The old pod kept running |
| 1 | 3 | Upgrade to `0.2.0` with `--force-conflicts`, collector off | Deployment image and tracing environment variables changed. Application pod restarted. Component unchanged |
| 2 | 4 | Collector on: `tmfc043-trace` Deployment, Service, ServiceAccount, Role, RoleBinding added; Component `managementFunction` added | Collector pod started, Component stayed `Complete` |

Chart version stayed `tmfc043-0.1.0` in every revision. The app version is `0.2.0` from revision 3 on.

### Code (branch commits)
- `62087df` input tracing in the application. New `tmfc043/telemetry.py` (one CONSUMER span per record, log line per
  span, parent context from `traceparent` header only, identifiers only, no payloads or secrets). `app.py` wraps the
  existing forward and commit in the span; business behaviour is unchanged. 30 unit tests pass.
- `e5f48e1` collector templates and the Component management entry, both off by default.
- The chart default was then set to `traceCollector.enabled: true` to match the deployed state (see note below).

### Cluster objects added or changed by revision 4
- Deployment `tmfc043-trace` (image `akumartmf/pf-netoai-trace-collector:v0.1.0`, the same version as the fault-management
  agent's collector), Service `tmfc043-trace`, ServiceAccount `tmfc043-trace`, Role and RoleBinding `tmfc043-trace`
  (pods get/list/watch, pods/log get, in `components` only).
- Component `tmfc043`: `managementFunction.exposedAPIs` gained `trace-collector-metrics` (openmetrics, path
  `/tmfc043/trace-collector-metrics`).
- Secret `demo-agent-langfuse-otel` is read by the collector (key `authorization`). It was not created or changed.

## Verification performed
- Application: create and clear through the trap receiver returned 200. Forwarded lines and span lines appeared in the
  pod log. The service-problem messages carried `traceparent` headers from the alarm agent, and the span's trace ID
  matched the header.
- Collector: reads the application pod log (`LOG_LABEL_SELECTOR=app=tmfc043`). Counters after a second test:
  `trace_collector_log_lines_read_total` 291, `trace_collector_events_exported_total` 8.
- Langfuse (traces API, read from inside the cluster): the test sessions appear with session IDs equal to the correlation
  IDs (`collector-test-…`, `trace-deploy-…`). The fault-management and alarm-management scopes are also present.
- Component: `Complete` throughout. Helm diff against the live cluster is empty after the chart default was updated.

## Known limits (say these on stage)
- Latency and status are approximate (see above). Trace continuity across components is by correlation ID, not by a
  native trace ID.
- The collector only sees lines inside its lookback window (`LOG_SINCE_SECONDS=60`). Lines older than that are not
  exported after a restart. The collector's dedupe cache is in memory.
- Malformed JSON, decoded before the record loop, is not covered by a span.

## How to undo it

Pick the target that matches what you need:

1. **Collector off, keep the app on 0.2.0** (no Component change beyond removing the entry):
   `helm upgrade tmfc043 charts/tmfc043 -n components --set traceCollector.enabled=false --force-conflicts --wait`
2. **Full rollback to the original 0.1.0** (removes the collector and tracing; this is the tested baseline):
   `helm rollback tmfc043 1 -n components --wait`
   Revision 1 has no collector objects, so Helm removes them.
3. Verify after either step: `kubectl get deploy tmfc043 -n components -o jsonpath='{.spec.template.spec.containers[0].image}'`
   gives the expected tag, the Component is `Complete`, and one create and one clear reach
   `oda.tmf642.command.createAlarm` and `oda.tmf642.command.clearAlarm`.

Rollback does not touch topics, Kafka ACLs, the Keycloak client, or messages already processed.

## Operational notes
- The chart default is now `traceCollector.enabled: true`. A plain `helm upgrade` keeps the collector on. To turn it off, set
  the value to `false` explicitly.
- Helm upgrades of this release need `--force-conflicts` while `kubectl` client-side apply still owns the image field. That
  is the same conflict we saw on the operator. Removing the old field owner is a separate clean-up.
- Image `lekhamaru/tmfc043:0.2.0` is on Docker Hub. The collector image is third-party (`akumartmf/…`); it was approved
  for use in `components`.
