# TMFC043 Fault Management component

Consumes `asyncapi` events and republishes them as `a2a` messages for the NetoAI agents. Code: `source/`. Chart: `chart/`.

| Flow | In (asyncapi) | Out (a2a) |
|---|---|---|
| Create alarm | `oda.trapReceiver.event.alarmDetected` | `oda.tmf642.command.createAlarm` |
| Clear alarm | `oda.trapReceiver.event.alarmCleared` | `oda.tmf642.command.clearAlarm` |
| Service problem created | `oda.alarmManagement.serviceProblemCreateNotificationEvent` | `oda.tmf656.serviceProblemCreateNotificationEvent` |
| Service problem state change | `oda.alarmManagement.serviceProblemStateChangeNotificationEvent` | `oda.tmf656.serviceProblemStateChangeNotificationEvent` |

The correlation id is copied unchanged. The alarm task id is `task_alarm_<externalAlarmId>`, the same for create and clear.
A2A over Kafka is a project convention, not an official A2A binding.

## APIs (proxy)
`/tmf-api/alarmManagement/v5/*` and `/tmf-api/serviceProblemManagement/v5/*` are forwarded unchanged to the NetoAI digital twin
(`TWIN_BASE_URL`, unset = answers 503). They are not yet declared in `exposedAPIs`, so the gateway does not publish them.

## Deploy (Helm)
Operator first, then the component. Both charts are in `charts/`.

```
# 1. Event Management Operator (release name canvas-eventmgmt-op; creates a Strimzi KafkaUser per component)
helm upgrade --install canvas-eventmgmt-op charts/eventmanagement-operator -n canvas \
  -f charts/eventmanagement-operator/examples/values-eks-dev.yaml

# 2. TMFC043. Pass the two CA certificates, or create Secrets tmfc043-kafka-ca / tmfc043-keycloak-ca (key ca.crt) first.
helm upgrade --install tmfc043 chart -n components \
  --set-file tls.kafkaCa=kafka-ca.crt --set-file tls.keycloakCa=keycloak-ca.crt \
  --set twin.baseUrl=http://<digital-twin>:<port>
```
The Component becomes `Complete` once the identity operator has created the Keycloak client Secret `tmfc043-secret`; the pod
starts after that. Image: `source/`. A KafkaUser is needed because Strimzi's User Operator removes the ACLs of any principal
without one; with `kafka.userManagement.enabled` the operator creates it, so the component chart leaves `kafka.createKafkaUser` false.

## Ordering
A clear can reach the alarm agent before its create. Create and clear are on different topics with different consumer
groups, and Kafka only orders messages within one topic. TMFC043 forwards each message as it arrives and does not reorder.
The alarm management agent must accept a clear for an alarm it has not seen yet, and match the create when it arrives,
using the shared task id (`task_alarm_<externalAlarmId>`).

## Tested on an AWS EKS dev cluster
Create and clear sent through the trap receiver (`/api/v1/trap`) appeared as A2A messages on `oda.tmf642.command.createAlarm` and `oda.tmf642.command.clearAlarm`
with the same correlation id and task id. A service problem event on `oda.alarmManagement.serviceProblemCreateNotificationEvent` was relayed to
`oda.tmf656.serviceProblemCreateNotificationEvent`. The 25 messages already on the trap receiver topics all transform.
