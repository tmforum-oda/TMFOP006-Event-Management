# Recording guide: operator use cases UC1–UC5

Record each use case as its own clip. For each one: show the change, show what the operator did, then say what it means.
Replace `$CTX` with your kube context and run every command from this folder (`demo-app/operator-use-cases`).

## Before you start (once)
```
export CTX=<your-kube-context>
kx() { kubectl --context "$CTX" "$@"; }
kx delete kafkauser.kafka.strimzi.io oda-uctest -n kafka --ignore-not-found   # clean start
kx get components.v1.oda.tmforum.org -n components | grep uctest || echo "no uctest yet"
```
Open a second terminal for the Kafka view. The easiest is Kafka UI:
```
kubectl --context "$CTX" port-forward -n kafka svc/kafka-ui 8081:80
```
Then open http://localhost:8081 → Topics. Filter on `uctest`.

Each step below ends with a check. Wait about 60–70 seconds after each `apply` before you check, because the operators act on a timer.

## What to show on screen, every step
```
kx get components.v1.oda.tmforum.org uctest -n components -o custom-columns=NAME:.metadata.name,STATE:.status.summary/status.deployment_status
kx get publishednotifications.oda.tmforum.org -n components -l oda.tmforum.org/componentName=uctest -o custom-columns=NAME:.metadata.name,TOPIC:.spec.topic,STATE:.status.eventManagement.state
kx get subscribednotifications.oda.tmforum.org -n components -l oda.tmforum.org/componentName=uctest -o custom-columns=NAME:.metadata.name,TOPIC:.spec.topic,STATE:.status.eventManagement.state
```
and in Kafka UI, the topics list. `script` does the same in one go: `CONTEXT=$CTX ./run-use-cases.sh` (about 10 minutes, use it as a backup recording).

---

## UC1 — Install a component
**Say:** a component is declared once. The operator creates a PublishedNotification for each published event and a SubscribedNotification for each subscribed one. Each notification creates its Kafka topic and grants the component access. Nothing else needs to be done.

1. **Show the file:** `cat uc1-install.yaml`. Point at `publishedEvents` (two events), `subscribedEvents` (one event, with `publisher` and `transport: native`), and `securityFunction`.
2. **Deploy:** `kx apply -f uc1-install.yaml`
3. **Watch the component:** `kx get components.v1.oda.tmforum.org uctest -n components -w` until `STATE` is `Complete` (press Ctrl-C).
4. **Show the notifications** (the three commands above). All three are `Active`.
5. **Show the topics:** in Kafka UI, `oda.uctest.event.created` and `oda.uctest.event.updated` are listed. The subscriber reads `oda.trapReceiver.event.alarmDetected`, which is owned by the trap receiver and already exists.
6. **Show the access:** `kx get kafkauser oda-uctest -n kafka -o yaml | sed -n '/acls/,$p' | head -40`. One rule per topic, with write for publishing and read for subscribing.

**Expected:** Complete, three notifications Active, two topics created. Access lines `Write`/`Describe` for the published topics and `Read`/`Describe` for the subscribed one.

---

## UC2 — Update: a publishedNotification's topic changes
**Say:** the component changes the topic of an event it already publishes. The operator should move the event to the new topic and stop using the old one.

1. **Show what changed:** `diff uc1-install.yaml uc2-update-changed-publishednotification.yaml`. Only the topic of `uc.event.updated` changes, from `oda.uctest.event.updated` to `oda.uctest.event.changed`.
2. **Deploy:** `kx apply -f uc2-update-changed-publishednotification.yaml`
3. **Show the component spec has the new topic:**
   `kx get components.v1.oda.tmforum.org uctest -n components -o jsonpath='{range .spec.eventNotification.publishedEvents[*]}{.name}{" -> "}{.topic}{"\n"}{end}'`
4. **Show the notification:** the `uctest-uc.event.updated` line still says `oda.uctest.event.updated`. In Kafka UI, `oda.uctest.event.changed` does not appear.

**Expected, and what to say on camera:** the new topic is **not** created. The PublishedNotification keeps the old topic. This is a gap in the upstream Component Operator: it creates the child when it is missing, and it does not update an existing child. The README in this folder has the details. Present it as a finding, not a bug in this demo.

---

## UC3 — Update: an additional API
**Say:** the component adds an event. The operator creates one more notification, one more topic and one more set of access rules, and the existing ones are untouched.

1. **Show the change:** `diff uc2-update-changed-publishednotification.yaml uc3-update-additional-api.yaml` (the new event is `uc.event.added`).
2. **Deploy:** `kx apply -f uc3-update-additional-api.yaml`
3. **Show the notifications:** `uctest-uc.event.added` is listed and `Active`. The other three are unchanged.
4. **Show the topic:** `oda.uctest.event.added` in Kafka UI.

**Expected:** a fourth Active notification, a new topic, and the other notifications unchanged.

---

## UC4 — Update: a deleted API
**Say:** the component removes an event. The operator should remove that notification, revoke its access and keep the topic, because other components may still read it.

1. **Show the change:** `diff uc3-update-additional-api.yaml uc4-update-deleted-api.yaml` (`uc.event.added` is removed).
2. **Deploy:** `kx apply -f uc4-update-deleted-api.yaml`
3. **Show the notifications:** `uctest-uc.event.added` is still `Active`.

**Expected, and what to say on camera:** the removed event's notification is **not** deleted. This is the same upstream gap as UC2: the Component Operator creates children for the events it sees and never removes children for events it no longer sees. For the demo, show the manual step, which works today:
```
kx delete publishednotification.oda.tmforum.org uctest-uc.event.added -n components
```
Then show that the topic `oda.uctest.event.added` still exists. The operator keeps topics on purpose, and its access rule for that topic is removed.

---

## UC5 — Delete the component
**Say:** deleting the component removes its notifications and the access the operator granted. The topics stay in place, because the operator never deletes topics by default.

1. **Deploy the delete:** `kx delete component.oda.tmforum.org uctest -n components`
2. **Wait about 80 seconds**, then show the notifications: the list is empty.
3. **Show the access:** `kx get kafkauser oda-uctest -n kafka` returns `NotFound`. The operator removed the user, and its ACLs with it.
4. **Show the topics** in Kafka UI: `oda.uctest.*` is still there. Say that this is deliberate (`KAFKA_ALLOW_TOPIC_DELETE=false`), and that you can clean them up by hand.

**Expected:** no notifications, no `oda-uctest` KafkaUser, topics kept.

---

## After recording
- Tidy up: `kx delete kafkauser.kafka.strimzi.io oda-uctest -n kafka --ignore-not-found`, and remove the `oda.uctest.*` topics if you want a clean broker.
- If UC2 or UC4 is recorded as "not applied", say it on the clip and point to the README. The two gaps need a change to the upstream Component Operator.
