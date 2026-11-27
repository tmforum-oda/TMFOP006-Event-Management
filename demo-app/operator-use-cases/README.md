# Operator use cases: install, update and delete a component

This walks through the five use cases of the deck (slides 13–17) on a live cluster. It uses a test component
`uctest` in namespace `components`, so no real component is touched. Run it as a demo or as a regression check.

## What you need
- `kubectl` with a context for the cluster, Helm, Strimzi Kafka (`kafka` namespace) and the Event Management Operator
  (chart `charts/eventmanagement-operator`, operator 0.3.2 or later, `kafka.userManagement.enabled: true`).
- Permission to read the `kafka-ui-admin` Secret, used only to list topics and ACLs.

## Run it
```
cd demo-app/operator-use-cases
CONTEXT=<your-kube-context> ./run-use-cases.sh
```
The script applies each step, waits about a minute, and prints the component state, the notifications with their
state, the topics, and the number of ACL lines for `oda-uctest`. Allow about 10 minutes.

## The steps and what to look for

| Step | Manifest | What should happen | What we saw (2026-11-11, AWS EKS dev cluster) |
|---|---|---|---|
| UC1 Install | `uc1-install.yaml` | Component reaches `Complete`. One PublishedNotification per published event, each Active, with its topic created. The subscription is Active and reads the trap receiver topic. | Complete. Three notifications Active. Both published topics created. |
| UC2 Update, changed publishedNotification | `uc2-update-changed-publishednotification.yaml` | The topic of `uc.event.updated` changes to `oda.uctest.event.changed`. The new topic is created and the old producer ACL is removed. | **Not applied.** The Component still shows the new topic, but the PublishedNotification keeps `oda.uctest.event.updated`. See gap 1. |
| UC3 Update, additional API | `uc3-update-additional-api.yaml` | A new PublishedNotification `uc.event.added` appears, Active, with its topic. | Applied. Topic created, ACLs granted. |
| UC4 Update, deleted API | `uc4-update-deleted-api.yaml` | `uc.event.added` is removed. Its PublishedNotification goes away and its producer ACL is revoked. The topic is kept. | **Not applied.** The PublishedNotification stays Active. See gap 2. |
| UC5 Delete component | `kubectl delete component uctest` | All child notifications are removed and the component's Kafka access is revoked. Topics are kept (topic deletion is off by policy). | Component and notifications removed. ACLs for `oda-uctest` 0 and its KafkaUser removed. |

## Known gaps (not fixed in this repository)
1. **Changed topic is not propagated.** `createPublishedNotificationResource` in the Component Operator
   (`componentOperator.py`) returns when the child PublishedNotification already exists. It never copies a changed
   spec into it. The fix belongs in the upstream Component Operator: patch the child when its spec differs.
2. **Removed event is not deleted.** The Component Operator creates children for the events it sees, but does not
   delete children whose event was removed. The same change is needed there.

Both gaps are in the Component Operator, which is outside this repository. Until they are fixed, UC2 and UC4 need a
manual step: delete the PublishedNotification with `kubectl delete publishednotification <name>`, or recreate the component.

## Things to know
- **Component status.** The component shows `Complete` only after the identity operator creates its Keycloak client.
  The test manifests declare `securityFunction` for this. Without that field the identity handler fails each minute
  and blocks updates, so UC3 looks like it did nothing.
- **Topics stay after delete.** The operator never deletes topics by default (`KAFKA_ALLOW_TOPIC_DELETE=false`), so
  `oda.uctest.*` topics remain after UC5. Remove them by hand if you want a clean broker.
- **ACL lag.** The operator writes the `KafkaUser`; the Strimzi User Operator applies its ACLs a little later.
  The script waits about a minute, which was enough in our runs.
