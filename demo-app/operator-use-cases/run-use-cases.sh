#!/usr/bin/env bash
# Runs the five operator use cases against a Component named "uctest" and prints the state after each step.
# Usage: CONTEXT=<kube context> ./run-use-cases.sh          (the cluster needs Strimzi, Keycloak and the operators)
set -euo pipefail
CTX="${CONTEXT:?set CONTEXT to your kubectl context}"
kx() { kubectl --context "$CTX" "$@"; }
PW=$(kx get secret kafka-ui-admin -n kafka -o jsonpath='{.data.password}' | base64 -d)
broker() { # runs a Kafka CLI on the broker with all its arguments; retries if the kubectl exec stream drops
  local tries=0
  until kx exec -n kafka oda-kafka-kafka-broker-0 -c kafka -- sh -c "printf 'security.protocol=SASL_PLAINTEXT\nsasl.mechanism=SCRAM-SHA-512\nsasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required username=\"kafka-ui-admin\" password=\"$PW\";\n' > /tmp/a.properties; bin/$1 --bootstrap-server localhost:9092 --command-config /tmp/a.properties ${@:2} 2>/dev/null; rm -f /tmp/a.properties"; do
    tries=$((tries+1)); [ "$tries" -ge 3 ] && break; sleep 5
  done
}
show() {
  echo "--- $1"
  echo "component: $(kx get components.v1.oda.tmforum.org uctest -n components -o jsonpath='{.status.summary/status.deployment_status}' 2>/dev/null || echo gone)"
  kx get publishednotifications.oda.tmforum.org -n components -l oda.tmforum.org/componentName=uctest -o custom-columns=PUBLISHED:.metadata.name,TOPIC:.spec.topic,STATE:.status.eventManagement.state --no-headers 2>/dev/null || true
  kx get subscribednotifications.oda.tmforum.org -n components -l oda.tmforum.org/componentName=uctest -o custom-columns=SUBSCRIBED:.metadata.name,TOPIC:.spec.topic,STATE:.status.eventManagement.state --no-headers 2>/dev/null || true
  echo "topics:   $(broker kafka-topics.sh --list | grep uctest | tr '\n' ' ')"
  echo "ACL lines for oda-uctest: $(broker kafka-acls.sh --list --principal User:oda-uctest | grep -c 'operation=' || true)"
}
wait_for() { sleep "${1:-70}"; }
here="$(cd "$(dirname "$0")" && pwd)"

kx delete kafkauser.kafka.strimzi.io oda-uctest -n kafka --ignore-not-found >/dev/null
echo "== UC1 install"; kx apply -f "$here/uc1-install.yaml"; wait_for; show "after UC1 install"
echo "== UC2 update: publishedNotification topic changed"; kx apply -f "$here/uc2-update-changed-publishednotification.yaml"; wait_for; show "after UC2 (expected: topic NOT changed, see README)"
echo "== UC3 update: additional API"; kx apply -f "$here/uc3-update-additional-api.yaml"; wait_for; show "after UC3 additional API"
echo "== UC4 update: deleted API"; kx apply -f "$here/uc4-update-deleted-api.yaml"; wait_for; show "after UC4 (expected: notification NOT removed, see README)"
echo "== UC5 delete component"; kx delete component.oda.tmforum.org uctest -n components; wait_for 80; show "after UC5 delete"
echo "KafkaUser oda-uctest: $(kx get kafkauser oda-uctest -n kafka --no-headers 2>/dev/null | awk '{print $1}' || true)"
