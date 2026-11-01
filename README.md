# TMFOP006-Event-Management

TM Forum Open Digital Architecture (ODA) Event Management Operator project.

```
.
└── demo-app/           Reference implementation(s) -- the TrapReceiver component
```

## demo-app/trapreceivercomponent

A worked reference implementation: an ODA component (publish-only,
TMF642 Alarm Management) that receives Zabbix alarm/clear traps over HTTP
and republishes them as TMF642-style CloudEvents on Kafka -- a
`PublishedNotification` registration in, a real Kafka topic with producer
ACLs out. See its own [README](demo-app/trapreceivercomponent/README.md)
for the full architecture, API reference, and deployment details. Deployed
and verified end-to-end on AWS EKS.
