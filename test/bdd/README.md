# BDD tests

Scaffold for behavior-driven tests of the Event Management Operator
(`.feature` files + step definitions). None exist yet in this repository --
`test/data/` holds the sample event payload and smoke-test script currently
used for manual verification.

Suggested layout once added:

```
test/bdd/
├── features/
│   └── publish_notification.feature
└── steps/
    └── publish_notification_steps.py
```
