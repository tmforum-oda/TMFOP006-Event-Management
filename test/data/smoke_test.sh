#!/usr/bin/env bash
set -euo pipefail
# Simple smoke test: POST a sample CloudEvent JSON to the handler
curl -s -H "Content-Type: application/json" -d @test_event.json http://127.0.0.1:8080/events | jq || true
