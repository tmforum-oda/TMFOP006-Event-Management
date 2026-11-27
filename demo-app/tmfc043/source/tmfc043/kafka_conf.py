"""Kafka client settings for TMFC043.

Authentication is OAuth (client credentials) against the canvas Keycloak, which is
what the broker's ``oauth`` listener (9095) validates. The client id and secret come
from the Secret the identity operator creates for the component (``tmfc043-secret``).
For a local or test broker, KAFKA_SECURITY_PROTOCOL=PLAINTEXT disables all of it.
"""

from __future__ import annotations

import os
import threading
import time
from typing import Any, Dict

import requests
try:  # kafka-python 3.x
    from kafka.net.sasl.oauth import AbstractTokenProvider
except ImportError:  # kafka-python 2.x
    from kafka.sasl.oauth import AbstractTokenProvider


class KeycloakTokenProvider(AbstractTokenProvider):
    """Fetches and caches a client-credentials token; refreshes 30 s before expiry."""

    def __init__(self, token_url: str, client_id: str, client_secret: str, ca_file: str | bool):
        super().__init__()
        self._url, self._id, self._secret, self._verify = token_url, client_id, client_secret, ca_file
        self._token, self._expires = "", 0.0
        self._lock = threading.Lock()

    def token(self) -> str:
        with self._lock:
            if time.time() >= self._expires - 30:
                resp = requests.post(
                    self._url,
                    data={"grant_type": "client_credentials", "client_id": self._id, "client_secret": self._secret},
                    verify=self._verify,
                    timeout=10,
                )
                resp.raise_for_status()
                body = resp.json()
                self._token = body["access_token"]
                self._expires = time.time() + int(body.get("expires_in", 60))
            return self._token


def client_kwargs(env=os.environ) -> Dict[str, Any]:
    protocol = env.get("KAFKA_SECURITY_PROTOCOL", "PLAINTEXT")
    kwargs: Dict[str, Any] = {
        "bootstrap_servers": env.get("KAFKA_BOOTSTRAP_SERVERS", "oda-kafka-kafka-bootstrap.kafka.svc.cluster.local:9095"),
        "security_protocol": protocol,
    }
    if protocol.startswith("SASL"):
        kwargs["sasl_mechanism"] = "OAUTHBEARER"
        kwargs["sasl_oauth_token_provider"] = KeycloakTokenProvider(
            env["KEYCLOAK_TOKEN_URL"],
            env["KAFKA_CLIENT_ID"],
            env["KAFKA_CLIENT_SECRET"],
            env.get("KEYCLOAK_CA_FILE") or True,
        )
    if protocol.endswith("SSL"):
        kwargs["ssl_cafile"] = env.get("KAFKA_SSL_CAFILE") or None
    return kwargs
