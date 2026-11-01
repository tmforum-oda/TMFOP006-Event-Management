# =============================================================
# trapreceivercomponent — Production Docker Image
# TMF642 Alarm Management | ODA Canvas | Kafka CloudEvent Pipeline
# =============================================================
#
# Fully self-contained -- build context is THIS folder:
#
#   cd demo-app/trapreceivercomponent
#   docker build -t trapreceivercomponent:1.0.0 .
#
# Or skip building entirely and use the public pre-built image:
#   jiotmraj/trapreceivercomponent:1.0.0 (linux/amd64 + linux/arm64)
#
# Easiest way to actually run it (Kafka + app together, no Kubernetes
# needed): see docker-compose.yml in this same folder.
# =============================================================

FROM python:3.11-slim

# ── Metadata ──────────────────────────────────────────────────────────────────
LABEL maintainer="TM Forum Reference Implementation" \
      version="1.0.0" \
      description="trapreceivercomponent — ODA Canvas Kafka CloudEvent Pipeline (TMF642)" \
      org.opencontainers.image.title="trapreceivercomponent" \
      org.opencontainers.image.version="1.0.0" \
      org.opencontainers.image.source="https://github.com/tmforum-oda/TMFOP006-Event-Management"

# ── Non-root user (security best practice) ────────────────────────────────────
RUN useradd --uid 10001 --create-home --shell /bin/bash trapreceiver

WORKDIR /app

# ── Install Python dependencies ───────────────────────────────────────────────
COPY python/requirements.txt ./requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# ── Copy application source ────────────────────────────────────────────────────
COPY python/trap_receiver/ ./trap_receiver/

# ── Copy VERSION file ─────────────────────────────────────────────────────────
COPY VERSION ./VERSION

# ── Create config directory (mounted from ConfigMap in K8s) ───────────────────
RUN mkdir -p /app/config && \
    chown -R trapreceiver:trapreceiver /app

# ── Switch to non-root user ───────────────────────────────────────────────────
USER trapreceiver

# ── Expose port ───────────────────────────────────────────────────────────────
EXPOSE 8080

# ── Health check ──────────────────────────────────────────────────────────────
HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
    CMD python3 -c "import urllib.request; urllib.request.urlopen('http://localhost:8080/api/v1/health')" || exit 1

# ── Entrypoint ────────────────────────────────────────────────────────────────
# CONFIG_PATH, PORT, HOST, KAFKA_BOOTSTRAP_SERVERS, TRAP_API_KEY, DASHBOARD_API_KEY
# all injected from ConfigMap + Secret at runtime via k8s envFrom
ENV PORT=8080 \
    HOST=0.0.0.0 \
    CONFIG_PATH=/app/config/config.yaml \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

CMD ["python3", "-m", "trap_receiver.app"]
