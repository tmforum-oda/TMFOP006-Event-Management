# =============================================================
# trapreceivercomponent — Production Docker Image
# TMF642 Alarm Management | ODA Canvas | Kafka CloudEvent Pipeline
# =============================================================
#
# Fully self-contained -- build context is THIS folder:
#
#   cd demo-app/trapreceivercomponent
#   docker build -t trapreceivercomponent:1.1.0 .
#
# Or skip building entirely and use the public pre-built image:
#   jiotmraj/trapreceivercomponent:1.1.0 (linux/amd64 + linux/arm64)
#
# Easiest way to actually run it (Kafka + app together, no Kubernetes
# needed): see docker-compose.yml in this same folder.
# =============================================================

FROM python:3.11-slim

# ── Metadata ──────────────────────────────────────────────────────────────────
LABEL maintainer="TM Forum Reference Implementation" \
      version="1.1.0" \
      description="trapreceivercomponent — ODA Canvas Kafka CloudEvent Pipeline (TMF642)" \
      org.opencontainers.image.title="trapreceivercomponent" \
      org.opencontainers.image.version="1.1.0" \
      org.opencontainers.image.source="https://github.com/tmforum-oda/TMFOP006-Event-Management"

# ── Non-root user (security best practice) ────────────────────────────────────
# Keep the named user for image-build readability (chown, HEALTHCHECK run as
# this user), but the Deployment's securityContext/USER below both reference
# the numeric UID: Kubernetes' runAsNonRoot check reads the image config's
# User field, and a named user there isn't reliably resolved to a UID by
# every container runtime -- `USER 10001` (numeric) avoids that ambiguity.
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

# ── Switch to non-root user (numeric UID, see comment above) ──────────────────
USER 10001

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

# gunicorn, not Flask's own dev server (not meant for production use).
# 2 workers: enough for this app's request volume without over-provisioning;
# bump via --workers if traffic grows. app:main's create_app() factory is
# called through a small WSGI shim (trap_receiver.wsgi) since gunicorn needs
# a module-level app object, not a main() that calls app.run() itself.
CMD ["gunicorn", "--bind=0.0.0.0:8080", "--workers=2", "--access-logfile=-", \
     "--error-logfile=-", "trap_receiver.wsgi:app"]
