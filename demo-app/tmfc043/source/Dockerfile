# TMFC043 fault management component.
# Build for the EKS nodes (amd64) even on an Apple Silicon laptop:
#   DOCKER_BUILDKIT=0 docker build --platform linux/amd64 -t <dockerhub-user>/tmfc043:<version> .
FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1 PIP_NO_CACHE_DIR=1
WORKDIR /app

COPY requirements.txt .
RUN pip install -r requirements.txt

COPY tmfc043/ .
RUN chmod -R a+rX /app && useradd --uid 10001 --no-create-home appuser
USER 10001

EXPOSE 8080
ENTRYPOINT ["python", "app.py"]
