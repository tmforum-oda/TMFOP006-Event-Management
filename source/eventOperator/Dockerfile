FROM python:3.12-slim

WORKDIR /src

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY eventManagementOperator.py adapter_manager.py kafka_user.py kafka_utils.py topic_naming.py utils.py log_wrapper.py ./

RUN useradd --uid 10001 --create-home eventmgmtop
USER 10001

ARG CICD_BUILD_TIME
ARG GIT_COMMIT_SHA
ENV CICD_BUILD_TIME=$CICD_BUILD_TIME \
    GIT_COMMIT_SHA=$GIT_COMMIT_SHA

# The Helm chart overrides this command to pass its own namespace flags.
CMD ["kopf", "run", "--all-namespaces", "--liveness=http://0.0.0.0:8080/healthz", "--verbose", "/src/eventManagementOperator.py"]
