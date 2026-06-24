ARG AIRFLOW_BASE_IMAGE=apache/airflow:2.9.1
FROM ${AIRFLOW_BASE_IMAGE}

ARG DATAFORM_CLI_VERSION=latest

USER root

RUN apt-get update \
    && apt-get install -y --no-install-recommends curl ca-certificates gnupg \
    && curl -fsSL https://deb.nodesource.com/setup_20.x | bash - \
    && apt-get install -y --no-install-recommends nodejs \
    && npm install -g @dataform/cli@${DATAFORM_CLI_VERSION} \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow
