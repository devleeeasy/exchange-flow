# Airflow 이미지 + dbt 전용 venv
# dbt-snowflake를 Airflow 파이썬 환경(_PIP_ADDITIONAL_REQUIREMENTS)에 함께 설치하면
# 의존성 충돌 위험이 커서, /opt/dbt_venv에 격리된 venv로 따로 설치한다.
# Airflow 태스크에서는 이 venv의 dbt 바이너리를 BashOperator로 직접 호출한다.

ARG AIRFLOW_IMAGE_NAME=apache/airflow:2.10.5-python3.11
FROM ${AIRFLOW_IMAGE_NAME}

USER root
COPY dbt/requirements.txt /opt/dbt_requirements.txt
RUN python -m venv /opt/dbt_venv && \
    /opt/dbt_venv/bin/pip install --no-cache-dir -r /opt/dbt_requirements.txt && \
    chmod -R a+rX /opt/dbt_venv
USER airflow
