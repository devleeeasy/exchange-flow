"""ExchangeFlow: 한국수출입은행 고시환율 일일 ELT 파이프라인.

extract -> load_to_raw -> dbt_build(staging+mart 변환 및 데이터 품질 테스트)
"""

from __future__ import annotations

import logging
from datetime import timedelta

import pendulum
from airflow.decorators import dag, task
from airflow.exceptions import AirflowSkipException
from airflow.operators.bash import BashOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

from extract import fetch_exchange_rates, save_to_local
from load_to_snowflake import copy_into_raw, put_file_to_stage

logger = logging.getLogger(__name__)

SNOWFLAKE_CONN_ID = "snowflake_default"
DATA_DIR = "/opt/airflow/data"
DBT_PROJECT_DIR = "/opt/airflow/dbt"
DBT_BIN = "/opt/dbt_venv/bin/dbt"

default_args = {
    "retries": 2,
    "retry_delay": timedelta(minutes=10),
}


@dag(
    dag_id="exchange_rate_pipeline",
    description="한국수출입은행 고시환율 일일 ELT 파이프라인",
    # 영업일 11:30 KST — API는 영업일 오전 11시 전후에 당일 고시환율을 갱신한다.
    schedule="30 11 * * 1-5",
    start_date=pendulum.datetime(2026, 8, 1, tz="Asia/Seoul"),
    catchup=False,
    default_args=default_args,
    tags=["exchangeflow"],
)
def exchange_rate_pipeline():

    @task
    def extract(logical_date=None) -> str:
        search_date = logical_date.in_timezone("Asia/Seoul").format("YYYYMMDD")
        payload = fetch_exchange_rates(search_date)
        if not payload:
            raise AirflowSkipException(f"{search_date}: 휴장일 또는 데이터 없음 — skip")
        file_path = save_to_local(payload, search_date, DATA_DIR)
        logger.info("saved %d rows -> %s", len(payload), file_path)
        return file_path

    @task
    def load_to_raw(file_path: str) -> None:
        hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
        conn = hook.get_conn()
        try:
            staged_name = put_file_to_stage(conn, file_path)
            copy_into_raw(conn, staged_name)
            logger.info("loaded %s -> RAW.EXCHANGE_RATE_RAW", staged_name)
        finally:
            conn.close()

    # STAGING/MART 변환(dbt models) + 데이터 품질 테스트(dbt tests)를 한 번에 실행.
    # dbt는 Airflow 파이썬 환경과 의존성 충돌을 피하기 위해 격리된 venv(/opt/dbt_venv)에
    # 설치돼 있다 (Dockerfile 참고). 기존 transform_staging/transform_mart/dq_check
    # 태스크를 이 하나의 태스크로 대체한다.
    dbt_build = BashOperator(
        task_id="dbt_build",
        bash_command=(
            f"{DBT_BIN} deps --project-dir {DBT_PROJECT_DIR} --profiles-dir {DBT_PROJECT_DIR} && "
            f"{DBT_BIN} build --project-dir {DBT_PROJECT_DIR} --profiles-dir {DBT_PROJECT_DIR}"
        ),
    )

    extracted_file = extract()
    loaded = load_to_raw(extracted_file)
    loaded >> dbt_build


exchange_rate_pipeline()
