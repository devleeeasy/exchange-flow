"""ExchangeFlow: 한국수출입은행 고시환율 일일 ELT 파이프라인.

extract -> load_to_raw -> transform_staging -> transform_mart -> dq_check
"""

from __future__ import annotations

import logging
from datetime import timedelta

import pendulum
from airflow.decorators import dag, task
from airflow.exceptions import AirflowSkipException
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator

from extract import fetch_exchange_rates, save_to_local
from load_to_snowflake import copy_into_raw, put_file_to_stage

logger = logging.getLogger(__name__)

SNOWFLAKE_CONN_ID = "snowflake_default"
DATA_DIR = "/opt/airflow/data"

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
    template_searchpath=["/opt/airflow/sql"],
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

    transform_staging = SnowflakeOperator(
        task_id="transform_staging",
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        sql="staging_transform.sql",
    )

    transform_mart = SnowflakeOperator(
        task_id="transform_mart",
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        sql="mart_transform.sql",
    )

    @task
    def dq_check(logical_date=None) -> None:
        search_date = logical_date.in_timezone("Asia/Seoul").format("YYYY-MM-DD")
        hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)

        row_count, null_count, dup_count = hook.get_first(
            f"""
            SELECT
                COUNT(*),
                COUNT_IF(rate IS NULL OR cur_unit IS NULL),
                COUNT(*) - COUNT(DISTINCT cur_unit)
            FROM MART.EXCHANGE_RATE_DAILY
            WHERE result_date = '{search_date}'
            """
        )

        if row_count == 0 or null_count > 0 or dup_count > 0:
            raise ValueError(
                f"DQ 실패 [{search_date}] rows={row_count} nulls={null_count} dups={dup_count}"
            )
        logger.info("DQ 통과 [%s] rows=%s", search_date, row_count)

    extracted_file = extract()
    loaded = load_to_raw(extracted_file)
    loaded >> transform_staging >> transform_mart >> dq_check()


exchange_rate_pipeline()
