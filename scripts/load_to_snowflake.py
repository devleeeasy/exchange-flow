"""로컬 JSON 파일을 Snowflake internal stage에 올리고 RAW 테이블로 COPY INTO 한다."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

STAGE = "@RAW.RAW_STAGE"
FILE_FORMAT = "RAW.JSON_FORMAT"


def put_file_to_stage(conn: Any, local_path: str, stage: str = STAGE) -> str:
    """local_path 파일을 stage에 업로드하고, 적재된 파일명을 반환한다."""
    cur = conn.cursor()
    try:
        cur.execute(
            f"PUT file://{Path(local_path).as_posix()} {stage} "
            "AUTO_COMPRESS=TRUE OVERWRITE=TRUE"
        )
    finally:
        cur.close()
    return Path(local_path).name


def copy_into_raw(conn: Any, staged_file_name: str, stage: str = STAGE, file_format: str = FILE_FORMAT) -> None:
    """stage에 있는 파일을 RAW.EXCHANGE_RATE_RAW(raw_data VARIANT, file_name, loaded_at)로 적재한다."""
    cur = conn.cursor()
    try:
        cur.execute(
            f"""
            COPY INTO RAW.EXCHANGE_RATE_RAW (raw_data, file_name, loaded_at)
            FROM (
                SELECT $1, METADATA$FILENAME, CURRENT_TIMESTAMP()
                FROM {stage}/{staged_file_name}
            )
            FILE_FORMAT = (FORMAT_NAME = {file_format})
            ON_ERROR = 'ABORT_STATEMENT'
            """
        )
    finally:
        cur.close()


if __name__ == "__main__":
    import argparse
    import os

    import snowflake.connector

    parser = argparse.ArgumentParser(description="RAW 적재 (로컬 테스트용)")
    parser.add_argument("--file", required=True, help="적재할 로컬 JSON 파일 경로")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO)
    conn = snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        warehouse=os.environ["SNOWFLAKE_WAREHOUSE"],
        database=os.environ.get("SNOWFLAKE_DATABASE", "EXCHANGEFLOW"),
        role=os.environ.get("SNOWFLAKE_ROLE"),
    )
    try:
        staged_name = put_file_to_stage(conn, args.file)
        copy_into_raw(conn, staged_name)
        logger.info("loaded %s -> RAW.EXCHANGE_RATE_RAW", staged_name)
    finally:
        conn.close()
