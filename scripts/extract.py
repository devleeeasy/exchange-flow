"""한국수출입은행 Open API에서 일별 고시환율을 가져와 로컬에 저장한다."""

from __future__ import annotations

import json
import logging
import os
from pathlib import Path

import requests

EXIM_API_URL = "https://oapi.koreaexim.go.kr/site/program/financial/exchangeJSON"

logger = logging.getLogger(__name__)


def fetch_exchange_rates(search_date: str, api_key: str | None = None) -> list[dict]:
    """search_date(YYYYMMDD) 기준 고시환율 목록을 조회한다.

    영업일이 아니거나 아직 고시 전(오전 11시 이전)이면 빈 리스트가 반환된다.
    """
    api_key = api_key or os.environ["EXIM_API_KEY"]
    resp = requests.get(
        EXIM_API_URL,
        params={"authkey": api_key, "searchdate": search_date, "data": "AP01"},
        timeout=10,
    )
    resp.raise_for_status()
    return resp.json()


def save_to_local(payload: list[dict], search_date: str, data_dir: str) -> str:
    Path(data_dir).mkdir(parents=True, exist_ok=True)
    file_path = Path(data_dir) / f"exchange_rate_{search_date}.json"
    file_path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
    return str(file_path)


if __name__ == "__main__":
    import argparse
    import datetime

    parser = argparse.ArgumentParser(description="환율 데이터 수집 (로컬 테스트용)")
    parser.add_argument("--date", default=datetime.date.today().strftime("%Y%m%d"))
    parser.add_argument("--out-dir", default="./data")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO)
    data = fetch_exchange_rates(args.date)
    if not data:
        logger.warning("%s: 응답 없음 (휴장일이거나 아직 고시 전일 수 있음)", args.date)
    else:
        path = save_to_local(data, args.date, args.out_dir)
        logger.info("saved %d rows -> %s", len(data), path)
