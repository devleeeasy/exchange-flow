# ExchangeFlow 개발 계획서

매일 환율 데이터를 자동 수집·적재·변환하는 ELT 파이프라인 — Docker·Airflow·Snowflake 기본기 증명용 미니 프로젝트

| 항목 | 내용 |
|---|---|
| 기간 | 1~2주 |
| 스택 | Docker / Airflow / Snowflake / Python |
| 데이터 소스 | 한국수출입은행 Open API |

## 아키텍처

```
extract (API 호출)
    -> load_to_raw (Stage PUT + COPY INTO RAW.EXCHANGE_RATE_RAW, VARIANT)
    -> transform_staging (MERGE, 타입 캐스팅)
    -> transform_mart (MERGE, 전일 대비 변동률 + 5영업일 이동평균)
    -> dq_check (건수/null/중복 체크)
```

Snowflake RAW → STAGING → MART 3단 스키마, 오케스트레이션은 Airflow DAG,
실행 환경은 Docker Compose(Airflow Webserver·Scheduler + Postgres 메타DB, LocalExecutor).

## 초기 설계 검토에서 반영한 사항

1. **스케줄 09:00 → 11:30 KST(평일)** — 한국수출입은행 API는 영업일 오전 11시 전후에
   당일 고시환율이 갱신됨. 휴장일에는 빈 응답이 오므로 실패가 아닌 skip으로 처리.
2. **RAW를 VARIANT 기반으로** — API 응답 JSON을 그대로 적재해 Snowflake 반정형 데이터
   적재 패턴을 보여줌 (`STRIP_OUTER_ARRAY`로 통화별 1행).
3. **적재 방식: Stage + COPY INTO** — Python Connector로 단순 INSERT하는 대신 내부
   Stage에 PUT 후 COPY INTO.
4. **STAGING/MART는 MERGE 기반 upsert** — `(result_date, cur_unit)` 키로 멱등성 확보,
   재실행/backfill 시 중복 없음.
5. **Snowflake 자격증명은 Airflow Connection(`snowflake_default`)으로 관리** — 코드에
   노출되지 않고 메타DB에 암호화 저장.
6. **Docker Compose는 LocalExecutor로 단순화** — CeleryExecutor+Worker는 미니 프로젝트
   범위에 과함. 분산 실행은 확장 아이디어로 남김.

## Snowflake 스키마

- `RAW.EXCHANGE_RATE_RAW(raw_data VARIANT, file_name STRING, loaded_at TIMESTAMP_NTZ)`
- `STAGING.EXCHANGE_RATE_STG(result_date DATE, cur_unit STRING, cur_nm STRING, rate FLOAT, updated_at TIMESTAMP_NTZ)`
- `MART.EXCHANGE_RATE_DAILY(result_date DATE, cur_unit STRING, rate FLOAT, prev_rate FLOAT, change_pct FLOAT, ma_5 FLOAT, updated_at TIMESTAMP_NTZ)`

## 단계별 일정

| 단계 | 내용 | 기간 |
|---|---|---|
| 1 | API 키 발급, 응답 구조 파악 | 1일 |
| 2 | Docker Compose로 Airflow 환경 구성 | 1~2일 |
| 3 | Snowflake 계정/DB/스키마/테이블 설계 | 1일 |
| 4 | Extract → Stage → RAW 적재 DAG 작성 | 2일 |
| 5 | STAGING/MART 변환 SQL 작성 | 2일 |
| 6 | 데이터 품질 체크, 에러 핸들링 | 1일 |
| 7 | README, 아키텍처 다이어그램 정리 | 1일 |

## 확장 아이디어

- 여러 통화 동시 비교, 변동률 임계치 초과 Slack 알림
- ~~Streamlit 대시보드~~ — 구현 완료 (`dashboard/app.py`)
- CeleryExecutor + Worker 분산 실행
- ~~dbt로 STAGING/MART 변환 이관~~ — 구현 완료 (`dbt/`, Dockerfile 격리 venv)

## 운영 배포

로컬 개발/검증을 우선 진행하고, 운영 배포(상시 호스팅)는 이후 단계에서 결정.
후보로 Oracle/AWS 프리티어 VM(비용 0원, docker-compose.yml 그대로 이전)을 우선
검토했고, Railway(상시 비용 발생하지만 배포 DX 우수)는 차선책, Vercel은 상시
멀티 컨테이너 서비스를 지원하지 않아 제외.

## 리스크 및 유의사항

- API 업데이트 시각(영업일 11시 전후), 주말·공휴일 무응답 — 스케줄/skip 로직에 반영
- Snowflake 무료 체험 기간·크레딧 한도 — 웨어하우스 `AUTO_SUSPEND`로 절약
- 자격증명 보안 — `.env`/키 파일은 `.gitignore`, `.env.example`만 커밋
- 재실행 멱등성 — MERGE 키 충돌 없이 안전하게 덮어써지는지 사전 테스트
