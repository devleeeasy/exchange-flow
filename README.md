# ExchangeFlow

매일 환율 데이터를 자동 수집·적재·변환하고 대시보드로 시각화하는 ELT 파이프라인

Docker(Airflow) → Snowflake RAW(VARIANT) → STAGING(캐스팅) → MART(변동률·이동평균) → Streamlit 대시보드

## 목차

- [아키텍처](#아키텍처)
- [사전 준비](#사전-준비)
- [로컬 실행](#로컬-실행)
- [대시보드](#대시보드)
- [폴더 구조](#폴더-구조)
- [기술 스택](#기술-스택)
- [운영 배포](#운영-배포)

## 아키텍처

```mermaid
flowchart TD
    API["한국수출입은행<br/>Open API"] -->|JSON 응답| extract

    subgraph Airflow["Docker Compose · Airflow (LocalExecutor)"]
        extract["extract<br/>API 호출 → 로컬 JSON"] --> load_to_raw["load_to_raw<br/>Stage PUT + COPY INTO"]
        load_to_raw --> transform_staging["transform_staging<br/>MERGE · 타입 캐스팅"]
        transform_staging --> transform_mart["transform_mart<br/>MERGE · 변동률 + 5일 이동평균"]
        transform_mart --> dq_check["dq_check<br/>건수 / NULL / 중복 검증"]
    end

    subgraph Snowflake["Snowflake"]
        RAW[("RAW.EXCHANGE_RATE_RAW<br/>VARIANT")] --> STAGING[("STAGING.EXCHANGE_RATE_STG")] --> MART[("MART.EXCHANGE_RATE_DAILY")]
    end

    Dashboard["dashboard/app.py<br/>Streamlit 대시보드"]

    load_to_raw -.-> RAW
    transform_staging -.-> STAGING
    transform_mart -.-> MART
    dq_check -.검증.-> MART
    MART -.조회.-> Dashboard
```

스케줄: 평일 11:30 KST (`30 11 * * 1-5`, Asia/Seoul) — 한국수출입은행 API는
영업일 오전 11시 전후 당일 고시환율을 갱신하며, 휴장일에는 빈 응답을 반환한다.
이 경우 `extract` 태스크가 실패가 아닌 skip으로 처리된다 (실행 로그로 확인됨).

MERGE 기반 upsert(`(result_date, cur_unit)` 키)로 STAGING/MART를 적재하므로
동일 날짜를 재실행하거나 backfill해도 중복 없이 안전하다.

## 사전 준비

1. **한국수출입은행 Open API 키** 발급: https://www.koreaexim.go.kr/ir/HPHKIR019M01
2. **Snowflake 계정** (무료 체험 가능) — 아래 SQL로 초기 스키마 생성

```sql
-- Snowflake Worksheet에서 실행
!source sql/create_tables.sql
```

## 로컬 실행

```bash
cp .env.example .env
# .env에 EXIM_API_KEY, SNOWFLAKE_* 값 채우기

docker compose up -d
```

Airflow 웹 UI: http://localhost:8080 (기본 계정 admin/admin, `.env`에서 변경 가능)

### Snowflake Connection 등록 (Airflow UI)

Admin → Connections → `+` 로 아래 값 등록:

| 필드 | 값 |
|---|---|
| Connection Id | `snowflake_default` |
| Connection Type | Snowflake |
| Schema | `RAW` |
| Login | `SNOWFLAKE_USER` |
| Password | `SNOWFLAKE_PASSWORD` |
| Account | `SNOWFLAKE_ACCOUNT` |
| Extra | `{"warehouse": "EXCHANGEFLOW_WH", "database": "EXCHANGEFLOW", "role": "<role>"}` |

등록 후 DAG `exchange_rate_pipeline`을 Unpause하고 Trigger로 수동 실행해 확인한다.

### 실행 확인

`extract → load_to_raw → transform_staging → transform_mart → dq_check` 전 구간을
로컬 Docker Compose 환경에서 end-to-end로 실행해 검증했다.

```
[dq_check] DQ 통과 [2026-08-14] rows=23
```

`airflow dags backfill`로 2026-08-10 ~ 2026-08-14(영업일 5일)을 채워 넣어
MART의 `ma_5`(5영업일 이동평균)가 실제 5일치 데이터로 계산되는 것도 확인했다
(예: USD 기준 2026-08-14 `ma_5=1416.06`).

휴장일(주말)에 스케줄 실행되면 `extract`가 API 빈 응답을 감지해 실패 없이 skip
처리되는 것도 함께 확인했다.

### 스크립트 단독 테스트 (Airflow 없이)

```bash
pip install -r requirements.txt

python scripts/extract.py --date 20260814
python scripts/load_to_snowflake.py --file data/exchange_rate_20260814.json
```

## 대시보드

MART.EXCHANGE_RATE_DAILY를 조회하는 Streamlit 환율 모니터링 대시보드. 4개 탭으로 구성:

- **개요** — KPI 카드, 추이 차트(정규화/5일 MA 오버레이), 기간 등락률 랭킹, 기간 요약 통계
- **통화별 상세** — 교차환율 계산기, 이동평균 이격도·추세 신호, 일별 변동률 히트맵
- **변동성 · 리스크** — 급변 알림(임계치 슬라이더), 연율화 변동성, 통화 간 상관관계
- **데이터 & 파이프라인** — 운영 카드(적재 행수/누락 고시일), 적재 이력, 원본 데이터

```bash
pip install -r requirements.txt
streamlit run dashboard/app.py
```

`.env`의 `SNOWFLAKE_*` 값을 그대로 사용한다 (로컬 스크립트 단독 실행과 동일).
`.streamlit/config.toml`로 테마(accent `#ff4b4b`)를 지정했다.

한계: 누락 고시일은 공휴일 캘린더 없이 평일 기준으로만 계산한다.

## 폴더 구조

```
exchangeflow/
├── dags/
│   └── exchange_rate_pipeline.py
├── scripts/
│   ├── extract.py
│   └── load_to_snowflake.py
├── dashboard/
│   └── app.py            # Streamlit 대시보드
├── .streamlit/
│   └── config.toml       # 대시보드 테마
├── sql/
│   ├── create_tables.sql
│   ├── staging_transform.sql
│   └── mart_transform.sql
├── docs/
│   └── PLAN.md          # 개발 계획서 (설계 의도, 단계별 일정)
├── docker-compose.yml
├── requirements.txt
├── .env.example
└── README.md
```

## 기술 스택

Docker Compose · Apache Airflow(LocalExecutor) · Snowflake · Python · Streamlit · Plotly · pandas

## 운영 배포

로컬 개발/검증을 우선 진행 중이며, 운영 배포(상시 호스팅)는 이후 단계에서 진행 예정.
