"""Streamlit 대시보드: MART.EXCHANGE_RATE_DAILY 조회 UI.

실행: streamlit run dashboard/app.py
"""

from __future__ import annotations

import os

import pandas as pd
import snowflake.connector
import streamlit as st
from dotenv import load_dotenv

load_dotenv()

st.set_page_config(page_title="ExchangeFlow 대시보드", layout="wide")


@st.cache_resource
def get_connection():
    return snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        warehouse=os.environ["SNOWFLAKE_WAREHOUSE"],
        database=os.environ.get("SNOWFLAKE_DATABASE", "EXCHANGEFLOW"),
        role=os.environ.get("SNOWFLAKE_ROLE"),
        schema="MART",
    )


@st.cache_data(ttl=300)
def load_data() -> pd.DataFrame:
    cur = get_connection().cursor()
    cur.execute(
        """
        SELECT result_date, cur_unit, rate, prev_rate, change_pct, ma_5
        FROM MART.EXCHANGE_RATE_DAILY
        ORDER BY result_date
        """
    )
    df = cur.fetch_pandas_all()
    df["RESULT_DATE"] = pd.to_datetime(df["RESULT_DATE"]).dt.date
    return df


st.title("ExchangeFlow 대시보드")
st.caption("한국수출입은행 고시환율 · Airflow → Snowflake ELT 결과 (MART.EXCHANGE_RATE_DAILY)")

df = load_data()

if df.empty:
    st.warning("MART.EXCHANGE_RATE_DAILY에 데이터가 없습니다. 파이프라인을 먼저 실행하세요.")
    st.stop()

currencies = sorted(df["CUR_UNIT"].unique())
selected = st.multiselect("통화 선택", currencies, default=currencies[: min(3, len(currencies))])

date_min, date_max = df["RESULT_DATE"].min(), df["RESULT_DATE"].max()
start, end = st.slider("기간", min_value=date_min, max_value=date_max, value=(date_min, date_max))

filtered = df[
    df["CUR_UNIT"].isin(selected) & df["RESULT_DATE"].between(start, end)
]

latest_date = df["RESULT_DATE"].max()
st.subheader(f"최신 고시환율 ({latest_date})")
latest = df[df["RESULT_DATE"] == latest_date].sort_values("CUR_UNIT")
for col, (_, row) in zip(st.columns(max(len(latest), 1)), latest.iterrows()):
    col.metric(
        row["CUR_UNIT"],
        f"{row['RATE']:.2f}",
        f"{row['CHANGE_PCT']:.2f}%" if pd.notna(row["CHANGE_PCT"]) else None,
    )

if not selected:
    st.info("통화를 하나 이상 선택하세요.")
else:
    st.subheader("환율 추이")
    st.line_chart(filtered.pivot(index="RESULT_DATE", columns="CUR_UNIT", values="RATE"))

    st.subheader("5영업일 이동평균 (ma_5)")
    st.line_chart(filtered.pivot(index="RESULT_DATE", columns="CUR_UNIT", values="MA_5"))

st.subheader("상세 데이터")
st.dataframe(
    filtered.sort_values(["RESULT_DATE", "CUR_UNIT"], ascending=[False, True]),
    use_container_width=True,
)
