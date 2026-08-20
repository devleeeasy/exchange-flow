"""ExchangeFlow 환율 모니터링 대시보드 (Streamlit + Snowflake)
------------------------------------------------------------
레이아웃 구조 (design_handoff_fx_dashboard 핸드오프 기준):
  1) 사이드바: 조회 기간 / 통화 선택 / 정규화·MA 오버레이 / 급변 알림 임계치 / 파이프라인 상태
  2) 헤더: 제목 · 데이터 신선도 배지 · CSV 다운로드
  3) 탭 - 개요: KPI 카드 · 추이 차트 · 등락률 랭킹 · 기간 요약 통계
  4) 탭 - 통화별 상세: 교차환율 계산기 · 이동평균 이격도 · 일별 변동률 히트맵
  5) 탭 - 변동성·리스크: 급변 알림 · 연율화 변동성 · 통화 간 상관관계
  6) 탭 - 데이터 & 파이프라인: 운영 카드 · 적재 이력 · 원본 데이터
  7) 푸터: 데이터 출처 · 마지막 업데이트

실행: streamlit run dashboard/app.py

한계: DAG 실행 소요시간은 Airflow 메타DB를 조회해야 하는데, 이 대시보드는
Snowflake에만 연결하므로 실제 값 대신 자리표시자를 보여준다. 누락 고시일은
공휴일 캘린더 없이 평일 기준으로만 계산한다.
"""

from __future__ import annotations

import os

import pandas as pd
import plotly.graph_objects as go
import snowflake.connector
import streamlit as st
from dotenv import load_dotenv

load_dotenv()

st.set_page_config(
    page_title="ExchangeFlow 대시보드",
    page_icon="\U0001f4b1",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.markdown(
    """
    <style>
    div[data-testid="stMetricValue"] { font-size: 1.6rem; }
    div[data-testid="stMetricLabel"] { font-size: 0.95rem; }
    </style>
    """,
    unsafe_allow_html=True,
)

UP = "#c9302c"     # rate 상승 = 원화 약세
DOWN = "#1266cc"   # rate 하락 = 원화 강세
PALETTE = ["#2563eb", "#ff4b4b", "#059669", "#d97706", "#7c3aed", "#0891b2", "#db2777", "#65a30d"]
DAG_ID = "exchange_rate_pipeline"


# ------------------------------------------------------------------
# 데이터 로딩 (Snowflake, 캐싱)
# ------------------------------------------------------------------
@st.cache_resource
def get_connection():
    return snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        warehouse=os.environ["SNOWFLAKE_WAREHOUSE"],
        database=os.environ.get("SNOWFLAKE_DATABASE", "EXCHANGEFLOW"),
        role=os.environ.get("SNOWFLAKE_ROLE"),
    )


@st.cache_data(ttl=600)
def load_data() -> pd.DataFrame:
    cur = get_connection().cursor()
    cur.execute(
        """
        SELECT d.result_date, d.cur_unit, s.cur_nm, d.rate, d.prev_rate,
               d.change_pct, d.ma_5, d.updated_at
        FROM MART.EXCHANGE_RATE_DAILY d
        LEFT JOIN (
            SELECT cur_unit, MAX(cur_nm) AS cur_nm
            FROM STAGING.EXCHANGE_RATE_STG
            GROUP BY cur_unit
        ) s USING (cur_unit)
        ORDER BY d.result_date, d.cur_unit
        """
    )
    df = cur.fetch_pandas_all()
    df["RESULT_DATE"] = pd.to_datetime(df["RESULT_DATE"])
    return df


df_all = load_data()

if df_all.empty:
    st.warning("MART.EXCHANGE_RATE_DAILY에 데이터가 없습니다. 파이프라인을 먼저 실행하세요.")
    st.stop()

cur_names = df_all.drop_duplicates("CUR_UNIT").set_index("CUR_UNIT")["CUR_NM"].to_dict()
all_codes = sorted(df_all["CUR_UNIT"].unique())
all_dates = sorted(df_all["RESULT_DATE"].unique())
latest_date = all_dates[-1]
latest_df = df_all[df_all["RESULT_DATE"] == latest_date].set_index("CUR_UNIT")


def label(cur_unit: str) -> str:
    name = cur_names.get(cur_unit)
    return f"{cur_unit}({name})" if isinstance(name, str) and name else cur_unit


def decimals_for(rate: float) -> int:
    if pd.isna(rate):
        return 2
    if rate < 10:
        return 4
    if rate < 100:
        return 3
    return 2


dec_map = {c: decimals_for(latest_df.loc[c, "RATE"]) for c in latest_df.index}


def fmt_num(value, dec: int = 2) -> str:
    if value is None or pd.isna(value):
        return "-"
    return f"{value:,.{dec}f}"


def fmt_pct(value) -> str:
    if value is None or pd.isna(value):
        return "-"
    return f"{value:+.2f}%"


def unit_of(cur_unit: str) -> int:
    return 100 if "(100)" in cur_unit else 1


def color_for(cur_unit: str, ordering: list[str]) -> str:
    idx = ordering.index(cur_unit) if cur_unit in ordering else all_codes.index(cur_unit)
    return PALETTE[idx % len(PALETTE)]


def humanize_delta(ts) -> str:
    if ts is None or pd.isna(ts):
        return "알 수 없음"
    secs = max((pd.Timestamp.utcnow().tz_localize(None) - pd.Timestamp(ts)).total_seconds(), 0)
    if secs < 3600:
        return f"{int(secs // 60)}분 전"
    if secs < 86400:
        return f"{int(secs // 3600)}시간 전"
    return f"{int(secs // 86400)}일 전"


# ------------------------------------------------------------------
# 사이드바 - 필터
# ------------------------------------------------------------------
st.sidebar.markdown("### \U0001f4b1 FX Rate Monitor")

PERIOD_WINDOWS = {"1주": 5, "1개월": 22, "3개월": 66, "1년": 250, "전체": None}
period_option = st.sidebar.radio(
    "조회 기간", list(PERIOD_WINDOWS), index=2, horizontal=True
)
window = PERIOD_WINDOWS[period_option]
dates_in_period = all_dates if window is None else all_dates[-window:]
df_period = df_all[df_all["RESULT_DATE"].isin(dates_in_period)]

default_codes = [c for c in ["USD", "EUR", "JPY(100)", "CNH"] if c in all_codes] or all_codes[
    : min(4, len(all_codes))
]
selected = st.sidebar.multiselect(
    "통화 선택", all_codes, default=default_codes, format_func=label
)
st.sidebar.caption(f"{len(selected)} / {len(all_codes)} 선택됨")

normalize = st.sidebar.checkbox("정규화 비교 (기준일 = 100)", value=False)
show_ma = st.sidebar.checkbox("5일 이동평균 오버레이", value=True)
threshold = st.sidebar.slider(
    "급변 알림 임계치", min_value=0.1, max_value=2.0, value=0.5, step=0.1, format="±%.1f%%"
)

st.sidebar.divider()
last_updated = df_all["UPDATED_AT"].max()
st.sidebar.markdown("\U0001f7e2 **파이프라인 정상**" if pd.notna(last_updated) else "\U0001f7e0 **상태 미확인**")
st.sidebar.caption(f"DAG `{DAG_ID}`")
st.sidebar.caption("MART.EXCHANGE_RATE_DAILY")
st.sidebar.caption(f"최근 적재 {last_updated}" if pd.notna(last_updated) else "최근 적재 기록 없음")

active_codes = selected or default_codes
filtered = df_period[df_period["CUR_UNIT"].isin(selected)]

# ------------------------------------------------------------------
# 헤더
# ------------------------------------------------------------------
header_l, header_r = st.columns([3, 1])
with header_l:
    st.markdown(
        "<div style='font-size:12px;font-weight:600;letter-spacing:.08em;"
        "color:#ff4b4b;text-transform:uppercase;'>한국수출입은행 고시환율</div>",
        unsafe_allow_html=True,
    )
    st.title("일별 환율 대시보드")
    st.caption(
        f"고시일자 {latest_date.strftime('%Y-%m-%d')} · {period_option} 구간 · "
        f"{len(all_codes)}개 통화 · Airflow → Snowflake ELT"
    )
with header_r:
    st.markdown(f"\U0001f7e2 데이터 신선도 **{humanize_delta(last_updated)}**")
    export_df = (filtered if selected else df_period).copy()
    export_df["CUR_UNIT"] = export_df["CUR_UNIT"].map(label)
    st.download_button(
        "CSV 다운로드",
        data=export_df.to_csv(index=False).encode("utf-8-sig"),
        file_name="exchange_rate_daily.csv",
        mime="text/csv",
    )

tab_overview, tab_detail, tab_risk, tab_pipeline = st.tabs(
    ["개요", "통화별 상세", "변동성 · 리스크", "데이터 & 파이프라인"]
)

# ------------------------------------------------------------------
# 탭 1: 개요
# ------------------------------------------------------------------
with tab_overview:
    kpi_codes = active_codes[:4]
    if kpi_codes:
        cols = st.columns(len(kpi_codes))
        for col, code in zip(cols, kpi_codes):
            if code not in latest_df.index:
                continue
            row = latest_df.loc[code]
            dec = dec_map.get(code, 2)
            delta = None
            if pd.notna(row["PREV_RATE"]):
                diff = row["RATE"] - row["PREV_RATE"]
                delta = f"{diff:+,.{dec}f} ({fmt_pct(row['CHANGE_PCT'])})"
            col.metric(label(code), fmt_num(row["RATE"], dec), delta)
            ma = row.get("MA_5")
            disp = (row["RATE"] / ma - 1) * 100 if pd.notna(ma) and ma else None
            col.caption(f"5일 MA {fmt_num(ma, dec)} · 이격도 {fmt_pct(disp)}")

    st.divider()

    chart_col, rank_col = st.columns([3, 1])
    with chart_col:
        st.subheader("정규화 추이 (기준일 = 100)" if normalize else "고시환율 추이 (KRW)")
        if not selected:
            st.info("사이드바에서 통화를 선택해주세요.")
        else:
            fig = go.Figure()
            for code in selected:
                cdf = df_period[df_period["CUR_UNIT"] == code].sort_values("RESULT_DATE")
                if cdf.empty:
                    continue
                base0 = cdf["RATE"].iloc[0]
                color = color_for(code, selected)
                y = (cdf["RATE"] / base0 * 100) if normalize and base0 else cdf["RATE"]
                fig.add_trace(
                    go.Scatter(
                        x=cdf["RESULT_DATE"], y=y, mode="lines",
                        name=label(code), line=dict(color=color, width=2),
                        hovertemplate="%{x|%Y-%m-%d}<br>%{y:,.2f}<extra>" + label(code) + "</extra>",
                    )
                )
                if show_ma:
                    ma_y = (cdf["MA_5"] / base0 * 100) if normalize and base0 else cdf["MA_5"]
                    fig.add_trace(
                        go.Scatter(
                            x=cdf["RESULT_DATE"], y=ma_y, mode="lines",
                            line=dict(color=color, width=1, dash="dot"), opacity=0.85,
                            name=f"{label(code)} MA5", showlegend=False,
                            hovertemplate="%{x|%Y-%m-%d}<br>MA5 %{y:,.2f}<extra></extra>",
                        )
                    )
            fig.update_layout(
                height=420, margin=dict(l=10, r=10, t=10, b=10),
                yaxis_title="정규화 지수 (기준=100)" if normalize else "환율",
                hovermode="x unified",
                legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
            )
            st.plotly_chart(fig, width="stretch")

    with rank_col:
        st.subheader("기간 등락률 랭킹")
        st.caption("기간 첫 고시일 대비 (+는 원화 약세)")
        rets = []
        for code in all_codes:
            cdf = df_period[df_period["CUR_UNIT"] == code].sort_values("RESULT_DATE")
            if cdf.empty:
                continue
            first, last = cdf["RATE"].iloc[0], cdf["RATE"].iloc[-1]
            if first:
                rets.append({"통화": label(code), "등락률(%)": round((last / first - 1) * 100, 2)})
        rank_df = pd.DataFrame(rets).sort_values("등락률(%)", ascending=False)
        top_bottom = pd.concat([rank_df.head(4), rank_df.tail(4)]).drop_duplicates()
        if not top_bottom.empty:
            mx = top_bottom["등락률(%)"].abs().max() or 1.0
            styled = (
                top_bottom.style.bar(
                    subset=["등락률(%)"], align="mid", vmin=-mx, vmax=mx, color=[DOWN, UP]
                ).format({"등락률(%)": "{:+.2f}%"})
            )
            st.dataframe(styled, hide_index=True, width="stretch")
        else:
            st.info("표시할 데이터가 없습니다.")

    st.divider()
    st.subheader("기간 요약 통계")
    st.caption(f"{period_option} · {len(dates_in_period)}개 고시일")
    if selected:
        rows = []
        for code in selected:
            cdf = df_period[df_period["CUR_UNIT"] == code].sort_values("RESULT_DATE")
            if cdf.empty:
                continue
            mn, mx_, avg = cdf["RATE"].min(), cdf["RATE"].max(), cdf["RATE"].mean()
            std = cdf["RATE"].std() if len(cdf) > 1 else None
            last = cdf["RATE"].iloc[-1]
            pos = ((last - mn) / (mx_ - mn) * 100) if mx_ != mn else 50.0
            rows.append(
                {
                    "통화": label(code), "최신": last, "최고": mx_, "최저": mn,
                    "평균": round(avg, 4), "표준편차": round(std, 4) if std is not None else None,
                    "변동폭(%)": round((mx_ / mn - 1) * 100, 2) if mn else None,
                    "현재 위치": round(pos, 1),
                }
            )
        summary_df = pd.DataFrame(rows)
        st.dataframe(
            summary_df, hide_index=True, width="stretch",
            column_config={
                "현재 위치": st.column_config.ProgressColumn(
                    "현재 위치", min_value=0, max_value=100, format="%.0f%%"
                ),
                "최신": st.column_config.NumberColumn(format="%.4f"),
                "최고": st.column_config.NumberColumn(format="%.4f"),
                "최저": st.column_config.NumberColumn(format="%.4f"),
                "평균": st.column_config.NumberColumn(format="%.4f"),
                "표준편차": st.column_config.NumberColumn(format="%.4f"),
                "변동폭(%)": st.column_config.NumberColumn(format="%.2f%%"),
            },
        )
    else:
        st.info("통화를 선택하면 요약 통계가 표시됩니다.")

# ------------------------------------------------------------------
# 탭 2: 통화별 상세
# ------------------------------------------------------------------
with tab_detail:
    calc_col, disp_col = st.columns([1, 2])
    with calc_col:
        st.subheader("교차환율 계산기")
        st.caption("KRW 고시환율 기준 재정환율 산출")
        base = st.selectbox("기준", all_codes, index=all_codes.index("USD") if "USD" in all_codes else 0)
        quote_options = [c for c in all_codes if c != base] or all_codes
        default_quote = "JPY(100)" if "JPY(100)" in quote_options else quote_options[0]
        quote = st.selectbox("대상", quote_options, index=quote_options.index(default_quote))

        if base in latest_df.index and quote in latest_df.index:
            b_latest, q_latest = latest_df.loc[base], latest_df.loc[quote]
            if q_latest["RATE"]:
                cross = (b_latest["RATE"] / unit_of(base)) / (q_latest["RATE"] / unit_of(quote))
                st.markdown(f"**1 {base.replace('(100)', '')} = ? {quote.replace('(100)', '')}**")
                st.markdown(f"### {cross:,.4f}")
                if pd.notna(b_latest["PREV_RATE"]) and pd.notna(q_latest["PREV_RATE"]) and q_latest["PREV_RATE"]:
                    cross_prev = (b_latest["PREV_RATE"] / unit_of(base)) / (q_latest["PREV_RATE"] / unit_of(quote))
                    if cross_prev:
                        d = (cross / cross_prev - 1) * 100
                        color = UP if d >= 0 else DOWN
                        st.markdown(
                            f"<span style='color:{color};font-weight:600;'>전일 대비 {fmt_pct(d)}</span>",
                            unsafe_allow_html=True,
                        )
        st.caption(
            "MART.EXCHANGE_RATE_DAILY의 동일 result_date rate를 나누어 계산합니다. "
            "100단위 통화(JPY(100) 등)는 100으로 보정 적용."
        )

    with disp_col:
        st.subheader("이동평균 이격도 · 추세 신호")
        st.caption("이격도 = (rate − ma_5) / ma_5")
        rows = []
        for code in active_codes:
            if code not in latest_df.index:
                continue
            row = latest_df.loc[code]
            dec = dec_map.get(code, 2)
            ma = row.get("MA_5")
            disp = (row["RATE"] / ma - 1) * 100 if pd.notna(ma) and ma else None
            if disp is None:
                signal = "데이터 부족"
            elif disp > 0.35:
                signal = "\U0001f534 과열 · 상방 이격"
            elif disp < -0.35:
                signal = "\U0001f535 과냉 · 하방 이격"
            else:
                signal = "⚪ 중립"

            cdf = df_period[df_period["CUR_UNIT"] == code].sort_values("RESULT_DATE")
            streak = 0
            for d in reversed(cdf["RATE"].diff().dropna().tolist()):
                s = 1 if d > 0 else (-1 if d < 0 else 0)
                if streak == 0:
                    streak = s
                elif s == 0 or (streak > 0) != (s > 0):
                    break
                else:
                    streak += s
            streak_label = (
                f"{'상승' if streak > 0 else '하락'} {abs(streak)}일" if streak else "-"
            )
            rows.append(
                {
                    "통화": label(code), "rate": fmt_num(row["RATE"], dec), "ma_5": fmt_num(ma, dec),
                    "이격도": fmt_pct(disp), "신호": signal, "연속 방향": streak_label,
                }
            )
        if rows:
            st.dataframe(pd.DataFrame(rows), hide_index=True, width="stretch")
        else:
            st.info("사이드바에서 통화를 선택해주세요.")

    st.divider()
    st.subheader("일별 변동률 히트맵")
    heat_dates = all_dates[-30:]
    st.caption(f"최근 {len(heat_dates)} 고시일 · change_pct · 원화 강세 ↔ 원화 약세")
    if active_codes:
        pivot = (
            df_all[df_all["CUR_UNIT"].isin(active_codes) & df_all["RESULT_DATE"].isin(heat_dates)]
            .pivot(index="CUR_UNIT", columns="RESULT_DATE", values="CHANGE_PCT")
            .reindex(active_codes)
        )
        pivot.index = [label(c) for c in pivot.index]
        pivot.columns = [d.strftime("%m/%d") for d in pivot.columns]
        fig = go.Figure(
            data=go.Heatmap(
                z=pivot.values, x=list(pivot.columns), y=list(pivot.index),
                colorscale=[[0, DOWN], [0.5, "#f5f5f5"], [1, UP]], zmid=0, zmin=-0.9, zmax=0.9,
                hovertemplate="%{y}<br>%{x}<br>%{z:+.2f}%<extra></extra>",
                colorbar=dict(title="change_pct"),
            )
        )
        fig.update_layout(height=90 + 28 * len(active_codes), margin=dict(l=10, r=10, t=10, b=10))
        st.plotly_chart(fig, width="stretch")
    else:
        st.info("사이드바에서 통화를 선택해주세요.")

# ------------------------------------------------------------------
# 탭 3: 변동성 · 리스크
# ------------------------------------------------------------------
with tab_risk:
    alert_col, vol_col = st.columns([6, 5])
    with alert_col:
        st.subheader("급변 알림")
        st.caption(f"|change_pct| ≥ {threshold:.1f}% · {period_option}")
        alerts = df_period[df_period["CHANGE_PCT"].abs() >= threshold].copy()
        alerts["ABS_PCT"] = alerts["CHANGE_PCT"].abs()
        alerts = alerts.sort_values(["RESULT_DATE", "ABS_PCT"], ascending=[False, False]).head(40)

        def grade(v: float) -> str:
            av = abs(v)
            if av >= threshold * 2:
                return "\U0001f534 심각"
            if av >= threshold * 1.4:
                return "\U0001f7e0 경고"
            return "\U0001f7e1 주의"

        rows = [
            {
                "고시일자": r["RESULT_DATE"].strftime("%Y-%m-%d"),
                "통화": label(r["CUR_UNIT"]),
                "rate": fmt_num(r["RATE"], dec_map.get(r["CUR_UNIT"], 2)),
                "전일대비": fmt_pct(r["CHANGE_PCT"]),
                "등급": grade(r["CHANGE_PCT"]),
            }
            for _, r in alerts.iterrows()
        ]
        if rows:
            st.dataframe(pd.DataFrame(rows), hide_index=True, width="stretch", height=330)
        else:
            st.info("임계치를 초과한 변동이 없습니다.")

    with vol_col:
        st.subheader("변동성 (연율화)")
        st.caption("stdev(change_pct) × √250")
        rows = []
        for code in active_codes:
            chg = df_period[df_period["CUR_UNIT"] == code]["CHANGE_PCT"].dropna()
            if len(chg) < 2:
                continue
            ann = chg.std() * (250 ** 0.5)
            tag = "\U0001f534 높음" if ann > 9 else ("\U0001f7e0 보통" if ann > 6 else "\U0001f7e2 낮음")
            rows.append({"통화": label(code), "연율화 변동성(%)": round(ann, 1), "구간": tag})
        if rows:
            vol_df = pd.DataFrame(rows).sort_values("연율화 변동성(%)", ascending=False)
            vmax = float(vol_df["연율화 변동성(%)"].max() or 1.0)
            st.dataframe(
                vol_df, hide_index=True, width="stretch",
                column_config={
                    "연율화 변동성(%)": st.column_config.ProgressColumn(
                        "연율화 변동성(%)", min_value=0.0, max_value=vmax, format="%.1f%%"
                    ),
                },
            )
        else:
            st.info("변동성 계산에는 2일 이상의 데이터가 필요합니다 (통화를 선택해주세요).")

    st.divider()
    st.subheader("통화 간 상관관계")
    st.caption(f"일간 change_pct 피어슨 상관 · {period_option}")
    corr_codes = active_codes[:7]
    if len(corr_codes) >= 2:
        wide = (
            df_period[df_period["CUR_UNIT"].isin(corr_codes)]
            .pivot(index="RESULT_DATE", columns="CUR_UNIT", values="CHANGE_PCT")
            .reindex(columns=corr_codes)
        )
        corr = wide.corr()
        corr.index = [label(c) for c in corr.index]
        corr.columns = [label(c) for c in corr.columns]

        def corr_style(v: float) -> str:
            if pd.isna(v):
                return ""
            a = min(abs(v), 1)
            bg = f"rgba(37,99,235,{0.08 + a * 0.72:.2f})" if v >= 0 else f"rgba(217,119,6,{0.08 + a * 0.72:.2f})"
            fg = "#fff" if a > 0.6 else "#31333f"
            return f"background-color:{bg}; color:{fg};"

        styled_corr = corr.style.map(corr_style).format("{:.2f}")
        st.dataframe(styled_corr, width="stretch")
    else:
        st.info("상관관계를 보려면 통화를 2개 이상 선택해주세요.")

# ------------------------------------------------------------------
# 탭 4: 데이터 & 파이프라인
# ------------------------------------------------------------------
with tab_pipeline:
    expected = len(all_codes)
    latest_count = int((df_all["RESULT_DATE"] == latest_date).sum())
    bdays = pd.bdate_range(start=df_all["RESULT_DATE"].min(), end=latest_date)
    missing_days = sorted(set(bdays.date) - {d.date() for d in all_dates})

    cols = st.columns(3)
    with cols[0]:
        st.metric("최신 고시일자", latest_date.strftime("%Y-%m-%d"))
        st.caption("MAX(result_date)")
    with cols[1]:
        st.metric("적재 행수 (당일)", str(latest_count))
        mark = "✅" if latest_count == expected else "⚠️"
        st.caption(f"{mark} 기대 {expected}행 · 결측 {max(expected - latest_count, 0)}")
    with cols[2]:
        st.metric("누락 고시일", str(len(missing_days)))
        mark = "✅" if not missing_days else "⚠️"
        st.caption(f"{mark} 평일 기준(공휴일 미반영), 데이터 구간 내")

    st.divider()
    st.subheader("적재 이력 (최근 20 고시일)")
    st.caption(f"고시일별 적재 행수 · 기대치 {expected}행")
    hist_dates = all_dates[-20:]
    counts = (
        df_all[df_all["RESULT_DATE"].isin(hist_dates)]
        .groupby("RESULT_DATE").size().reindex(hist_dates, fill_value=0)
    )
    bar_colors = [
        "#2563eb" if c == expected else ("#c9302c" if c == 0 else "#d97706") for c in counts.values
    ]
    fig = go.Figure(
        go.Bar(
            x=[d.strftime("%m/%d") for d in counts.index], y=counts.values,
            marker_color=bar_colors, hovertemplate="%{x}<br>%{y}행<extra></extra>",
        )
    )
    fig.update_layout(height=220, margin=dict(l=10, r=10, t=10, b=10), yaxis_range=[0, expected + 2])
    st.plotly_chart(fig, width="stretch")

    st.divider()
    st.subheader("원본 데이터")
    st.caption(f"MART.EXCHANGE_RATE_DAILY · {len(df_all)}행 중 최신 60행")
    raw = df_all.sort_values(["RESULT_DATE", "CUR_UNIT"], ascending=[False, True]).head(60).copy()
    raw["CUR_UNIT"] = raw["CUR_UNIT"].map(label)
    raw_display = raw[["RESULT_DATE", "CUR_UNIT", "RATE", "PREV_RATE", "CHANGE_PCT", "MA_5"]].rename(
        columns={
            "RESULT_DATE": "고시일자", "CUR_UNIT": "통화", "RATE": "매매기준율",
            "PREV_RATE": "전일 환율", "CHANGE_PCT": "전일대비(%)", "MA_5": "5일 이동평균",
        }
    )
    st.dataframe(raw_display, hide_index=True, width="stretch", height=430)

# ------------------------------------------------------------------
# 푸터
# ------------------------------------------------------------------
st.divider()
foot_l, foot_r = st.columns([2, 1])
with foot_l:
    st.caption(f"출처: 한국수출입은행 고시환율 API · 적재 Airflow(`{DAG_ID}`) → Snowflake")
with foot_r:
    st.caption(f"마지막 업데이트 {last_updated if pd.notna(last_updated) else '알 수 없음'}")
