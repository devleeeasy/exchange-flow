-- STAGING -> MART
-- 통화별 전일 대비 변동률(change_pct)과 5영업일 이동평균(ma_5)을 계산한다.

{{
    config(
        materialized="incremental",
        unique_key=["result_date", "cur_unit"],
        incremental_strategy="merge"
    )
}}

select
    result_date,
    cur_unit,
    rate,
    lag(rate) over (partition by cur_unit order by result_date) as prev_rate,
    round(
        (rate - lag(rate) over (partition by cur_unit order by result_date))
        / lag(rate) over (partition by cur_unit order by result_date) * 100,
        4
    ) as change_pct,
    round(
        avg(rate) over (
            partition by cur_unit order by result_date
            rows between 4 preceding and current row
        ),
        4
    ) as ma_5,
    convert_timezone('UTC', current_timestamp())::timestamp_ntz as updated_at
from {{ ref('stg_exchange_rate') }}
