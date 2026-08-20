-- RAW(VARIANT) -> STAGING(타입 캐스팅)
-- file_name(exchange_rate_YYYYMMDD.json[.gz])에서 수집 기준일을 추출한다.
-- result != '1'(API 실패/휴장 응답)은 제외하고, 같은 날짜/통화 중 가장 최근 적재분만 반영한다.

{{
    config(
        materialized="incremental",
        unique_key=["result_date", "cur_unit"],
        incremental_strategy="merge"
    )
}}

select
    to_date(regexp_substr(file_name, '[0-9]{8}'), 'YYYYMMDD') as result_date,
    raw_data:cur_unit::string as cur_unit,
    raw_data:cur_nm::string as cur_nm,
    replace(raw_data:deal_bas_r::string, ',', '')::float as rate,
    convert_timezone('UTC', current_timestamp())::timestamp_ntz as updated_at
from {{ source('raw', 'exchange_rate_raw') }}
where raw_data:result::string = '1'
qualify row_number() over (
    partition by regexp_substr(file_name, '[0-9]{8}'), raw_data:cur_unit::string
    order by loaded_at desc
) = 1
