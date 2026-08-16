-- RAW(VARIANT) -> STAGING(타입 캐스팅) MERGE
-- file_name(exchange_rate_YYYYMMDD.json[.gz])에서 수집 기준일을 추출한다.
-- result != '1' (API 실패/휴장 응답)은 제외하고, 같은 날짜/통화 중 가장 최근 적재분만 반영한다.

MERGE INTO STAGING.EXCHANGE_RATE_STG tgt
USING (
    SELECT
        TO_DATE(REGEXP_SUBSTR(file_name, '\d{8}'), 'YYYYMMDD') AS result_date,
        raw_data:cur_unit::STRING AS cur_unit,
        raw_data:cur_nm::STRING AS cur_nm,
        REPLACE(raw_data:deal_bas_r::STRING, ',', '')::FLOAT AS rate,
        loaded_at
    FROM RAW.EXCHANGE_RATE_RAW
    WHERE raw_data:result::STRING = '1'
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY REGEXP_SUBSTR(file_name, '\d{8}'), raw_data:cur_unit::STRING
        ORDER BY loaded_at DESC
    ) = 1
) src
ON tgt.result_date = src.result_date
   AND tgt.cur_unit = src.cur_unit
WHEN MATCHED THEN UPDATE SET
    tgt.cur_nm     = src.cur_nm,
    tgt.rate       = src.rate,
    tgt.updated_at = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN INSERT (result_date, cur_unit, cur_nm, rate, updated_at)
VALUES (src.result_date, src.cur_unit, src.cur_nm, src.rate, CURRENT_TIMESTAMP());
