-- STAGING -> MART MERGE
-- 통화별 전일 대비 변동률(change_pct)과 5영업일 이동평균(ma_5)을 계산한다.

MERGE INTO MART.EXCHANGE_RATE_DAILY tgt
USING (
    SELECT
        result_date,
        cur_unit,
        rate,
        LAG(rate) OVER (PARTITION BY cur_unit ORDER BY result_date) AS prev_rate,
        ROUND(
            (rate - LAG(rate) OVER (PARTITION BY cur_unit ORDER BY result_date))
            / LAG(rate) OVER (PARTITION BY cur_unit ORDER BY result_date) * 100,
            4
        ) AS change_pct,
        ROUND(
            AVG(rate) OVER (
                PARTITION BY cur_unit ORDER BY result_date
                ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
            ),
            4
        ) AS ma_5
    FROM STAGING.EXCHANGE_RATE_STG
) src
ON tgt.result_date = src.result_date
   AND tgt.cur_unit = src.cur_unit
WHEN MATCHED THEN UPDATE SET
    tgt.rate       = src.rate,
    tgt.prev_rate  = src.prev_rate,
    tgt.change_pct = src.change_pct,
    tgt.ma_5       = src.ma_5,
    tgt.updated_at = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN INSERT (result_date, cur_unit, rate, prev_rate, change_pct, ma_5, updated_at)
VALUES (src.result_date, src.cur_unit, src.rate, src.prev_rate, src.change_pct, src.ma_5, CURRENT_TIMESTAMP());
