-- MART.EXCHANGE_RATE_DAILY의 최신 고시일이 너무 오래되지 않았는지 확인한다.
-- 평일 스케줄 + 주말 미고시를 감안해 4일(영업일 3일 + 주말) 이상 벌어지면 실패.
-- (기존 Python dq_check의 "당일 행 0건" 체크를 대체)

select max(result_date) as latest_result_date
from {{ ref('exchange_rate_daily') }}
having max(result_date) < dateadd(day, -4, current_date())
   or max(result_date) is null
