{{
    config(
        materialized='incremental', 
        incremental_strategy='merge', 
                file_format='delta', unique_key='tt_key'   )
}}

select date, concat(ticker, ts) tt_key, ts, ticker, avg_bid_pr, avg_ask_pr, avg_mid_pr 
  from (
select date,
    cast(concat(date(ts), ' ', lpad(hour(ts), 2, '0'), ':', '00',  ':', '00') as timestamp) ts,
    ticker,
    avg(bid_pr) avg_bid_pr,
    avg(ask_pr) avg_ask_pr, 
    avg( bid_pr + ((ask_pr - bid_pr) / 2)) avg_mid_pr

from {{ ref('stg_quotes')}}

{% if is_incremental() %}

  -- this filter will only be applied on an incremental run
  where ts > (select max(ts) from {{ this }})

{% endif %}

group by date, cast(concat(date(ts), ' ', lpad(hour(ts), 2, '0'), ':', lpad(minute(ts), 2, '0'),  ':', '00') as timestamp) ,
    ticker) foo