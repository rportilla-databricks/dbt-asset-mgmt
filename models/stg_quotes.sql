{{
    config(
        materialized='incremental', 
        incremental_strategy='merge'
    )
}}


select
    cast(concat(date(ts), ' ', lpad(hour(ts), 2, '0'), ':', lpad(minute(ts), 2, '0'),  ':', '00') as timestamp) ts,
    ticker,
    avg(bid_pr) avg_bid_pr,
    avg(ask_pr) avg_ask_pr, 
    avg( bid_pr + ((ask_pr - bid_pr) / 2)) avg_mid_pr

from asset_mgmt.quotes
group by cast(concat(date(ts), ' ', lpad(hour(ts), 2, '0'), ':', lpad(minute(ts), 2, '0'),  ':', '00') as timestamp) ,
    ticker

{% if is_incremental() %}

  -- this filter will only be applied on an incremental run
  where ts > (select max(ts) from {{ this }})

{% endif %}