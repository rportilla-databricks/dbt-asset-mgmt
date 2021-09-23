{{
    config(
        materialized='incremental'
    )
}}


select
    concat(date(ts), lpad(hour(ts), 2, '0'), lpad(minute(ts), 2, '0'), lpad(second(ts), 2, '0')) ts,
    ticker,
    average(bid_pr) avg_bid_pr
    average(ask_pr) avg_ask_pr
    

from asset_mgmt.positions

{% if is_incremental() %}

  -- this filter will only be applied on an incremental run
  where ts > (select max(ts) from {{ this }})

{% endif %}