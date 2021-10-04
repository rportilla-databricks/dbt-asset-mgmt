{{
    config(
        materialized='incremental', 
        incremental_strategy='merge'
    )
}}


select
    cast(concat(date(ts), ' ', lpad(hour(ts), 2, '0'), ':', lpad(minute(ts), 2, '0'),  ':', '00') as timestamp) ts,
    ticker,
    sum(case when side_cd = 'B' then quantity else -1*quantity end quantity) aggregate_qty

from asset_mgmt.trades
group by cast(concat(date(ts), ' ', lpad(hour(ts), 2, '0'), ':', lpad(minute(ts), 2, '0'),  ':', '00') as timestamp) ,
    ticker

{% if is_incremental() %}

  -- this filter will only be applied on an incremental run
  where ts > (select max(ts) from {{ this }})

{% endif %}