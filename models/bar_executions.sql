{{
    config(
        materialized='incremental', 
        incremental_strategy='merge', 
        file_format='delta' , unique_key='tt_key'    )
}}

select date, concat(ticker, ts) tt_key, ts, ticker, aggregate_qty
  from (
select date, 
    cast(concat(date(ts), ' ', lpad(hour(ts), 2, '0'), ':', '00',  ':', '00') as timestamp) ts,
    ticker,
    sum(case when side_cd = 'B' then quantity else -1*quantity end) aggregate_qty

from {{ ref('stg_executions')}}

{% if is_incremental() %}

  -- this filter will only be applied on an incremental run
  where ts > (select max(ts) from {{ this }})

{% endif %}

group by date, cast(concat(date(ts), ' ', lpad(hour(ts), 2, '0'), ':', '00'  ':', '00') as timestamp) ,
    ticker
  ) foo