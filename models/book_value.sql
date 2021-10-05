{{
    config(
        materialized='incremental', 
        incremental_strategy='merge', 
        file_format='delta',
            unique_key='ticker'
    )
}}

select  ticker, ts, sum(shares) shares, sum(shares)*max(value) value
from 
(
(select ticker, ts, shares, null value
from positions

  -- this filter will only be applied on an incremental run
 {% if is_incremental() %}

  -- this filter will only be applied on an incremental run
  where ts > (select max(ts) from {{ this }})

{% endif %}


) 
union all 
(select e.ticker, e.ts, aggregate_qty shares, avg_mid_pr value
from (select * from {{ ref('bar_executions')}}
{% if is_incremental() %}

  -- this filter will only be applied on an incremental run
  where ts > (select max(ts) from {{ ref('bar_executions')}})

{% endif %}


) e 
join  (select * from {{ ref('bar_quotes')}}

{% if is_incremental() %}

  -- this filter will only be applied on an incremental run
  where ts > (select max(ts) from {{ ref('bar_quotes')}})

{% endif %}

) q 
on e.ticker = q.ticker 
and e.ts = q.ts ) 
) foo
group by ticker, ts
