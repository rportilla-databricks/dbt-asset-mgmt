{{
    config(
        materialized='incremental', 
        incremental_strategy='merge', 
        file_format='delta' , unique_key='tt_key')
}}

select date, tt_key, ts, ticker, sum(shares) shares
from (
select date, tt_key, ts, ticker, sum(aggregate_qty) over (partition by ticker order by ts) shares
from 
{{ ref('bar_executions')}}
{% if is_incremental() %}

  -- this filter will only be applied on an incremental run
  where ts >= (select max(ts) from {{this}})

{% endif %}
) foo 
group by date, tt_key, ts, ticker