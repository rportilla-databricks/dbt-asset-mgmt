{{
    config(
        materialized='incremental', 
        incremental_strategy='merge', 
        file_format='delta',
            unique_key='tt_key'
    )
}}

select tt_key, ticker, ts, sum(shares) shares, sum(value) value 
from (
select a.tt_key, a.ticker, a.ts, shares, shares*avg_mid_pr value
from {{ref('positions_shares')}} a join {{ref('bar_quotes')}} b
on a.tt_key = b.tt_key ) foo 
group by tt_key, ticker, ts