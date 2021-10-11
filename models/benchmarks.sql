select x.*, dollar_value / agg_quantity vwap
from (select ticker, event_ts, sum(size) over (partition by ticker order by event_ts) agg_quantity, sum(size*price) over (partition by ticker order by event_ts) dollar_value,
 avg(price) over (partition by ticker order by cast(event_ts as integer) range between 20 preceding and current row) sma
from {{ref('stg_market_trades')}} 
) x