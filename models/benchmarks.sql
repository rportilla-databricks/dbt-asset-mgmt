select
  x.*,
  dllr_value / volume vwap,
  volume total_volume,
  num_block_trades
from
  (
    select
      ticker,
      date_trunc('MINUTE', event_ts) ts,
      sum(dollar_value) dllr_value,
      sum(agg_quantity) volume,
      sum(case when agg_quantity >= 10000 then 1 else 0 end) num_block_trades,
      count(agg_quantity) num_trades
    from
          (
            select
              ticker,
              event_ts,
              sum(size) over (
                partition by ticker
                order by
                  event_ts
              ) agg_quantity,
              sum(size * price) over (
                partition by ticker
                order by
                  event_ts
              ) dollar_value,
              avg(price) over (
                partition by ticker
                order by
                  cast(event_ts as integer) range between 20 preceding
                  and current row
              ) sma
            from
              ref({{'stg_market_trades'}}) 
          ) x
        group by
          ticker,
          ts
      ) x