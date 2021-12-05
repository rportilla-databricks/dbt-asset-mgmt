with latest as (select last(shares) over (partition by ticker order by ts desc) last_s, last(value) over (partition by ticker order by ts desc) last_v from {{ref('book_value')}})
select b.ts, b.ticker, last_v value, last_s shares, avg(a.sentiment.compound) sentiment  
from {{ref('stg_sentiment')}} a join 
{{ref('book_value')}} b 
on a.read_ts = b.ts
and a.ticker = b.ticker
join latest
group by b.ts, b.ticker, last_v, last_s 