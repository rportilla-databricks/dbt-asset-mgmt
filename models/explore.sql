
select b.ts, b.ticker, avg(a.sentiment.compound) sentiment  from {{ref('stg_sentiment')}} a join {{ref('book_value')}} b 
on a.read_ts = b.ts
and a.ticker = b.ticker
group by b.ts, b.ticker

--select distinct read_ts from {{ref('stg_sentiment')}}