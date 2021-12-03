--select read_ts, ticker, avg(sentiment.compound) from {{ref('stg_sentiment')}} group by read_ts, ticker

select * from {{ref('stg_sentiment')}} a join {{ref('book_value')}} b 

on a.read_ts = b.ts

--select distinct read_ts from {{ref('stg_sentiment')}}