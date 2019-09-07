select 
replace(actor_id,'person:stocktwits:','') actor_id,
replace(id,'tag:firehose.stocktwits.com:note/','') tweet_id,
--symbols.symbol,
body,
case when lower(body) like '%bull%' then 1 else 0 end bullish,
case when lower(body) like '%bear%' then 1 else 0 end bearish,
entities_sentiment.basic sentiment,  
object_postedtime
from
(select *
 from
(select
id
,body
,actor.id actor_id
,object.postedtime object_postedtime
--,entities.symbols entities_symbols
,entities.sentiment entities_sentiment
from zipped
where entities.symbols != array[] 
) zipped 
--cross join
--UNNEST(entities_symbols) t(symbols) 
) tweet