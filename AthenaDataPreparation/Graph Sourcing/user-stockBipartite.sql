select 
actor_id user, 
(stocks.id + 1000000) stock,
date(date_parse(object_postedtime, '%Y-%m-%dT%H:%i:%sZ')) date,
count(distinct tweet_id) tweet_count
from
(select 
actor_id,
symbols.symbol,  
object_postedtime,
tweet_id
 from
(select
replace(actor.id,'person:stocktwits:','') actor_id,
replace(id,'tag:firehose.stocktwits.com:note/','') tweet_id,
entities.symbols entities_symbols,
object.postedtime object_postedtime
from zipped
where entities.symbols != array[]
-- join on stocks to retrieve financial measures. join on tweet_id for sentiment
) zipped 
cross join
UNNEST(entities_symbols) t(symbols)) network
left outer join stocks
on network.symbol = stocks.symbol
where stocks.id  is not null
group by 1,2,3