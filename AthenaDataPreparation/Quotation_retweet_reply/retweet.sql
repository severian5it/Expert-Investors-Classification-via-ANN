select
retweeter,
displayname.actor_id retweeted,
symbol,
object_postedtime date
from
(select 
rtrim(replace(retweet,'@',''),':') retweetedname,
replace(actor_id,'person:stocktwits:','') retweeter,
symbols.symbol,
object_postedtime
from
(select
split(body,' ') body,
body body2,
actor.id actor_id,
object.postedtime object_postedtime,
entities.symbols entities_symbols 
from zipped
where  body like'%RT%@%'
) zipped 
cross join
UNNEST(entities_symbols) t(symbols)
cross join
UNNEST(body) t(retweet)
where retweet like '@%' 
and trim(retweet) <> '@')
inner join displayname
on trim(preferredusername) = trim(retweetedname)