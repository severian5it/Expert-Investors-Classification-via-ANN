select
quoter,
displayname.actor_id quoted,
symbol,
object_postedtime date
from
(select 
rtrim(replace(retweet,'@',''),':') quotedname,
replace(actor_id,'person:stocktwits:','') quoter,
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
where  body like'%@%'
and body not like'%RT%@%'
) zipped 
cross join
UNNEST(entities_symbols) t(symbols)
cross join
UNNEST(body) t(retweet)
where retweet like '@%' 
and trim(retweet) <> '@')
inner join displayname
on trim(preferredusername) = trim(quotedname)