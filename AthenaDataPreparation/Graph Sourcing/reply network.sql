// network coming from reply, retweets and message are in different query.

select 
replace(actor_id,'person:stocktwits:','') actor_id,
symbols.symbol,
object_postedtime,
replace(replied_id,'person:stocktwits:','') replied_id
from
(
select
a.actor.id actor_id
,a.entities.symbols entities_symbols 
,b.actor.id replied_id
,a.object.postedtime object_postedtime
from zipped a 
inner join 
zipped b
on a.inreplyto.id  is not null
and a.inreplyto.id  = b.id
) zipped 
cross join
UNNEST(entities_symbols) t(symbols)