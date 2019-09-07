select 
replace(actor_id,'person:stocktwits:','') reply_from,
replace(replied_id,'person:stocktwits:','') reply_to,
symbols.symbol,
object_postedtime
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