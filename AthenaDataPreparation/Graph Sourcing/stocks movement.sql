with base as(
 select 
actor_id,
symbols.symbol stock,  
date(date_parse(object_postedtime, '%Y-%m-%dT%H:%i:%sZ')) tweet_dt
 from
(select
replace(actor.id,'person:stocktwits:','') actor_id,
entities.symbols entities_symbols,
object.postedtime object_postedtime
from zipped
where entities.symbols != array[]
) zipped 
cross join
UNNEST(entities_symbols) t(symbols)),
collect_base as(select base.stock,
       base.tweet_dt,
       array_agg(distinct actor_id) user_twittering_about,
       cardinality(array_agg(distinct actor_id)) distinct_twitters
from base
group by 1,2),
base_day_before as(
 select 
actor_id,
symbols.symbol stock,  
date(date_parse(object_postedtime, '%Y-%m-%dT%H:%i:%sZ')) tweet_dt
 from
(select
replace(actor.id,'person:stocktwits:','') actor_id,
entities.symbols entities_symbols,
object.postedtime object_postedtime
from zipped
where entities.symbols != array[]
) zipped 
cross join
UNNEST(entities_symbols) t(symbols)),
collect_base_day_before as(select base_day_before.stock,
       base_day_before.tweet_dt,
       array_agg(distinct actor_id) user_twittering_about,
       cardinality(array_agg(distinct actor_id)) distinct_twitters
from base_day_before
group by 1,2),
movementa as (select 
collect_base.stock,
collect_base.tweet_dt,
collect_base.user_twittering_about,
collect_base.distinct_twitters,
collect_base_day_before.user_twittering_about user_twittering_about_day_before,
collect_base_day_before.distinct_twitters distinct_twitters_day_before,
array_intersect(collect_base.user_twittering_about,collect_base_day_before.user_twittering_about) twitter_staying,
array_except(collect_base.user_twittering_about,collect_base_day_before.user_twittering_about) new_twitterer,
array_except(collect_base_day_before.user_twittering_about,collect_base.user_twittering_about) twitterer_lost
from collect_base
left outer join
collect_base_day_before
on collect_base.stock = collect_base_day_before.stock
and (collect_base.tweet_dt - interval '1' day) = collect_base_day_before.tweet_dt),
movementb as (
  select * from movementa
  )
movement as (select   
movementa.stock stock_from,
movementb.stock stock_to,
movementa.tweet_dt,
array_intersect(movementa.twitterer_lost,movementb.new_twitterer) twitterer_moving,
cardinality(array_intersect(movementa.twitterer_lost,movementb.new_twitterer)) nbr_twitterer_moving
from 
movementa
inner join
movementb
on movementa.tweet_dt = movementb.tweet_dt
and movementa.stock != movementb.stock
and arrays_overlap(movementa.twitterer_lost,movementb.new_twitterer))
select 
a.stock_id stock_from,
b.stock_id stock_to,
tweet_dt,
twitterer_moving,
nbr_twitterer_moving
from movement
left outer join stocks a
on movement.stock_from = a.symbol
left outer join stocks b
on movement.stock_to = b.symbol