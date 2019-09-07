with group_username as(
select 
replace(actor.id,'person:stocktwits:','') actor_id,
array_agg(distinct actor.preferredusername) list_username
from
zipped
group by 1  
)  
select 
a.actor_id,
a.actor_objecttype,
a.actor_displayname,
a.actor_preferredusername,
group_username.list_username,
object_postedtime last_post,
a.actor_followerscount,
a.actor_followingcount,
a.actor_followingstockscount,
a.actor_statusescount,
a.actor_links_href,
a.actor_links_rel,
a.actor_link,
a.actor_image,
a.tradingstrategy_assetsfrequentlytraded, 
a.tradingstrategy_approach,
a.tradingstrategy_holdingperiod,
a.tradingstrategy_experience,
a.actor_classification
from
(select 
distinct
replace(actor_id,'person:stocktwits:','') actor_id,
actor_objecttype,
actor_displayname,
actor_preferredusername,
object_postedtime,
ROW_NUMBER() OVER (PARTITION BY actor_id ORDER BY object_postedtime DESC) AS _row_desc,
actor_followerscount,
actor_followingcount,
actor_followingstockscount,
actor_statusescount,
actor_links[1].href actor_links_href,
actor_links[1].rel actor_links_rel,
actor_link,
actor_image,
tradingstrategy.assetsfrequentlytraded tradingstrategy_assetsfrequentlytraded, 
tradingstrategy.assetsfrequentlytraded tradingstrategy_approach,
tradingstrategy.holdingperiod tradingstrategy_holdingperiod,
tradingstrategy.experience tradingstrategy_experience,
actor_classification
from
(select
verb
,id
,body
,actor.id actor_id
,actor.objecttype actor_objecttype
,actor.displayname actor_displayname
,actor.preferredusername actor_preferredusername
,actor.followerscount actor_followerscount
,actor.followingcount actor_followingcount
,actor.followingstockscount actor_followingstockscount
,actor.statusescount actor_statusescount
,actor.summary actor_summary
,actor.links actor_links-- vector
,actor.link actor_link
,actor.image actor_image
,actor.tradingstrategy tradingstrategy
,actor.classification actor_classification
,object.id object_id
,object.objecttype object_objecttype
,object.postedtime object_postedtime
,object.updatedtime object_updatedtime
,object.summary object_summary
,object.link object_link
,provider.displayname provider_displayname
,provider.link provider_link
,link
,entities
,entities.symbols entities_symbols 
,entities.sentiment entities_sentiment
,entities.video entities_video
,inreplyto.objecttype inreplyto_objecttype
,inreplyto.id inreplyto_id
, sharednote.objecttype sharednote_objecttype
, sharednote.id sharednote_id
from zipped
) zipped cross join
UNNEST(entities_symbols) t(symbols)
order by 1) a
left outer join group_username
on group_username.actor_id = a.actor_id
where _row_desc = 1