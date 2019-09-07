/* pulling out retweets( those starting with RT) and message to other user, just @, missing replyto, with different query
** some users are missing. Maybe deleted? I should loook better into Siobhan thesis
***/

select
displayname.actor_id retweeted_id,
retweet.actor_id,
symbol,
object_postedtime
from
(
select 
rtrim(replace(retweet,'@',''),':') retweetname,
replace(actor_id,'person:stocktwits:','') actor_id,
symbols.symbol,
object_postedtime
from
(

(select
verb
,id
,split(body,' ') body
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
where body like'%@%'
--limit 1000
) zipped 
cross join
UNNEST(entities_symbols) t(symbols)
cross join
UNNEST(body) t(retweet))
where retweet like '@%' 
and trim(retweet) <> '@' 
) retweet
left outer join displayname
on trim(preferredusername) = trim(retweetname)