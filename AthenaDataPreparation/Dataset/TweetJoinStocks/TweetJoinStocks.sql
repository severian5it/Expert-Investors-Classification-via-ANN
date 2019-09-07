with stockfinance as
(
select
name stock,
coalesce(lead(date(date_parse(date, '%Y-%m-%d')),1) over (partition by name order by date(date_parse(date, '%Y-%m-%d')) desc),date '1970-01-01') as prev_stock_dt,
date(date_parse(date, '%Y-%m-%d')) stock_dt,
lag(date(date_parse(date, '%Y-%m-%d')),1) over (partition by name order by date(date_parse(date, '%Y-%m-%d')) desc) as stock_dtafter,
lag(date(date_parse(date, '%Y-%m-%d')),5) over (partition by name order by date(date_parse(date, '%Y-%m-%d')) desc) as stock_5dtafter,
open,
close,
volume,
lag(open,1) over (partition by name order by date(date_parse(date, '%Y-%m-%d')) desc) as open1after,
lag(close,1) over (partition by name order by date(date_parse(date, '%Y-%m-%d')) desc) as close1after,
lag(volume,1) over (partition by name order by date(date_parse(date, '%Y-%m-%d')) desc) as volume1after,
lag(open,5) over (partition by name order by date(date_parse(date, '%Y-%m-%d')) desc) as open5after,
lag(close,5) over (partition by name order by date(date_parse(date, '%Y-%m-%d')) desc) as close5after,
lag(volume,5) over (partition by name order by date(date_parse(date, '%Y-%m-%d')) desc) as volume5after
from 
stocks_finance
),
tweet as
(select
replace(actor_id,'person:stocktwits:','') actor_id,
symbols.symbol stock,
case when cast(col2 as double) > 0  then 1 else 0 end pos_sentiment,
case when cast(col2 as double) < 0  then 1 else 0 end neg_sentiment,
case when lower(body) like '%bull%' then 1 else 0 end bullish_in_text,
case when lower(body) like '%bear%' then 1 else 0 end bearish_in_text,
case when entities_sentiment.basic = 'Bullish' then 1 else 0 end  bullish_self_tagged, 
case when entities_sentiment.basic = 'Bearish' then 1 else 0 end  bearish_self_tagged,  
date(date_parse(object_postedtime, '%Y-%m-%dT%H:%i:%sZ')) tweet_dt
 from
(select
id
,cast(replace(id,'tag:firehose.stocktwits.com:note/','') as integer) tweet_id
,actor.id actor_id
,body
,object.postedtime object_postedtime
,entities.symbols entities_symbols
,entities.sentiment entities_sentiment
from zipped
where entities.symbols != array[]) zipped
left outer join sentiment
on col1 = tweet_id
cross join
UNNEST(entities_symbols) t(symbols)
),
tweet_grouped_by_user as 
(
select
tweet_dt,
actor_id,
stock,
sum(pos_sentiment) positive_polarity_tweet,
sum(neg_sentiment) negative_polarity_tweet,
sum(bullish_in_text) bullish_in_text_tweet,
sum(bearish_in_text) bearish_in_text_tweet,
sum(bullish_self_tagged) bullish_self_tag_tweet, 
sum(bearish_self_tagged) bearish_self_tag_tweet,
count(*) nbr_tweet
from tweet
group by 1,2,3
)
select 
tweet_dt,
actor_id,
tweet_grouped_by_user.stock,
positive_polarity_tweet,
negative_polarity_tweet,
bullish_in_text_tweet,
bearish_in_text_tweet,
bullish_self_tag_tweet, 
bearish_self_tag_tweet,
nbr_tweet,
s.open open_same_day,
s.close close_same_day,
s.volume volume_same_day,
s.open1after open_day_after,
s.close1after close_day_after,
s.volume1after volume_day_after,
s.open5after open_5day_after,
s.close5after close_5day_after,
s.volume5after volume_5day_after,
s.stock_dt same_day,
s.stock_dtafter day_after,
s.stock_5dtafter days5_after
from tweet_grouped_by_user
inner join stockfinance s 
on  s.stock = tweet_grouped_by_user.stock
and (tweet_grouped_by_user.tweet_dt ) > s.prev_stock_dt 
and (tweet_grouped_by_user.tweet_dt ) <= s.stock_dt --we are taking upper extreme
where stock_5dtafter is not null