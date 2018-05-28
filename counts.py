from datetime import datetime, timedelta
from dateutil import tz

from sqlalchemy import or_, and_

from fintweet.models import Session, Tweet, TweetCashtags, TweetHashtags, TweetMentions

session = Session()

ZONE_NY = tz.gettz('America/New_York')
ZONE_UTC = tz.gettz('UTC')


def convert_date(input_dt, zone_from=ZONE_NY, zone_to=ZONE_UTC):
    utc_datetime = datetime.strptime(input_dt, '%Y-%m-%d %H:%M:%S')
    # Tell the datetime object that it's in NY time zone since datetime objects are 'naive' by default
    utc_datetime = utc_datetime.replace(tzinfo=zone_from)
    return utc_datetime.astimezone(zone_to)


def get_period(date, period_type=0):
    if period_type == 0:
        period_start = date + ' 09:30:00'
        period_end = date + ' 16:00:00'
    elif period_type == -1:
        period_start = date + ' 00:00:00'
        period_end = date + ' 09:30:00'
    elif period_type == 1:
        period_start = date + ' 16:00:00'
        period_end = (datetime.strptime(date, '%Y-%m-%d') + timedelta(days=1)).strftime("%Y-%m-%d %H:%M:%S")
    return {'start': convert_date(period_start), 'end': convert_date(period_end)}


def get_tweets_in_period(c_tag, date, period_type=0):
    period = get_period(date, period_type)
    if period_type in (0, -1):
        filter_period = and_(
            Tweet.date == period['start'].date(),
            Tweet.time >= period['start'].time(),
            Tweet.time < period['end'].time())
    elif period_type == 1:
        start = and_(Tweet.date == period['start'].date(), Tweet.time >= period['start'].time()).self_group()
        end = and_(Tweet.date == period['end'].date(), Tweet.time < period['end'].time()).self_group()
        filter_period = or_(start, end)
    tweets = session.query(TweetCashtags.tweet_id).join(Tweet) \
        .filter(TweetCashtags.cashtags == c_tag) \
        .filter(filter_period)
    return [t[0] for t in tweets.all()]


def get_users_count(tweet_list):
    q = session.query(Tweet.user_id).filter(Tweet.tweet_id.in_(tweet_list)).group_by(Tweet.user_id)
    return q.count()


def get_retweet_count(tweet_list):
    q = session.query(Tweet.tweet_id).filter(Tweet.tweet_id.in_(tweet_list)).filter(Tweet.retweet_status is True)
    return q.count()


def get_hashtag_count(tweet_list):
    q = session.query(TweetHashtags.hashtags).filter(TweetHashtags.tweet_id.in_(tweet_list))
    return q.count()


def get_replys_count(tweet_list):
    q = session.query(Tweet.tweet_id).filter(Tweet.tweet_id.in_(tweet_list)).filter(Tweet.reply_to > 0)
    return q.count()


def get_mentions_count(tweet_list):
    q = session.query(TweetMentions.user_id).filter(TweetMentions.tweet_id.in_(tweet_list))
    return q.count()


date = '2016-12-29'
# get_tweets_in_period('$AAPL', date, -1)
tweets_count_list = get_tweets_in_period('$AAPL', date, 0)
# get_tweets_in_period('$AAPL', date, 1)
print(tweets_count_list)
print(len(tweets_count_list))
print(get_users_count(tweets_count_list))
print(get_retweet_count(tweets_count_list))
print(get_hashtag_count(tweets_count_list))
print(get_replys_count(tweets_count_list))
print(get_mentions_count(tweets_count_list))
