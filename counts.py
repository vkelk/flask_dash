from datetime import datetime, timedelta
from dateutil import tz
from pprint import pprint

from sqlalchemy import or_, and_, text

from fintweet.models import Session, Tweet, TweetCashtags, TweetHashtags, TweetMentions, User, UserCount
from application.project.models import TradingDays

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
    pprint(period)
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
    true_list = ['1', 'True', 'true']
    q = session.query(Tweet.tweet_id) \
        .filter(Tweet.tweet_id.in_(tweet_list)) \
        .filter(Tweet.retweet_status.in_(true_list))
    # q = session.query(Tweet.tweet_id).filter(Tweet.tweet_id.in_(tweet_list)).filter(Tweet.retweet_status is True)
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


def convert_datetime(input_dt, zone_from=ZONE_NY, zone_to=ZONE_UTC):
    utc_datetime = datetime.strptime(input_dt, '%Y-%m-%d %H:%M:%S')
    # Tell the datetime object that it's in NY time zone since datetime objects are 'naive' by default
    utc_datetime = utc_datetime.replace(tzinfo=zone_from)
    return utc_datetime.astimezone(zone_to)


def get_tweet_list(c):
    date_delta = datetime.strptime(c['date_to'], '%Y-%m-%d') - datetime.strptime(c['date_from'], '%Y-%m-%d')
    trading_days = session.query(TradingDays.date) \
        .filter(TradingDays.is_trading == True) \
        .filter(TradingDays.date.between(c['date_from'], c['date_to']))
    days_list = [d[0] for d in trading_days.all()]
    result = []
    for i in range(date_delta.days + 1):
        date_input = (datetime.strptime(c['date_from'], '%Y-%m-%d') + timedelta(days=i)).date()
        c['date_input'] = date_input
        day = {}
        # if c['day_status'] in ['trading', 'non-trading']:
        if c['day_status'] in ['trading', 'all'] and date_input in days_list:
            day['date'] = date_input
            day['day_status'] = 'trading'
            day['tweet_ids'] = get_tweet_ids(c)
            result.append(day)
        elif c['day_status'] in ['non-trading', 'all'] and date_input not in days_list:
            day['date'] = date_input
            day['day_status'] = 'non-trading'
            day['tweet_ids'] = get_tweet_ids(c)
            result.append(day)
        else:
            print('Skipping date', date_input)
    return result


def get_tweet_ids(c):
    date_input = c['date_input'].strftime("%Y-%m-%d")
    if c['date_from'] == c['date_to']:
        print('same day')
        datetime_start = convert_date(c['date_from'] + ' ' + c['time_from'])
        datetime_end = convert_date(c['date_to'] + ' ' + c['time_to'])
    elif c['date_from'] == date_input:
        datetime_start = convert_date(c['date_from'] + ' ' + c['time_from'])
        datetime_end = convert_date(c['date_from'] + ' ' + '23:59:59')
    elif c['date_to'] == date_input:
        datetime_start = convert_date(c['date_to'] + ' ' + '00:00:00')
        datetime_end = convert_date(c['date_to'] + ' ' + c['time_to'])
    else:
        datetime_start = convert_date(date_input + ' ' + '00:00:00')
        datetime_end = convert_date(date_input + ' ' + '23:59:59')
    print(datetime_start, datetime_end)
    filter_period = text(
        "fintweet.tweet.date + fintweet.tweet.time between timestamp '"
        + str(datetime_start)
        + "' and timestamp '"
        + str(datetime_end)
        + "'")
    tweets = session.query(TweetCashtags.tweet_id).join(Tweet) \
        .filter(TweetCashtags.cashtags == c['cashtag']) \
        .filter(filter_period)
    if 'date_joined' in c:
        tweets = tweets.join(User).filter(User.date_joined >= c['date_joined'])
    if 'following' in c:
        tweets = tweets.join(UserCount, Tweet.user_id == UserCount.user_id).filter(UserCount.following >= c['following'])
    if 'followers' in c:
        tweets = tweets.join(UserCount, Tweet.user_id == UserCount.user_id).filter(UserCount.follower >= c['followers'])
    # print(tweets)
    # print([[t[0] for t in tweets.all()]])
    return [t[0] for t in tweets.all()]


conditions = {
    'cashtag': '$AAPL',
    'date_from': '2016-12-09',
    'date_to': '2016-12-12',
    'time_from': '09:30:00',
    'time_to': '16:00:00',
    'day_status': 'all',
    'date_joined': '2015-01-01',
    'followers': 100,
    'following': 1000
}

date1 = '2012-01-01 00:00:00'
print(convert_date(date1))
# get_tweets_in_period('$AAPL', date, -1)
# tweets_count_list = get_tweets_in_period('$AAPL', date, 0)
# tweets_count_list = get_tweet_list(conditions)
# get_tweets_in_period('$AAPL', date, 1)
# pprint(tweets_count_list)
# for d in tweets_count_list:
    # print(str(d['date']), len(d['tweet_ids']))
# print('tweets', len(tweets_count_list))
# print('users', get_users_count(tweets_count_list))
# print('retweet', get_retweet_count(tweets_count_list))
# print('hashtags', get_hashtag_count(tweets_count_list))
# print('replys', get_replys_count(tweets_count_list))
# print('mentions', get_mentions_count(tweets_count_list))
