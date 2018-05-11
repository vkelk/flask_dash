from datetime import datetime, timedelta
from dateutil import tz

from sqlalchemy import func, or_, and_

from fintweet.models import Session, Tweet, TweetCashtags

session = Session()

ZONE_NY = tz.gettz('America/New_York')
ZONE_UTC = tz.gettz('UTC')


def convert_date(input_dt, zone_from=ZONE_NY, zone_to=ZONE_UTC):
    utc_datetime = datetime.strptime(input_dt, '%Y-%m-%d %H:%M:%S')
    # Tell the datetime object that it's in NY time zone since datetime objects are 'naive' by default
    utc_datetime = utc_datetime.replace(tzinfo=zone_from)
    return utc_datetime.astimezone(zone_to)


date_open = convert_date('2017-02-02 09:30:00').date()
time_open = convert_date('2017-02-02 09:30:00').time()
time_close = convert_date('2017-02-02 16:00:00').time()
date_pre_open = convert_date('2017-02-02 00:00:00').date()
time_pre_open = convert_date('2017-02-02 00:00:00').time()
date_post_close = convert_date('2017-02-03 00:00:00').date()
time_post_close = convert_date('2017-02-03 00:00:00').time()


c_tag = '$AAPL'


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


def count_period(c_tag, date, period_type=0):
    period = get_period(date, period_type)
    if period_type in (0, -1):
        filter_period = and_(Tweet.date == period['start'].date(), Tweet.time >= period['start'].time(), Tweet.time < period['end'].time())
    elif period_type == 1:
        start = and_(Tweet.date == period['start'].date(), Tweet.time >= period['start'].time()).self_group()
        end = and_(Tweet.date == period['end'].date(), Tweet.time < period['end'].time()).self_group()
        filter_period = or_(start, end)
    # start = and_(Tweet.date == period['start'].date(), Tweet.time >= period['start'].time()).self_group()
    # end = and_(Tweet.date == period['end'].date(), Tweet.time < period['end'].time()).self_group()
    q = session.query(TweetCashtags.tweet_id).join(Tweet) \
        .filter(TweetCashtags.cashtags == c_tag) \
        .filter(filter_period)
    print(q)
    print(period)
    print(q.count())


date = '2016-02-02'
count_period('$AAPL', date, -1)
count_period('$AAPL', date, 0)
count_period('$AAPL', date, 1)
exit()

q1 = session \
            .query(Tweet.tweet_id) \
            .join(TweetCashtags) \
            .filter(TweetCashtags.cashtags == c_tag) \
            .filter(Tweet.date == date_open) \
            .filter(Tweet.time >= time_open) \
            .filter(Tweet.time <= time_close)

q2 = session \
            .query(Tweet.tweet_id, Tweet.date, Tweet.time) \
            .join(TweetCashtags) \
            .filter(((Tweet.date == date_open) & (Tweet.time > time_close)) | ((Tweet.date == date_post_close) & (Tweet.time < time_post_close))) \
            .order_by(Tweet.tweet_id)
print(q2)
for t in q2.all():
    print(t.tweet_id, t.date, t.time)
