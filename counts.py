from datetime import datetime, timedelta
from dateutil import tz

from sqlalchemy import func, or_

from fintweet.models import Session, Tweet, TweetCashtags

session = Session()

from_zone = tz.gettz('America/New_York')
to_zone = tz.gettz('UTC')

nyse_open = datetime.strptime('2017-02-01 09:30:00', '%Y-%m-%d %H:%M:%S')
nyse_close = datetime.strptime('2017-02-01 16:00:00', '%Y-%m-%d %H:%M:%S')
nyse_pre_open = nyse_open - timedelta(hours=9.5)
nyse_post_close = nyse_close + timedelta(hours=8)

# Tell the datetime object that it's in NY time zone since datetime objects are 'naive' by default
nyse_open = nyse_open.replace(tzinfo=from_zone)
nyse_close = nyse_close.replace(tzinfo=from_zone)
nyse_pre_open = nyse_pre_open.replace(tzinfo=from_zone)
nyse_post_close = nyse_post_close.replace(tzinfo=from_zone)

# Convert time zone to UTC
utc_open = nyse_open.astimezone(to_zone)
utc_close = nyse_close.astimezone(to_zone)
utc_pre_open = nyse_pre_open.astimezone(to_zone)
utc_post_close = nyse_post_close.astimezone(to_zone)

date_open = utc_open.date()
time_open = utc_open.time()
date_close = utc_close.date()
time_close = utc_close.time()
date_pre_open = utc_pre_open.date()
time_pre_open = utc_pre_open.time()
date_post_close = utc_post_close.date()
time_post_close = utc_post_close.time()

# q = session.query(Tweet) \
#     .filter(Tweet.date == date_open) \
#     .filter(Tweet.time >= time_open) \
#     .filter(Tweet.time <= time_close)

c_tag = '$AAPL'

q1 = session \
            .query(Tweet.tweet_id) \
            .join(TweetCashtags) \
            .filter(TweetCashtags.cashtags == c_tag) \
            .filter(Tweet.date == date_open) \
            .filter(Tweet.time >= time_open) \
            .filter(Tweet.time <= time_close)

q2 = session \
            .query(Tweet.tweet_id) \
            .join(TweetCashtags) \
            .filter(TweetCashtags.cashtags == c_tag) \
            .filter(((Tweet.date == date_open) & (Tweet.time > time_close)) | ((Tweet.date == date_post_close) & (Tweet.time < time_post_close)))
print(q2)
