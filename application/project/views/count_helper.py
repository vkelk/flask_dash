from datetime import datetime
from dateutil import tz
from pprint import pprint
import sys
from sqlalchemy import Table, create_engine, MetaData, func, Column, BigInteger, String, DateTime, distinct
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, scoped_session
from sqlalchemy.sql.expression import literal_column

from application.config import base_config

Base = declarative_base()
pg_config = {
    'username': base_config.POSTGRES_USER,
    'password': base_config.POSTGRES_PASS,
    'database': base_config.POSTGRES_DB,
    'host': base_config.POSTGRES_HOST}
pg_dsn = "postgresql+psycopg2://{username}:{password}@{host}:5432/{database}".format(**pg_config)
db_engine = create_engine(
    pg_dsn,
    connect_args={"application_name": 'counts:' + str(__name__)},
    pool_size=100,
    pool_recycle=600,
    max_overflow=0,
    encoding='utf-8'
    )
fintweet_meta = MetaData(bind=db_engine, schema="fintweet")
Session = sessionmaker(bind=db_engine, autoflush=False)
# ScopedSession = scoped_session(sessionmaker(bind=db_engine))
# ScopedSession = scoped_session(sessionmaker(bind=db_engine, autoflush=False))
ScopedSessionAuto = scoped_session(sessionmaker(bind=db_engine, autocommit=True, autoflush=False))
ZONE_NY = tz.gettz('America/New_York')
ZONE_UTC = tz.gettz('UTC')


def convert_date(input_dt, zone_from=ZONE_NY, zone_to=ZONE_UTC):
    utc_datetime = datetime.strptime(input_dt, '%Y-%m-%d %H:%M:%S')
    # Tell the datetime object that it's in NY time zone since datetime objects are 'naive' by default
    utc_datetime = utc_datetime.replace(tzinfo=zone_from)
    return utc_datetime.astimezone(zone_to)


class Tweet(Base):
    __table__ = Table('tweet', fintweet_meta, autoload=True)


class User(Base):
    __table__ = Table('user', fintweet_meta, autoload=True)


class UserCount(Base):
    __table__ = Table('user_count', fintweet_meta, autoload=True)


class TweetHashtag(Base):
    __table__ = Table('tweet_hashtags', fintweet_meta, autoload=True)


class TweetMention(Base):
    __table__ = Table('tweet_mentions', fintweet_meta, autoload=True)


class mvCashtags(Base):
    # __table__ = Table('mv_cashtags', fintweet_meta, autoload=True)
    __tablename__ = 'mv_cashtags'
    __table_args__ = {"schema": "fintweet"}

    id = Column(BigInteger, primary_key=True)
    tweet_id = Column(BigInteger)
    user_id = Column(BigInteger)
    cashtags = Column(String(120))
    datetime = Column(DateTime)


def get_users_count(tweet_list, sess):
    session = sess()
    q = session.query(
        mvCashtags.user_id,
        User.twitter_handle,
        User.date_joined,
        User.location,
        func.count(distinct(mvCashtags.tweet_id))
        ) \
        .join(User, User.user_id == mvCashtags.user_id) \
        .filter(mvCashtags.tweet_id.in_(tweet_list)) \
        .group_by(mvCashtags.user_id, User.twitter_handle, User.date_joined, User.location)
    fields = ['user_id', 'twiiter_handle', 'date_joined', 'location', 'counts']
    return [dict(zip(fields, d)) for d in q.all()]


def get_retweet_count(tweet_list, sess):
    true_list = ['1', 'True', 'true']
    session = sess()
    q = session.query(func.count(mvCashtags.tweet_id)) \
        .join(Tweet, mvCashtags.tweet_id == Tweet.tweet_id) \
        .filter(mvCashtags.tweet_id.in_(tweet_list)) \
        .filter(Tweet.retweet_status.in_(true_list))
    return q.scalar()


def get_hashtag_count(tweet_list, sess):
    session = sess()
    q = session.query(TweetHashtag.hashtags, func.count(distinct(TweetHashtag.tweet_id))) \
        .filter(TweetHashtag.tweet_id.in_(tweet_list)) \
        .group_by(TweetHashtag.hashtags)
    fields = ['hashtag', 'counts']
    # print(q.all())
    return [dict(zip(fields, d)) for d in q.all()]


def get_replys_count(tweet_list, sess):
    session = sess()
    q = session.query(func.count(Tweet.tweet_id)) \
        .filter(Tweet.tweet_id.in_(tweet_list)) \
        .filter(Tweet.reply_to > 0) \
        .filter(Tweet.reply_to != Tweet.tweet_id)
    return q.scalar()


def get_mentions_count(tweet_list, sess):
    session = sess()
    q = session.query(TweetMention.mentions, func.count(distinct(TweetMention.tweet_id))) \
        .filter(TweetMention.tweet_id.in_(tweet_list)) \
        .group_by(TweetMention.mentions)
    fields = ['mention', 'counts']
    return [dict(zip(fields, d)) for d in q.all()]


def load_counts(t):
    try:
        ScopedSession = scoped_session(sessionmaker(bind=db_engine))
        t['retweets'] = get_retweet_count(t['tweet_ids'], ScopedSession)
        t['replies'] = get_replys_count(t['tweet_ids'], ScopedSession)
        t['users_list'] = get_users_count(t['tweet_ids'], ScopedSession)
        t['users'] = len(t['users_list'])
        t['mentions_list'] = get_mentions_count(t['tweet_ids'], ScopedSession)
        t['mentions'] = len(t['mentions_list'])
        t['hashtags_list'] = get_hashtag_count(t['tweet_ids'], ScopedSession)
        t['hashtags'] = len(t['hashtags_list'])
    except Exception as e:
        fname = sys._getframe().f_code.co_name
        print(fname, type(e), str(e))
    finally:
        ScopedSession.remove()
    return t


def get_tweet_ids(c):
    ScopedSession = scoped_session(sessionmaker(bind=db_engine))
    date_input = c['date_input'].strftime("%Y-%m-%d")
    if c['date_from'] == c['date_to']:
        datetime_start = convert_date(c['date_from'].strftime("%Y-%m-%d") + ' ' + c['time_from'].strftime("%H:%M:%S"))
        datetime_end = convert_date(c['date_to'].strftime("%Y-%m-%d") + ' ' + c['time_to'].strftime("%H:%M:%S"))
    elif c['date_from'].strftime("%Y-%m-%d") == date_input:
        datetime_start = convert_date(c['date_from'].strftime("%Y-%m-%d") + ' ' + c['time_from'].strftime("%H:%M:%S"))
        datetime_end = convert_date(c['date_from'].strftime("%Y-%m-%d") + ' ' + '23:59:59')
    elif c['date_to'].strftime("%Y-%m-%d") == date_input:
        datetime_start = convert_date(c['date_to'].strftime("%Y-%m-%d") + ' ' + '00:00:00')
        datetime_end = convert_date(c['date_to'].strftime("%Y-%m-%d") + ' ' + c['time_to'].strftime("%H:%M:%S"))
    else:
        datetime_start = convert_date(date_input + ' ' + '00:00:00')
        datetime_end = convert_date(date_input + ' ' + '23:59:59')
    tweets = ScopedSession.query(mvCashtags.tweet_id) \
        .filter(mvCashtags.cashtags == c['cashtag']) \
        .filter(mvCashtags.datetime.between(datetime_start, datetime_end))
    if 'date_joined' in c and c['date_joined']:
        tweets = tweets.join(User, mvCashtags.user_id == User.user_id).filter(User.date_joined >= c['date_joined'])
    if 'following' in c and c['following']:
        tweets = tweets.join(UserCount, mvCashtags.user_id == UserCount.user_id) \
            .filter(UserCount.following >= c['following'])
    if 'followers' in c and c['followers']:
        tweets = tweets.join(UserCount, mvCashtags.user_id == UserCount.user_id) \
            .filter(UserCount.follower >= c['followers'])
    try:
        data = {
            'tweet_ids': [t[0] for t in tweets.all()],
            'date': date_input,
            'day_status': c['day_status'],
            'cashtag': c['cashtag']
            }
    except Exception as e:
        fname = sys._getframe().f_code.co_name
        print(fname, type(e), str(e))
    finally:
        ScopedSession.remove()
    return data

