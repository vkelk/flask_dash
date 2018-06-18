from datetime import datetime, timedelta
from dateutil import tz
import logging

from sqlalchemy import Table, create_engine, MetaData, func, Column, BigInteger, String, DateTime, distinct
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, scoped_session
from sqlalchemy.sql.expression import true

import settings

Base = declarative_base()
pg_config = {
    'username': settings.PG_USER,
    'password': settings.PG_PASSWORD,
    'database': settings.PG_DBNAME,
    'host': settings.DB_HOST}
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
project_meta = MetaData(bind=db_engine, schema="dashboard")
Session = sessionmaker(bind=db_engine, autoflush=False)
ScopedSessionAuto = scoped_session(sessionmaker(bind=db_engine, autocommit=True, autoflush=False))
ZONE_NY = tz.gettz('America/New_York')
ZONE_UTC = tz.gettz('UTC')
logger = logging.getLogger(__name__)


def convert_date(input_dt, zone_from=ZONE_NY, zone_to=ZONE_UTC):
    if isinstance(input_dt, str):
        utc_datetime = datetime.strptime(input_dt, '%Y-%m-%d %H:%M:%S')
    elif isinstance(input_dt, datetime):
        utc_datetime = input_dt
    else:
        logger.error('Incorect date format')
        return None
    # utc_datetime = datetime.strptime(input_dt, '%Y-%m-%d %H:%M:%S')
    # Tell the datetime object that it's in NY time zone since datetime objects are 'naive' by default
    utc_datetime = utc_datetime.replace(tzinfo=zone_from)
    # utc_datetime = input_dt.replace(tzinfo=zone_from)
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


class TradingDays(Base):
    __table__ = Table('trading_days', project_meta, autoload=True)


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
        logger.debug('Processed %s', t)
    except Exception:
        logger.exception('message')
    finally:
        ScopedSession.remove()
    return t


def get_tweet_ids(c):
    ScopedSession = scoped_session(sessionmaker(bind=db_engine))
    session = ScopedSession()
    date_input = c['date']
    if c['date_from'].date() == c['date_to'].date():
        datetime_start = convert_date(c['date_from'])
        datetime_end = convert_date(c['date_to'])
    elif c['date_from'].date() == date_input.date():
        datetime_start = convert_date(c['date_from'])
        datetime_end = convert_date(c['date_from'].strftime("%Y-%m-%d") + ' ' + '23:59:59')
    elif c['date_to'].date() == date_input.date():
        datetime_start = convert_date(c['date_to'].strftime("%Y-%m-%d") + ' ' + '00:00:00')
        datetime_end = convert_date(c['date_to'])
    else:
        datetime_start = convert_date(date_input.strftime("%Y-%m-%d") + ' ' + '00:00:00')
        datetime_end = convert_date(date_input.strftime("%Y-%m-%d") + ' ' + '23:59:59')
    logger.debug('Start date: %s, end date: %s', datetime_start, datetime_end)
    # print(datetime_start, datetime_end)
    tweets = session.query(mvCashtags.tweet_id) \
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
    except Exception:
        logger.exception('message')
    finally:
        session.close()
        ScopedSession.remove()
    return data


def validate_frequency(freq):
    if 24 % freq == 0:
        return True
    logger.warning('Frequency setting is not valid. Chose another value.')
    return False


def get_trading_periods(c):
    logger.debug('Getting periods')
    ScopedSession = scoped_session(sessionmaker(bind=db_engine))
    session = ScopedSession()
    query_trading_days = session.query(TradingDays.date) \
        .filter(TradingDays.is_trading == true()) \
        .filter(TradingDays.date.between(c['date_from'].date(), c['date_to'].date()))
    trading_days = [d[0] for d in query_trading_days.all()]
    if settings.frequency and validate_frequency(settings.frequency):
        logger.debug('Found setting for frequecy: %s', settings.frequency)
        days = []
        last_date_time = c['date_from']
        while last_date_time <= c['date_to']:
            if settings.days == 'all':
                days.append(last_date_time)
            elif settings.days == 'trading' and last_date_time.date() in trading_days:
                days.append(last_date_time)
            elif settings.days == 'non-trading' and last_date_time.date() not in trading_days:
                days.append(last_date_time)
            else:
                logger.warning('Datetime %s does not met any condition', last_date_time)
            last_date_time = last_date_time + timedelta(hours=settings.frequency)
            # print(last_date_time)
        return days
    logger.debug('Frequency setting is empty or not valid')
    if settings.days == 'trading':
        return [datetime.combine(d, datetime.min.time()) for d in trading_days]
    else:
        days = []
        date_delta = c['date_to'] - c['date_from']
        for i in range(date_delta.days + 1):
            date_input = (c['date_from'] + timedelta(days=i))
            if settings.days == 'all':
                days.append(date_input)
            elif settings.days == 'non-trading' and date_input not in trading_days:
                days.append(date_input)
            else:
                logger.error('Incorrect day status defined in settings')
                raise
        return [datetime.combine(d, datetime.min.time()) for d in days]
    logger.error('Empty result')
    raise
