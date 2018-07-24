import concurrent.futures as cf
from datetime import datetime, date
from dateutil import tz
import logging
import sys

from sqlalchemy import Table, create_engine, MetaData, func, Column, BigInteger, String, DateTime, distinct, Time, cast, Date
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, scoped_session
from sqlalchemy.sql.expression import true
from sqlalchemy.dialects import postgresql

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
    pool_size=300,
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


class TweetCount(Base):
    __table__ = Table('tweet_count', fintweet_meta, autoload=True)


class User(Base):
    __table__ = Table('user', fintweet_meta, autoload=True)


class UserCount(Base):
    __table__ = Table('user_count', fintweet_meta, autoload=True)


class TweetHashtag(Base):
    __table__ = Table('tweet_hashtags', fintweet_meta, autoload=True)


class TweetCashtag(Base):
    __table__ = Table('tweet_cashtags', fintweet_meta, autoload=True)


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


class FintweetCounts(Base):
    __table__ = Table('fintweet_counts', project_meta, autoload=True)


class FintweetCountTweets(Base):
    __table__ = Table('fintweet_count_tweets', project_meta, autoload=True)


def get_users_count(tweet_list, sess):
    true_list = ['1', 'True', 'true']
    session = sess()
    q = session.query(
        mvCashtags.user_id,
        User.twitter_handle,
        User.date_joined,
        User.location,
        func.count(distinct(mvCashtags.tweet_id))
        ) \
        .join(User, User.user_id == mvCashtags.user_id) \
        .join(Tweet, Tweet.user_id == mvCashtags.user_id) \
        .filter(mvCashtags.tweet_id.in_(tweet_list)) \
        .filter(Tweet.retweet_status.notin_(true_list)) \
        .group_by(mvCashtags.user_id, User.twitter_handle, User.date_joined, User.location)
    fields = ['user_id', 'twiiter_handle', 'date_joined', 'location', 'counts']
    result = [dict(zip(fields, d)) for d in q.all()]
    session.close()
    return result


def get_hashtag_count(tweet_list, sess):
    try:
        session = sess()
        q = session.query(TweetHashtag.hashtags, func.count(distinct(TweetHashtag.tweet_id))) \
            .filter(TweetHashtag.tweet_id.in_(tweet_list)) \
            .group_by(TweetHashtag.hashtags)
        fields = ['hashtag', 'counts']
        # print(q.all())
        result = [dict(zip(fields, d)) for d in q.all()]
    except Exception:
        logger.error('Cannot run get_hashtag_count query')
        logger.exception('message')
    finally:
        session.close()
    return result


def get_mentions_count(tweet_list, sess):
    try:
        session = sess()
        q = session.query(TweetMention.mentions, func.count(distinct(TweetMention.tweet_id))) \
            .filter(TweetMention.tweet_id.in_(tweet_list)) \
            .group_by(TweetMention.mentions)
        fields = ['mention', 'counts']
        result = [dict(zip(fields, d)) for d in q.all()]
    except Exception:
        logger.error('Cannot run get_mentions_count query')
        logger.exception('message')
    finally:
        session.close()
    return result


def get_tweet_counts(tweet_list, sess):
    try:
        session = sess()
        q = session.query(func.sum(TweetCount.reply).label('replies'),
                          func.sum(TweetCount.retweet).label('retweets'),
                          func.sum(TweetCount.favorite).label('favorites')) \
            .filter(TweetCount.tweet_id.in_(tweet_list))
        fields = ['replies', 'retweets', 'favorites']
        result = [dict(zip(fields, d)) for d in q.all()]
    except Exception:
        logger.error('Cannot run get_tweet_counts query')
        logger.exception('message')
    finally:
        session.close()
    return result


def get_user_counts(tweet_list, sess):
    try:
        session = sess()
        q = session.query(distinct(UserCount.user_id), UserCount.follower) \
            .join(Tweet, UserCount.user_id == Tweet.user_id) \
            .filter(Tweet.tweet_id.in_(tweet_list))
        fields = ['user_id', 'follower']
        result = [dict(zip(fields, d)) for d in q.all()]
        followers = 0
        for r in result:
            if isinstance(r['follower'], int):
                followers += r['follower']
    except Exception:
        logger.error('Cannot run get_user_counts query')
        logger.exception('message')
    finally:
        session.close()
    return followers


def load_counts(t):
    # print(t)
    try:
        logger.debug('Inspecting %s tweets for %s', t['tweets_count'], t['date'])
        ScopedSession = scoped_session(sessionmaker(bind=db_engine))
        with cf.ThreadPoolExecutor(max_workers=4) as executor:
            # retweet_future = executor.submit(get_retweet_count, t['tweet_ids'], ScopedSession)
            # replys_future = executor.submit(get_replys_count, t['tweet_ids'], ScopedSession)
            try:
                # tweet_count_future = executor.submit(get_tweet_counts, t['tweet_ids'], ScopedSession)
                users_future = executor.submit(get_users_count, t['tweet_ids'], ScopedSession)
                # users_retweet_future = executor.submit(get_users_retweet_count, t['tweet_ids'], ScopedSession)
                mentions_future = executor.submit(get_mentions_count, t['tweet_ids'], ScopedSession)
                hashtag_future = executor.submit(get_hashtag_count, t['tweet_ids'], ScopedSession)
                user_counts_future = executor.submit(get_user_counts, t['tweet_ids'], ScopedSession)
            except (KeyboardInterrupt, SystemExit):
                ScopedSession.remove()
                sys.exit()
        cf.wait([users_future, mentions_future, hashtag_future, user_counts_future])
        t['users_list'] = users_future.result()
        t['mentions_list'] = mentions_future.result()
        t['hashtags_list'] = hashtag_future.result()
        t['users'] = len(t['users_list'])
        t['mentions'] = len(t['mentions_list'])
        t['hashtags'] = len(t['hashtags_list'])
        t['user_followers'] = user_counts_future.result()
        # logger.debug('Processed %s', t)
    except Exception:
        logger.exception('message')
    finally:
        ScopedSession.remove()
    return t


def get_cashtag_periods(c):
    logger.info('Getting tweet list for %s', c['cashtag'])
    date_start = c['date_from'].date()
    date_end = c['date_to'].date()
    time_start = c['date_from'].time()
    time_end = c['date_to'].time()
    duration = (datetime.combine(date.min, time_end) - datetime.combine(date.min, time_start)).total_seconds()
    if c['day_status'] in ['non_trading', 'post_market']:
        duration += 1
    # print(time_start, time_end)
    ScopedSession = scoped_session(sessionmaker(bind=db_engine))
    session = ScopedSession()
    try:
        # interval = func.to_timestamp(func.floor((func.extract('epoch', Tweet.date + Tweet.time)/interval_time)) * interval_time) \
        #     .op('AT TIME ZONE')('EST').label('interval')
        trading_days = session.query(TradingDays.date) \
            .filter(TradingDays.is_trading == true()) \
            .filter(TradingDays.date.between(date_start, date_end))
        tdays_list = [d[0] for d in trading_days.all()]
        tweets = session.query(
                cast(mvCashtags.datetime, Date).label('date'),
                func.array_agg(mvCashtags.tweet_id).label('tweet_ids'),
                func.sum(TweetCount.reply).label('replies'),
                func.sum(TweetCount.retweet).label('retweets'),
                func.sum(TweetCount.favorite).label('favorites'),
            ) \
            .join(TweetCount, TweetCount.tweet_id == mvCashtags.tweet_id) \
            .filter(mvCashtags.cashtags == c['cashtag']) \
            .filter(mvCashtags.datetime.between(date_start, date_end)) \
            .filter(cast(mvCashtags.datetime, Time).between(time_start, time_end))
        if c['day_status'] in ['trading', 'pre_market', 'post_market']:
            tweets = tweets.filter(cast(mvCashtags.datetime, Date).in_(tdays_list))
        else:
            tweets = tweets.filter(cast(mvCashtags.datetime, Date).notin_(tdays_list))
        if 'date_joined' in c and c['date_joined']:
            tweets = tweets.join(User, mvCashtags.user_id == User.user_id).filter(User.date_joined >= c['date_joined'])
        if 'following' in c and c['following']:
            tweets = tweets.join(UserCount, mvCashtags.user_id == UserCount.user_id) \
                .filter(UserCount.following >= c['following'])
        if 'followers' in c and c['followers']:
            tweets = tweets.join(UserCount, mvCashtags.user_id == UserCount.user_id) \
                .filter(UserCount.follower >= c['followers'])
        tweets = tweets.group_by('date')
        # print(tweets.statement.compile(dialect=postgresql.dialect()))
        # sys.exit()
    except Exception:
        logger.error('Could not execute get_cashtag_periods')
        logger.exception('message')
    finally:
        session.close()
        ScopedSession.remove()
    period_list = []
    # print(tweets)
    # print(tweets.first())
    for t in tweets.execution_options(stream_results=True):
        # sys.exit()
        # print(t[1])
        period = {
            # 'cashtag': t[0],
            'date': t[0],
            'tweets_count': len(t[1]),
            'tweet_ids': t[1],
            'replies': t[2],
            'retweets': t[3],
            'favorites': t[4],
            'hours': duration / 3600
        }
        if c['day_status'] in ['trading', 'pre_market', 'post_market'] and period['date'] in tdays_list:
            period['day_status'] = c['day_status']
        elif c['day_status'] in ['non_trading'] and period['date'] not in tdays_list:
            period['day_status'] = c['day_status']
        else:
            logger.debug('Skipping date %s. Do not apply the "%s" contition', period['date'], c['day_status'])
            continue
        yield period
        # period_list.append(period)
    logger.info('Completed tweet list for %s', c['cashtag'])
    # return period_list
