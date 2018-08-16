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
stocktwits_meta = MetaData(bind=db_engine, schema="stocktwits")
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


class FtUser(Base):
    __table__ = Table('user', fintweet_meta, autoload=True)


class FtUserCount(Base):
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


class StUser(Base):
    __table__ = Table('user', stocktwits_meta, autoload=True)


class StUserCount(Base):
    __table__ = Table('user_count', stocktwits_meta, autoload=True)


class Idea(Base):
    __table__ = Table('ideas', stocktwits_meta, autoload=True)


class IdeasCount(Base):
    __table__ = Table('ideas_count', stocktwits_meta, autoload=True)


class IdeaHashtag(Base):
    __table__ = Table('idea_hashtags', stocktwits_meta, autoload=True)


class StmvCashtags(Base):
    __tablename__ = 'mv_cashtags'
    __table_args__ = {"schema": "stocktwits"}

    id = Column(BigInteger, primary_key=True)
    ideas_id = Column(BigInteger)
    user_id = Column(BigInteger)
    cashtag = Column(String(120))
    datetime = Column(DateTime)


class TradingDays(Base):
    __table__ = Table('trading_days', project_meta, autoload=True)


class FintweetCounts(Base):
    __table__ = Table('fintweet_counts', project_meta, autoload=True)


class FintweetCountTweets(Base):
    __table__ = Table('fintweet_count_tweets', project_meta, autoload=True)


def get_users_count(tweet_list, sess):
    try:
        session = sess()
        q = session.query(
            Tweet.user_id,
            FtUser.twitter_handle,
            FtUser.date_joined,
            FtUser.location,
            func.count(distinct(Tweet.tweet_id))
            ) \
            .join(FtUser, FtUser.user_id == Tweet.user_id) \
            .filter(Tweet.tweet_id.in_(tweet_list)) \
            .group_by(Tweet.user_id, FtUser.twitter_handle, FtUser.date_joined, FtUser.location)
        fields = ['user_id', 'twiiter_handle', 'date_joined', 'location', 'counts']
        result = [dict(zip(fields, d)) for d in q.all()]
    except Exception:
        logger.error('Cannot run get_hashtag_count query')
        logger.exception('message')
    finally:
        session.close()
    # print(q)
    # sys.exit()
    return result


def get_st_users_count(tweet_list, sess):
    try:
        session = sess()
        q = session.query(
            Idea.user_id,
            StUser.user_handle,
            StUser.date_joined,
            StUser.location,
            func.count(distinct(Idea.ideas_id))
            ) \
            .join(StUser, StUser.user_id == Idea.user_id) \
            .filter(Idea.ideas_id.in_(tweet_list)) \
            .group_by(Idea.user_id, StUser.user_handle, StUser.date_joined, StUser.location)
        fields = ['user_id', 'user_handle', 'date_joined', 'location', 'counts']
        result = [dict(zip(fields, d)) for d in q.all()]
    except Exception:
        logger.error('Cannot run get_st_users_count query')
        logger.exception('message')
    finally:
        session.close()
    # print(q)
    # sys.exit()
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


def get_st_hashtag_count(tweet_list, sess):
    try:
        session = sess()
        q = session.query(IdeaHashtag.hashtag, func.count(distinct(IdeaHashtag.ideas_id))) \
            .filter(IdeaHashtag.ideas_id.in_(tweet_list)) \
            .group_by(IdeaHashtag.hashtag)
        fields = ['hashtag', 'counts']
        # print(q.all())
        result = [dict(zip(fields, d)) for d in q.all()]
    except Exception:
        logger.error('Cannot run get_st_hashtag_count query')
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
        q = session.query(distinct(FtUserCount.user_id), FtUserCount.follower) \
            .join(Tweet, FtUserCount.user_id == Tweet.user_id) \
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


def get_st_user_counts(tweet_list, sess):
    try:
        session = sess()
        q = session.query(distinct(StUserCount.user_id), StUserCount.followers) \
            .join(Idea, StUserCount.user_id == Idea.user_id) \
            .filter(Idea.ideas_id.in_(tweet_list))
        fields = ['user_id', 'followers']
        result = [dict(zip(fields, d)) for d in q.all()]
        followers = 0
        for r in result:
            if isinstance(r['followers'], int):
                followers += r['followers']
    except Exception:
        logger.error('Cannot run get_st_user_counts query')
        logger.exception('message')
    finally:
        session.close()
    return followers


def page_query(q):
    offset = 0
    while True:
        r = False
        for elem in q.limit(5).offset(offset):
            r = True
            yield elem
        offset += 5
        print(offset)
        if not r:
            break


def load_counts_v2(t):
    # print(t)
    try:
        logger.debug('Inspecting %s tweets for %s', t['tweets_count'], t['date'])
        ScopedSession = scoped_session(sessionmaker(bind=db_engine))
        t['users_list'] = get_users_count(t['tweet_ids'], ScopedSession)
        t['mentions_list'] = get_mentions_count(t['tweet_ids'], ScopedSession)
        t['hashtags_list'] = get_hashtag_count(t['tweet_ids'], ScopedSession)
        t['user_followers'] = get_user_counts(t['tweet_ids'], ScopedSession)
        t['users'] = len(t['users_list'])
        t['mentions'] = len(t['mentions_list'])
        t['hashtags'] = len(t['hashtags_list'])
    except Exception:
        logger.exception('message')
    finally:
        ScopedSession.remove()
    return t


def load_stocktwits_counts(t):
    # print(t)
    try:
        logger.debug('Inspecting %s ideas for %s', t['tweets_count'], t['date'])
        ScopedSession = scoped_session(sessionmaker(bind=db_engine))
        t['users_list'] = get_st_users_count(t['tweet_ids'], ScopedSession)
        t['hashtags_list'] = get_st_hashtag_count(t['tweet_ids'], ScopedSession)
        t['user_followers'] = get_st_user_counts(t['tweet_ids'], ScopedSession)
        t['users'] = len(t['users_list'])
        t['hashtags'] = len(t['hashtags_list'])
        t['mentions_list'] = []
        t['mentions'] = len(t['mentions_list'])
    except Exception:
        logger.exception('message')
    finally:
        ScopedSession.remove()
    return t


def load_counts(t):
    # print(t)
    try:
        logger.debug('Inspecting %s tweets for %s', t['tweets_count'], t['date'])
        ScopedSession = scoped_session(sessionmaker(bind=db_engine))
        with cf.ThreadPoolExecutor(max_workers=4) as executor:
            try:
                users_future = executor.submit(get_users_count, t['tweet_ids'], ScopedSession)
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
        t['user_followers'] = user_counts_future.result()
        t['users'] = len(t['users_list'])
        t['mentions'] = len(t['mentions_list'])
        t['hashtags'] = len(t['hashtags_list'])
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
    if c['day_status'] in ['non_trading', 'post_market', 'all']:
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
            .filter(cast(mvCashtags.datetime, Date).between(date_start, date_end)) \
            .filter(cast(mvCashtags.datetime, Time).between(time_start, time_end))
        if c['day_status'] in ['trading', 'pre_market', 'post_market']:
            tweets = tweets.filter(cast(mvCashtags.datetime, Date).in_(tdays_list))
        elif c['day_status'] == 'non_trading':
            tweets = tweets.filter(cast(mvCashtags.datetime, Date).notin_(tdays_list))
        if 'date_joined' in c and c['date_joined']:
            tweets = tweets.join(FtUser, mvCashtags.user_id == FtUser.user_id).filter(FtUser.date_joined >= c['date_joined'])
        if 'following' in c and c['following']:
            tweets = tweets.join(FtUserCount, mvCashtags.user_id == FtUserCount.user_id) \
                .filter(FtUserCount.following >= c['following'])
        if 'followers' in c and c['followers']:
            tweets = tweets.join(FtUserCount, mvCashtags.user_id == FtUserCount.user_id) \
                .filter(FtUserCount.follower >= c['followers'])
        tweets = tweets.group_by('date')
    except Exception:
        logger.error('Could not execute get_cashtag_periods')
        logger.exception('message')
    finally:
        session.close()
        ScopedSession.remove()
    for t in tweets.execution_options(stream_results=True):
        period = {
            'date': t[0],
            'tweets_count': len(t[1]),
            'tweet_ids': t[1],
            'replies': t[2],
            'retweets': t[3],
            'favorites': t[4],
            'hours': duration / 3600
        }
        if c['day_status'] in ['trading', 'pre_market', 'post_market', 'all'] and period['date'] in tdays_list:
            period['day_status'] = c['day_status']
        elif c['day_status'] in ['non_trading', 'all'] and period['date'] not in tdays_list:
            period['day_status'] = c['day_status']
        else:
            logger.debug('Skipping date %s. Do not apply the "%s" contition', period['date'], c['day_status'])
            continue
        yield period
    logger.info('Completed tweet list for %s', c['cashtag'])


def get_st_cashtag_periods(c):
    logger.info('Getting idea list for %s', c['cashtag'])
    date_start = c['date_from'].date()
    date_end = c['date_to'].date()
    time_start = c['date_from'].time()
    time_end = c['date_to'].time()
    duration = (datetime.combine(date.min, time_end) - datetime.combine(date.min, time_start)).total_seconds()
    if c['day_status'] in ['non_trading', 'post_market', 'all']:
        duration += 1
    ScopedSession = scoped_session(sessionmaker(bind=db_engine))
    session = ScopedSession()
    try:
        trading_days = session.query(TradingDays.date) \
            .filter(TradingDays.is_trading == true()) \
            .filter(TradingDays.date.between(date_start, date_end))
        tdays_list = [d[0] for d in trading_days.all()]
        ideas = session.query(
                cast(StmvCashtags.datetime, Date).label('date'),
                func.array_agg(StmvCashtags.ideas_id).label('ideas_id'),
                func.sum(func.coalesce(IdeasCount.replies, 0)).label('replies'),
                func.sum(func.coalesce(IdeasCount.reshares, 0)).label('retweets'),
                func.sum(func.coalesce(IdeasCount.likes, 0)).label('favorites'),
            ) \
            .join(IdeasCount, IdeasCount.ideas_id == StmvCashtags.ideas_id) \
            .filter(StmvCashtags.cashtag == c['cashtag']) \
            .filter(cast(StmvCashtags.datetime, Date).between(date_start, date_end)) \
            .filter(cast(StmvCashtags.datetime, Time).between(time_start, time_end))
        if c['day_status'] in ['trading', 'pre_market', 'post_market']:
            ideas = ideas.filter(cast(StmvCashtags.datetime, Date).in_(tdays_list))
        elif c['day_status'] == 'non_trading':
            ideas = ideas.filter(cast(StmvCashtags.datetime, Date).notin_(tdays_list))
        if 'date_joined' in c and c['date_joined']:
            ideas = ideas.join(FtUser, StmvCashtags.user_id == StUser.user_id).filter(StUser.date_joined >= c['date_joined'])
        if 'following' in c and c['following']:
            ideas = ideas.join(StUserCount, StmvCashtags.user_id == StUserCount.user_id) \
                .filter(StUserCount.following >= c['following'])
        if 'followers' in c and c['followers']:
            ideas = ideas.join(StUserCount, StmvCashtags.user_id == StUserCount.user_id) \
                .filter(StUserCount.follower >= c['followers'])
        ideas = ideas.group_by(cast(StmvCashtags.datetime, Date))
        # print(tweets.statement.compile(dialect=postgresql.dialect()))
        # sys.exit()
    except Exception:
        logger.error('Could not execute get_st_cashtag_periods')
        logger.exception('message')
    finally:
        session.close()
        ScopedSession.remove()
    for t in ideas.execution_options(stream_results=True):
        period = {
            'date': t[0],
            'tweets_count': len(t[1]),
            'tweet_ids': t[1],
            'replies': t[2],
            'retweets': t[3],
            'favorites': t[4],
            'hours': duration / 3600
        }
        if c['day_status'] in ['trading', 'pre_market', 'post_market', 'all'] and period['date'] in tdays_list:
            period['day_status'] = c['day_status']
        elif c['day_status'] in ['non_trading', 'all'] and period['date'] not in tdays_list:
            period['day_status'] = c['day_status']
        else:
            logger.debug('Skipping date %s. Do not apply the "%s" contition', period['date'], c['day_status'])
            continue
        yield period
    logger.info('Completed ideas list for %s', c['cashtag'])
