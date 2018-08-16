import concurrent.futures as cf
from datetime import datetime, date
from dateutil import tz
import logging
import sys

import psycopg2

import settings

pg_config = {
    'username': settings.PG_USER,
    'password': settings.PG_PASSWORD,
    'host': settings.DB_HOST,
    'dbname': settings.PG_DBNAME}

logger = logging.getLogger(__name__)

try:
    cnx = psycopg2.connect(**pg_config)
    cur = cnx.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute("SET SEARCH_PATH = %s" % 'fintweet')
    cnx.commit()
    logger.info('Connected to database')
except psycopg2.Error as err:
    logger.error(err)


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


def ft_tweets_trade(t, sess):
    logger.info('Getting tweets_trade for %s', t['cashtag'])
    
    try:
        session = sess()
        trading_days = session.query(TradingDays.date).subquery()
        q = session.query(mvCashtags) \
            .filter(mvCashtags.cashtags == t['cashtag']) \
            .filter(cast(mvCashtags.datetime, Date).in_(trading_days))
        print(q.count())
    except Exception:
        logging.exception('message')
    finally:
        session.close()


def get_fintweet_data(t):
    print(t)
    try:
        logger.debug('Getting counts for %s', t['cashtag'])
        ScopedSession = scoped_session(sessionmaker(bind=db_engine))
        t['tweets_trade'] = ft_tweets_trade(t, ScopedSession)
    except Exception:
        logger.exception('message')
    finally:
        ScopedSession.remove()
    return t


def get_ft_periods(c):
    logger.info('Getting fintweet periods for %s', c['cashtag'])
    date_start = c['date_from'].date()
    date_end = c['date_to'].date()
    ScopedSession = scoped_session(sessionmaker(bind=db_engine))
    session = ScopedSession()
    try:
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
    except Exception:
        pass
    finally:
        pass
