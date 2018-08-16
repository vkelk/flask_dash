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
logger = logging.getLogger(__name__)


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

