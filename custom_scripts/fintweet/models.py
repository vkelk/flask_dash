from sqlalchemy import Table, create_engine, MetaData
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship, sessionmaker, scoped_session

from .settings import PG_USER, PG_PASSWORD, PG_DBNAME, DB_HOST

Base = declarative_base()
pg_config = {'username': PG_USER, 'password': PG_PASSWORD, 'database': PG_DBNAME, 'host': DB_HOST}
pg_dsn = "postgresql://{username}:{password}@{host}:5432/{database}".format(**pg_config)
# db_engine = create_engine(pg_dsn)
db_engine = create_engine(
    pg_dsn,
    connect_args={"application_name": 'fintweet:' + str(__name__)},
    pool_size=100,
    pool_recycle=600,
    max_overflow=0,
    encoding='utf-8'
    )
fintweet_meta = MetaData(bind=db_engine, schema="fintweet")
dashboard_meta = MetaData(bind=db_engine, schema="dashboard")
Session = sessionmaker(bind=db_engine, autoflush=False)
ScopedSession = scoped_session(sessionmaker(bind=db_engine, autoflush=False))
ScopedSessionAuto = scoped_session(sessionmaker(bind=db_engine, autocommit=True, autoflush=False))


class User(Base):
    __table__ = Table('user', fintweet_meta, autoload=True)
    tweets = relationship('Tweet')
    counts = relationship('UserCount')

    def __str__(self):
        return self.user_id


class UserCount(Base):
    __table__ = Table('user_count', fintweet_meta, autoload=True)

    def __str__(self):
        return self.user_id


class TweetCount(Base):
    __table__ = Table('tweet_count', fintweet_meta, autoload=True)

    def __str__(self):
        return self.tweet_id


class TweetMentions(Base):
    __table__ = Table('tweet_mentions', fintweet_meta, autoload=True)


class TweetCashtags(Base):
    __table__ = Table('tweet_cashtags', fintweet_meta, autoload=True)

    def __str__(self):
        return self.cashtags


class TweetHashtags(Base):
    __table__ = Table('tweet_hashtags', fintweet_meta, autoload=True)


class TweetUrl(Base):
    __table__ = Table('tweet_url', fintweet_meta, autoload=True)


class Tweet(Base):
    __table__ = Table('tweet', fintweet_meta, autoload=True)
    counts = relationship('TweetCount', cascade="all,delete", backref="tweet")
    ment_s = relationship('TweetMentions', cascade="all,delete", backref="tweet")
    cash_s = relationship('TweetCashtags', cascade="all,delete", backref="tweet")
    hash_s = relationship('TweetHashtags', cascade="all,delete", backref="tweet")
    url_s = relationship('TweetUrl', cascade="all,delete", backref="tweet")

    def __str__(self):
        return str(self.tweet_id)

    def __repr__(self):
        return "<Tweet(tweet_id='%s', user_id='%s')>" % (self.tweet_id, self.user_id)


class FileInfo(Base):
    __table__ = Table('fileinfo', fintweet_meta, autoload=True)


class CompanySec(Base):
    __table__ = Table('company_sec', dashboard_meta, autoload=True)


class TweetDeleted(Base):
    __table__ = Table('tweet_deleted', fintweet_meta, autoload=True)
