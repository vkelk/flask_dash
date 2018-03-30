from sqlalchemy import Table, create_engine, MetaData
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship, sessionmaker, scoped_session

from .settings import PG_USER, PG_PASSWORD, PG_DBNAME, DB_HOST

Base = declarative_base()
pg_config = {'username': PG_USER, 'password': PG_PASSWORD, 'database': PG_DBNAME, 'host': DB_HOST}
pg_dsn = "postgresql+psycopg2://{username}:{password}@{host}:5432/{database}".format(**pg_config)
db_engine = create_engine(pg_dsn)
db_meta = MetaData(bind=db_engine, schema="fintweet")
Session = sessionmaker(bind=db_engine, autoflush=False)
ScopedSession = scoped_session(sessionmaker(bind=db_engine, autoflush=False))
ScopedSessionAuto = scoped_session(sessionmaker(bind=db_engine, autocommit=True, autoflush=False))


class User(Base):
    __table__ = Table('user', db_meta, autoload=True)
    tweets = relationship('Tweet')
    counts = relationship('UserCount')

    def __str__(self):
        return self.user_id


class UserCount(Base):
    __table__ = Table('user_count', db_meta, autoload=True)

    def __str__(self):
        return self.user_id


class TweetCount(Base):
    __table__ = Table('tweet_count', db_meta, autoload=True)

    def __str__(self):
        return self.tweet_id


class TweetMentions(Base):
    __table__ = Table('tweet_mentions', db_meta, autoload=True)


class TweetCashtags(Base):
    __table__ = Table('tweet_cashtags', db_meta, autoload=True)

    def __str__(self):
        return self.cashtags


class TweetHashtags(Base):
    __table__ = Table('tweet_hashtags', db_meta, autoload=True)


class TweetUrl(Base):
    __table__ = Table('tweet_url', db_meta, autoload=True)


class Tweet(Base):
    __table__ = Table('tweet', db_meta, autoload=True)
    counts = relationship('TweetCount')
    ment_s = relationship('TweetMentions')
    cash_s = relationship('TweetCashtags')
    hash_s = relationship('TweetHashtags')
    url_s = relationship('TweetUrl')

    def __str__(self):
        return str(self.tweet_id)

    def __repr__(self):
        return "<Tweet(tweet_id='%s', user_id='%s')>" % (self.tweet_id, self.user_id)


class FileInfo(Base):
    __table__ = Table('fileinfo', db_meta, autoload=True)
