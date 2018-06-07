from sqlalchemy import Table, create_engine, MetaData
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship, sessionmaker, scoped_session

from .settings import PG_USER, PG_PASSWORD, PG_DBNAME, DB_HOST

Base = declarative_base()
pg_config = {'username': PG_USER, 'password': PG_PASSWORD, 'database': PG_DBNAME, 'host': DB_HOST}
pg_dsn = "postgresql+psycopg2://{username}:{password}@{host}:5432/{database}".format(**pg_config)
db_engine = create_engine(pg_dsn)
stocktwits_meta = MetaData(bind=db_engine, schema="stocktwits")
dashboard_meta = MetaData(bind=db_engine, schema="dashboard")
Session = sessionmaker(bind=db_engine, autoflush=False)
ScopedSession = scoped_session(sessionmaker(bind=db_engine, autoflush=False))
ScopedSessionAuto = scoped_session(sessionmaker(bind=db_engine, autocommit=True, autoflush=False))


class Stocktwits(Base):
    __abstract__ = True
    __table_args__ = {"schema": "stocktwits"}


class User(Stocktwits):
    __table__ = Table('user', stocktwits_meta, autoload=True)
    __tablename__ = 'user'

    def __repr__(self):
        return self.user_handle


class UserCount(Stocktwits):
    __table__ = Table('user_count', stocktwits_meta, autoload=True)
    __tablename__ = 'user_count'

    def __repr__(self):
        return self.user_id


class UserStrategy(Stocktwits):
    __table__ = Table('user_strategy', stocktwits_meta, autoload=True)
    __tablename__ = 'user_strategy'

    def __repr__(self):
        return self.user_id


class Ideas(Stocktwits):
    __table__ = Table('ideas', stocktwits_meta, autoload=True)
    __tablename__ = 'ideas'

    def __repr__(self):
        return self.ideas_id


class IdeaCounts(Stocktwits):
    __table__ = Table('ideas_count', stocktwits_meta, autoload=True)
    __tablename__ = 'ideas_count'

    def __repr__(self):
        return self.ideas_id


class IdeaCashtags(Stocktwits):
    __table__ = Table('idea_cashtags', stocktwits_meta, autoload=True)
    __tablename__ = 'idea_cashtags'

    def __repr__(self):
        return self.cashtag


class IdeaHashtags(Stocktwits):
    __table__ = Table('idea_hashtags', stocktwits_meta, autoload=True)
    __tablename__ = 'idea_hashtags'

    def __repr__(self):
        return self.hashtag


class IdeaUrls(Stocktwits):
    __table__ = Table('ideas_url', stocktwits_meta, autoload=True)
    __tablename__ = 'ideas_url'

    def __repr__(self):
        return self.url


class Replys(Stocktwits):
    __table__ = Table('reply', stocktwits_meta, autoload=True)
    __tablename__ = 'reply'

    def __repr__(self):
        return self.reply_id
