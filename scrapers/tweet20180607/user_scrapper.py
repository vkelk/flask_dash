from datetime import datetime
from dateutil import parser as dateparser
import os.path
import logging
import logging.config
from multiprocessing import Process
import multiprocessing.dummy
import re
import sys
import warnings
import os

from sqlalchemy import create_engine, MetaData, Table, event, exc, Column, BigInteger, String, DateTime, distinct
from sqlalchemy.orm import sessionmaker, relationship
from sqlalchemy.ext.declarative import declarative_base
import sqlalchemy.exc

import settings
import tweet_api

ISUSERPROFILE = True


class Twit:
    def __init__(self):
        pass

    def json(self):
        return {'date': self.unixtime, 'text': self.text, 'screen_name': self.screen_name, 'user_name': self.user_name,
                'user_id': self.user_id,
                'id': self.id, 'retweets_count': self.retweets_count, 'favorites_count': self.favorites_count,
                'permalink': self.permalink, 'urls': self.urls, 'mentions': self.mentions,
                'hashtags': self.hashtags, 'is_retweet': self.is_retweet, 'symbols': self.symbols,
                'is_protected': self.is_protected}

    def __repr__(self):
        return self.text


def add_engine_pidguard(engine):
    @event.listens_for(engine, "connect")
    def connect(dbapi_connection, connection_record):
        connection_record.info['pid'] = os.getpid()

    @event.listens_for(engine, "checkout")
    def checkout(dbapi_connection, connection_record, connection_proxy):
        pid = os.getpid()
        if connection_record.info['pid'] != pid:
            # substitute log.debug() or similar here as desired
            warnings.warn(
                "Parent process %(orig)s forked (%(newproc)s) with an open "
                "database connection, "
                "which is being discarded and recreated." %
                {"newproc": pid, "orig": connection_record.info['pid']})
            connection_record.connection = connection_proxy.connection = None
            raise exc.DisconnectionError(
                "Connection record belongs to pid %s, "
                "attempting to check out in pid %s" %
                (connection_record.info['pid'], pid)
            )


def user_loader(n, user_queue, pg_dsn, proxy):
    # db_engine = create_engine(pg_dsn, pool_size=1)
    # add_engine_pidguard(db_engine)
    DstSession = sessionmaker(bind=db_engine, autoflush=False)
    session = DstSession()
    twitter_scraper = tweet_api.TweetScraper(proxy, IS_PROFILE_SEARCH=None, logname='awam')
    logger.info('Scraper %s with Proxy %s started', n, proxy)
    # print('Scraper {} with Proxy {} started'.format(n, proxy))
    while True:
        is_there_anything_to_record = False
        r = user_queue.get()
        if r:
            username, user_id = r
            u = Twit()
            if twitter_scraper.get_user_profile(username, u):
                if not user_id:
                    user_id = u.user_id
                user = session.query(User).filter_by(user_id=user_id).first()
                if hasattr(u, 'user_status') and u.user_status in ['not_exists', 'suspended']:
                    user.user_status = u.user_status
                    try:
                        session.add(user)
                        session.commit()
                        logger.warning('%s user status set to %s', username, u.user_status)
                    except Exception as e:
                        logger.exception("message")
                        # logger.error('%s %s', type(e), str(e))
                        raise
                    continue
                if u.utc_offset:
                    if u.utc_offset / 3600 >= 0:
                        timezone = 'UTC+' + str(int(u.utc_offset / 3600))
                    else:
                        timezone = 'UTC' + str(int(u.utc_offset / 3600))
                else:
                    timezone = None

                if not user:
                    user = User(user_id=user_id,
                                twitter_handle=username[:120],
                                user_name=u.username[:120],
                                location=u.user_location[:255] if u.user_location else None,
                                date_joined=dateparser.parse(u.user_created) if u.user_created else None,
                                timezone=timezone[:10] if timezone else None,
                                website=u.website[:255] if u.website else None,
                                user_intro=u.user_description[:255] if u.user_description else None,
                                verified=u.is_verified)
                else:
                    user.user_name = u.username[:120]
                    user.location = u.user_location[:255] if u.user_location else None
                    user.date_joined = dateparser.parse(u.user_created) if u.user_created else None
                    user.timezone = timezone[:10] if timezone else None
                    user.website = u.website[:255] if u.website else None
                    user.user_intro = u.user_description[:255] if u.user_description else None
                    user.verified = u.is_verified
                session.add(user)
                is_there_anything_to_record = True
                user_count = session.query(UserCount).filter_by(user_id=user_id).first()
                if not user_count:
                    user_count = UserCount(follower=u.user_followers_count, following=u.user_following_count,
                        tweets=u.user_tweet_count, likes=u.likes, lists=u.user_listed_count)
                else:
                    user_count.follower = u.user_followers_count
                    user_count.following = u.user_following_count
                    user_count.tweets = u.user_tweet_count
                    user_count.likes = u.likes
                    user_count.lists = u.user_listed_count
                session.add(user_count)
                user.counts.append(user_count)
                is_there_anything_to_record = True

                if is_there_anything_to_record:
                    try:
                        session.commit()
                        logger.info('Updated user: %s %s', user_id, username)
                        # print('Updated user:', user_id)
                    except sqlalchemy.exc.IntegrityError as err:
                        if re.search("duplicate key value violates unique constraint", err.args[0]):
                            logger.warning('ROLLBACK USER %s', username)
                            # print('ROLLBACK USER', username)
                            session.rollback()
                    except Exception as e:
                        logger.error('%s %s with user %s', type(e), str(e), username)
                        raise
                    # logger.info('%s Loaded %s', n, username)
                    # print('{} Loaded {}'.format(n, username))
            del (u)
        else:
            logger.warning('Empty queue')
            return


def user_incomplete(user):
    if user.user_name is None:
        return True
    elif user.location is None:
        return True
    elif user.date_joined is None:
        return True
    elif user.timezone is None:
        return True
    return False


def counts_incomplete(counts):
    if counts.follower == 0:
        return True
    elif counts.following == 0:
        return True
    elif counts.tweets == 0:
        return True
    elif counts.likes == 0:
        return True
    return False


def check_user(user_queue, pg_dsn, proxy_list):
    # db_engine = create_engine(pg_dsn, pool_size=1)
    # add_engine_pidguard(db_engine)
    DstSession = sessionmaker(bind=db_engine, autoflush=False)
    session = DstSession()
    username_list = []
    count = 0
    try:
        for idx, user_id in enumerate(get_users_list()):
            # user_id = tweet.user_id
            user = session.query(User).filter_by(user_id=user_id).first()
            counts = session.query(UserCount).filter_by(user_id=user_id).first()
            if not user or user_incomplete(user) or counts_incomplete(counts):
                # Getting the user name from permalink
                # print('Permalink', tweet.permalink)
                # username = re.findall('https://twitter.com/(.+?)/status', tweet.permalink)[0]
                username = user.twitter_handle
                if username not in username_list:
                    user_queue.put((username, user_id))
                    count += 1
                    username_list.append(username)
    except Exception:
        logger.exception('message')
        raise
    logger.info('Total checked %s users, processed %s', idx, count)
    # Send poisoned pill
    for s in proxy_list:
        user_queue.put(None)
    # print('Total checked {} users, processed {}'.format(idx, count))
    return


def get_users_list():
    session = DstSession()
    q = session.query(distinct(Tweet.user_id)) \
        .filter(Tweet.date.between('2012-01-01', '2016-12-31')) \
        .join(TweetCashtags) \
        .group_by(Tweet.user_id)
    return [d[0] for d in q.all()]


Base = declarative_base()
pg_config = {'username': settings.PG_USER, 'password': settings.PG_PASSWORD, 'database': settings.PG_DBNAME,
             'host': settings.DB_HOST}
pg_dsn = "postgresql+psycopg2://{username}:{password}@{host}:5432/{database}".format(**pg_config)
db_engine = create_engine(
    pg_dsn,
    connect_args={"application_name": 'user_scraper:' + str(__name__)},
    pool_size=100, 
    max_overflow=0, 
    encoding='utf-8'
    )
add_engine_pidguard(db_engine)
pg_meta = MetaData(bind=db_engine, schema="fintweet")


class User(Base):
    __table__ = Table('user', pg_meta, autoload=True)
    tweets = relationship('Tweet')
    counts = relationship('UserCount')


class UserCount(Base):
    __table__ = Table('user_count', pg_meta, autoload=True)


class TweetCount(Base):
    __table__ = Table('tweet_count', pg_meta, autoload=True)


class TweetMentions(Base):
    __table__ = Table('tweet_mentions', pg_meta, autoload=True)


class TweetCashtags(Base):
    __table__ = Table('tweet_cashtags', pg_meta, autoload=True)


class TweetHashtags(Base):
    __table__ = Table('tweet_hashtags', pg_meta, autoload=True)


class TweetUrl(Base):
    __table__ = Table('tweet_url', pg_meta, autoload=True)


class Tweet(Base):
    __table__ = Table('tweet', pg_meta, autoload=True)
    counts = relationship('TweetCount')
    ment_s = relationship('TweetMentions')
    cash_s = relationship('TweetCashtags')
    hash_s = relationship('TweetHashtags')
    url_s = relationship('TweetUrl')


class mvCashtags(Base):
    # __table__ = Table('mv_cashtags', fintweet_meta, autoload=True)
    __tablename__ = 'mv_cashtags'
    __table_args__ = {"schema": "fintweet"}

    id = Column(BigInteger, primary_key=True)
    tweet_id = Column(BigInteger)
    user_id = Column(BigInteger)
    cashtags = Column(String(120))
    datetime = Column(DateTime)


def create_logger():
    log_file = 'user_scrapper_' + str(datetime.now().strftime('%Y-%m-%d')) + '.log'
    logging.config.fileConfig('log.ini', defaults={'logfilename': log_file})
    return logging.getLogger(__name__)


DstSession = sessionmaker(bind=db_engine, autoflush=False)
logger = create_logger()

if __name__ == '__main__':
    dstssn = DstSession()
    try:
        command = sys.argv[1]
    except IndexError:
        command = ''

    if command == 'nolocation':
        ISLOCATION = False
    else:
        ISLOCATION = True

    user_queue = multiprocessing.Queue(1000)
    pp = []
    p = Process(target=check_user, args=(user_queue, pg_dsn, settings.proxy_list))
    p.start()
    pp.append(p)

    for idx, s in enumerate(settings.proxy_list):
        p = Process(target=user_loader, args=(idx + 1, user_queue, pg_dsn, s))
        p.start()
        pp.append(p)

    for p in pp:
        p.join()
