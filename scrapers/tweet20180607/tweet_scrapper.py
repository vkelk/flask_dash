#!/usr/bin/env python
import os.path
from multiprocessing.dummy import Pool as ThreadPool, Lock
# from multiprocessing import Process
import csv
import logging
import logging.config
import multiprocessing.dummy
import re
import sys
import time
from dateutil import parser as dateparser
# from openpyxl import load_workbook

from datetime import datetime, timedelta
import settings
from sqlalchemy import create_engine, MetaData, Table, func, Column, BigInteger, String, DateTime
from sqlalchemy import event, exc
from sqlalchemy.orm import sessionmaker, scoped_session
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
import warnings
import os
import sqlalchemy.exc
import tweet_api
import random


IS_PROFILE_SEARCH = False
ISUSERPROFILE = True

time_wait = 0
flag1 = False

emoticons_str = r"""
    (?:
        [:=;] # Eyes
        [oO\-]? # Nose (optional)
        [D\)\]\(\]/\\OpP] # Mouth
    )"""
regex_str = [
    # emoticons_str,
    r'<[^>]+>',  # HTML tags
    r'(?:@[\w_]+)',  # @-mentions
    r"(?:\#[a-zA-Z0-9]\w+)",    # hash-tags
    r"(?:\B\$[A-Za-z][A-Za-z0-9]{0,4}\b)",    # cash-tags
    r'(?:http[s]?:\/\/(?:[a-z]|[0-9]|[$-_@.&amp;+]|[!*\(\),]|(?:%[0-9a-f][0-9a-f]))+)',  # URLs

    r'(?:(?:\d+,?)+(?:\.?\d+)?)',  # numbers
    r"(?:[a-z][a-z'\-_]+[a-z])",  # words with - and '
    r'(?:[\w_]+)',  # other words
    r'(?:\S)'  # anything else
]
tokens_re = re.compile(r'(' + '|'.join(regex_str) + ')', re.VERBOSE | re.IGNORECASE)
emoticon_re = re.compile(r'^' + emoticons_str + '$', re.VERBOSE | re.IGNORECASE)


def tokenize(s, lowercase=False):
    tokens = tokens_re.findall(s)
    if lowercase:
        tokens = [token if emoticon_re.search(token) else token.lower() for token in tokens]
    return tokens


fieldnames = ["QueryStartDate", "QueryEndDate", "Query", "DateOfActivity", "UserScreenName", "Keyword", "Location",
                "Website", "DateJoined", "IsMention", "UserID", "TimeOfActivity", "Hashtags", "Re_tweet",
                "NumberOfReplies", "NumberOfRe_tweets", "NumberOfFavorites", "Tweet", "tweet_id", "tweet_url",
                "is_verified", "Urls", "UserFollowersCount", "UserFollowingCount", "UserTweetsCount", "LikesCount",
                "CashtagSymbols", "user_location", "permno"]


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


def scra(query, i, proxy, lock, session=None):
    twitter_scraper = tweet_api.TweetScraper(proxy, IS_PROFILE_SEARCH=IS_PROFILE_SEARCH, logname='awam')
    count = 0
    idx = 0

    # Receiving random credentials from the settings file
    credential = settings.twitter_logins[random.randrange(0, len(settings.twitter_logins))]
    login = credential['login']
    password = credential['password']
    for idx, t in enumerate(twitter_scraper.get_new_search(query, login, password)):
        data = {}
        data['tweet_id'] = t.id
        data['UserID'] = t.user_id
        session = Session()
        twit = session.query(Tweet).filter_by(tweet_id=data['tweet_id']).first()
        # if twit and ISUSERPROFILE:
        if twit and twit.user_id:
            logger.debug('Tweet %s exists in database', twit.tweet_id)
            session.close()
            Session.remove()
            continue
        if twit and twit.user_id is None:
            try:
                twit.user_id = data['UserID']
                session.add(twit)
                session.flush()
                session.commit()
                logger.info('Updated tweet_id %s with user_id %s', data['tweet_id'], data['UserID'])
            except Exception as e:
                logger.exception('message')
                raise
            Session.remove()
            continue
        # data['permno'] = permno
        data['user_location'] = t.user_location
        data['LikesCount'] = t.likes
        data['Website'] = t.website
        data['QueryStartDate'] = query[2]
        data['QueryEndDate'] = query[3]
        data['Query'] = query[1]
        data['Keyword'] = query[1]
        data['TimeOfActivity'] = time.strftime('%H:%M:%S', time.localtime(t.unixtime))
        data['DateOfActivity'] = time.strftime('%d/%m/%Y', time.localtime(t.unixtime))
        data['tweet_url'] = t.permalink
        data['UserScreenName'] = t.screen_name
        data['UserName'] = t.user_name
        data['TimeZone'] = t.time_zone
        data['UserTweetsCount'] = t.user_tweet_count
        data['UserFollowersCount'] = t.user_followers_count
        data['UserFollowingCount'] = t.user_following_count
        data['NumberOfFavorites'] = t.favorites_count
        data['NumberOfRe_tweets'] = t.retweets_count
        data['NumberOfReplies'] = t.replyes_count
        data['Re_tweet'] = t.is_retweet
        data['is_verified'] = t.is_verified
        # data['isProtected'] = t.is_protected
        data['isReply'] = t.is_reply
        try:
            data['ReplyTweetID'] = int(t.data_conversation_id)
        except ValueError:
            data['ReplyTweetID'] = None
        data['ReplyUserId'] = t.is_reply_id
        # data['ReplyScreenName'] = t.is_reply_screen_name
        # data['Lang'] = t.lang
        data['Hashtags'] = ' '.join(t.hashtags)
        data['Urls'] = ', '.join(t.urls)
        data['CashtagSymbols'] = '\n'.join(t.symbols)
        # data['Mentions_id'] = '\n'.join(t.mentions_id)
        data['IsMention'] = ' '.join(t.mentions_name)
        data['Location'] = t.location_name
        # data['Location ID'] = t.location_id
        data['DateJoined'] = dateparser.parse(t.user_created) if t.user_created else None
        data['Tweet'] = t.text

        if hasattr(t, 'utc_offset') and t.utc_offset:
            if t.utc_offset / 3600 >= 0:
                data['TimeZoneUTC'] = 'UTC+' + str(int(t.utc_offset / 3600))
            else:
                data['TimeZoneUTC'] = 'UTC' + str(int(t.utc_offset / 3600))
        else:
            data['TimeZoneUTC'] = None

        tokens = tokenize(t.text)
        cashtags = set([term.upper() for term in tokens if term.startswith('$') and len(term) > 1])
        if len(cashtags) == 0:
            logger.info('Skipping %s does not contain cashtags', data['tweet_id'])
            Session.remove()
            continue
        hashtags = set([term.upper() for term in tokens if term.startswith('#') and len(term) > 1])
        # mentions = [term for term in tokens if term.startswith('@') and len(term) > 1]
        urls = set([term for term in tokens if term.startswith('http') and len(term) > 4])

        if ISUSERPROFILE:
            pass
            # dd = re.findall('\w+ (\w+) (\d+) \d+:\d+:\d+ \+\d+ (\d+)', data['DateJoined'])
            # try:
            #     date_joined = datetime.strptime(' '.join(dd[0]), '%b %d %Y')
            # except IndexError:
            #     print(data['DateJoined'], dd)
            #     exit()
        else:
            date_joined = None
            t.user_listed_count = None
            t.username = t.user_name
            t.user_timezone = None
            t.user_description = None
            data['Website'] = None
            data['is_verified'] = None

        user = session.query(User).filter_by(user_id=data['UserID']).first()
        if not user:
            user = User(user_id=data['UserID'],
                        twitter_handle=data['UserScreenName'][:120],
                        user_name=data['UserName'][:120],
                        location=data['user_location'][:255] if data['user_location'] else None,
                        date_joined=data['DateJoined'],
                        timezone=data['TimeZoneUTC'][:10] if data['TimeZoneUTC'] else None,
                        website=data['Website'][:255] if data['Website'] else None,
                        user_intro=t.user_description[:255] if t.user_description else None,
                        verified=data['is_verified'])
            if not session.query(UserCount).filter_by(user_id=data['UserID']).first():
                user_count = UserCount(follower=data['UserFollowersCount'],
                                       following=data['UserFollowingCount'],
                                       tweets=data['UserTweetsCount'],
                                       likes=data['NumberOfFavorites'],
                                       lists=t.user_listed_count)
            try:
                session.add(user_count)
                user.counts.append(user_count)
                session.add(user)
                session.flush()
                session.commit()
                logger.info('Inserted new user: %s', data['UserID'])
            except sqlalchemy.exc.IntegrityError as err:
                if re.search("duplicate key value violates unique constraint", err.args[0]):
                    logger.warning('ROLLBACK USER %s', data['UserID'])
                    session.rollback()
            except Exception as e:
                logger.exception('message')
                raise
        # twit = session.query(Tweet).filter_by(tweet_id=data['tweet_id']).first()
        # if not twit:
        if data['isReply'] and data['ReplyTweetID'] != data['tweet_id']:
            reply_to = data['ReplyTweetID']
        else:
            reply_to = None
        twit = Tweet(tweet_id=data['tweet_id'],
                     user_id=data['UserID'],
                     date=datetime.strptime(data['DateOfActivity'], '%d/%m/%Y'),
                     time=data['TimeOfActivity'],
                     timezone=data['TimeZoneUTC'][:10] if data['TimeZoneUTC'] else None,
                     retweet_status=data['Re_tweet'],
                     text=data['Tweet'],
                     reply_to=reply_to,
                     location=data['Location'][:255] if data['Location'] else None,
                     permalink=data['tweet_url'][:255] if data['tweet_url'] else None,
                     emoticon=','.join(t.emoji) if t.emoji else None)
        tweet_count = TweetCount(reply=data['NumberOfReplies'],
                                 favorite=data['NumberOfFavorites'],
                                 retweet=t.retweets_count)

        if not session.query(TweetHashtags).filter_by(tweet_id=data['tweet_id']).first():
            for hash_s in hashtags:
                tweet_hashtag = TweetHashtags(hashtags=hash_s[:45])
                twit.hash_s.append(tweet_hashtag)
                session.add(tweet_hashtag)

        if not session.query(TweetUrl).filter_by(tweet_id=data['tweet_id']).first():
            for url_s in urls:
                tweet_url = TweetUrl(url=url_s[:255])
                twit.url_s.append(tweet_url)
                session.add(tweet_url)

        if not session.query(TweetCashtags).filter_by(tweet_id=data['tweet_id']).first():
            for cash_s in cashtags:
                tweet_cashtag = TweetCashtags(cashtags=cash_s[:45])
                twit.cash_s.append(tweet_cashtag)
                session.add(tweet_cashtag)

        if not session.query(TweetMentions).filter_by(tweet_id=data['tweet_id']).first():
            for ment_s in t.ment_s:
                tweet_mentions = TweetMentions(mentions=ment_s[0][:45], user_id=ment_s[1])
                twit.ment_s.append(tweet_mentions)
                session.add(tweet_mentions)
        # user.tweets.append(twit)

        if not session.query(TweetCount).filter_by(tweet_id=int(data['tweet_id'])).first():
            twit.counts.append(tweet_count)
            session.add(tweet_count)
        else:
            i = 1
        try:
            session.add(twit)
            session.flush()
            session.commit()
            if twit.reply_to is not None:
                logger.info('Inserted new Tweet %s as reply to %s', twit.tweet_id, twit.reply_to)
            else:
                logger.info('Inserted new Tweet: %s', twit.tweet_id)
        except sqlalchemy.exc.IntegrityError as err:
            if re.search('duplicate key value violates unique constraint', err.args[0]):
                logger.warning('ROLLBACK duplicate entry')
                session.rollback()
        except Exception as e:
            logger.exception('message')
            raise
        finally:
            Session.remove()
        count += 1
    logger.info('Query %s: %s tweets of which %s are new', query, idx, count)
    lock.acquire()
    date_begin = query[2]
    date_end = query[3]
    if count >= 0:
        with open('report.csv', 'a') as f:
            data = {}
            fdnames = ['time', 'query_name', 'number', 'date_from', 'date_to']
            writer = csv.DictWriter(f, lineterminator='\n', fieldnames=fdnames, dialect='excel', quotechar='"',
                                    quoting=csv.QUOTE_ALL)
            data['time'] = time.strftime('%Y-%m-%d %H:%M:%S')
            data['query_name'] = query[1]
            data['number'] = count
            data['date_from'] = date_begin
            data['date_to'] = date_end
            writer.writerow(data)
        lock.release()
        return count
    else:
        logger.warning('Count is %s', count)
        with open('error.csv', 'a') as f:
            data = {}
            fdnames = ['time', 'query_name', 'number', 'date_from', 'date_to']
            writer = csv.DictWriter(f, lineterminator='\n', fieldnames=fdnames, dialect='excel', quotechar='"',
                                    quoting=csv.QUOTE_ALL)
            data['time'] = time.strftime('%Y-%m-%d %H:%M:%S')
            data['query_name'] = query[1]
            data['number'] = count
            data['date_from'] = date_begin
            data['date_to'] = date_end
            writer.writerow(data)
        lock.release()
        return False


def scrape_query(user_queue, proxy, lock, pg_dsn):
    while not user_queue.empty():
        query, i = user_queue.get()

        logging.info('START %s %s %s', i, proxy, query)
        # TODO filter:nativeretweets
        try:
            res = scra(query, i, proxy, lock)
        except tweet_api.LoadingError:
            logger.warning('LoadingError except')
            return False
        except Exception:
            logger.exception('message')
            raise
        # if not res:
        #     logger.warning('Empty scrape result %s %s', query, i)
        #     # print('     SCRAP_USER Error in', query, i)
        #     with open('error_list.txt', 'a') as f:
        #         f.write(query[0] + '\n')
        # else:
        logger.info('ENDED %s, %s, %s, %s', i, proxy, query, res)
    return True


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


def get_cashtags_list():
    q = Session.query(TweetCashtags.cashtags) \
        .join(Tweet) \
        .filter(Tweet.date.between('2012-01-01', '2016-12-31')) \
        .group_by(TweetCashtags.cashtags) \
        .having(func.count(TweetCashtags.cashtags).between(settings.CASHTAGS_MIN_COUNT, settings.CASHTAGS_MAX_COUNT))
    fields = ['cashtag', 'count']
    return [dict(zip(fields, d)) for d in q.all()]


def create_logger():
    log_file = 'tweet_scrapper_' + str(datetime.now().strftime('%Y-%m-%d')) + '.log'
    logging.config.fileConfig('log.ini', defaults={'logfilename': log_file})
    return logging.getLogger(__name__)


logger = create_logger()

Base = declarative_base()
pg_config = {'username': settings.PG_USER, 'password': settings.PG_PASSWORD, 'database': settings.PG_DBNAME,
                'host': settings.DB_HOST}
pg_dsn = "postgresql+psycopg2://{username}:{password}@{host}:5432/{database}".format(**pg_config)
db_engine = create_engine(pg_dsn, connect_args={"application_name": 'tweet_scraper:' + str(__name__)})
add_engine_pidguard(db_engine)
pg_meta = MetaData(bind=db_engine, schema="fintweet")
try:
    db_engine.engine.execute('SELECT 1')
except exc.OperationalError:
    logger.error('Cannot connect to database server %s', settings.DB_HOST)
    exit()


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


# class mvCashtags(Base):
#     # __table__ = Table('mv_cashtags', fintweet_meta, autoload=True)
#     __tablename__ = 'mv_cashtags'
#     __table_args__ = {"schema": "fintweet"}

#     id = Column(BigInteger, primary_key=True)
#     tweet_id = Column(BigInteger)
#     user_id = Column(BigInteger)
#     cashtags = Column(String(120))
#     datetime = Column(DateTime)




if __name__ == '__main__':
    DstSession = sessionmaker(bind=db_engine, autoflush=False)
    # dstssn = DstSession()
    session_factory = sessionmaker(bind=db_engine, autoflush=False)
    Session = scoped_session(session_factory)

    try:
        command = sys.argv[1]
    except IndexError:
        command = ''

    if command == 'nolocation':
        ISLOCATION = False
    else:
        ISLOCATION = True

    user_queue = multiprocessing.dummy.Queue()
    working_ctags = get_cashtags_list()
    logger.debug('Total %s cashtags will be processed', len(working_ctags))
    i = 0
    t1 = settings.DATE_START
    t2 = settings.DATE_END
    date_delta = datetime.strptime(t2, '%Y-%m-%d') - datetime.strptime(t1, '%Y-%m-%d')
    for days in range(0, date_delta.days + 1, settings.FREQUENCY):
        date_end = datetime.strptime(t2, '%Y-%m-%d') - timedelta(days=days)
        date_start = date_end - timedelta(days=settings.FREQUENCY)
        for ticker in working_ctags:
            query = ticker['cashtag'].lower().strip('$'), \
                ticker['cashtag'].lower().strip(' '), date_start.strftime("%Y-%m-%d"), date_end.strftime("%Y-%m-%d")
            user_queue.put((query, i))
            i += 1
    try:
        lock = Lock()
        # Single process for testings
        # scrape_query(user_queue, settings.proxy_list[5], lock, pg_dsn)
        # exit()

        pool = ThreadPool(len(settings.proxy_list))
        pool.map(lambda x: (scrape_query(user_queue, x, lock, pg_dsn)), settings.proxy_list)
    except KeyboardInterrupt:
        logger.warning('Interrupted by keyboard.')
    finally:
        db_engine.dispose()
        logger.info('db_engine disposed')
        logger.info('Exiting...')
        exit()
