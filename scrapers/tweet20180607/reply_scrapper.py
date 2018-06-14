from datetime import datetime
from dateutil import parser as dateparser
import json
import logging
import logging.config
import os.path
from multiprocessing import Process
import multiprocessing.dummy
import os
import re
import sys
import time
import warnings

from sqlalchemy import create_engine, MetaData, Table, func, Column, BigInteger, String, DateTime
from sqlalchemy import event, exc
from sqlalchemy.orm import sessionmaker, scoped_session
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship

import settings
import tweet_api


ISUSERPROFILE = True


class Twit:
    pass
    # def __init__(self):
    #     pass
    #
    # def json(self):
    #     return {'date': self.unixtime, 'text': self.text, 'screen_name': self.screen_name, 'user_name': self.user_name,
    #             'user_id': self.user_id,
    #             'id': self.id, 'retweets_count': self.retweets_count, 'favorites_count': self.favorites_count,
    #             'permalink': self.permalink, 'urls': self.urls, 'mentions': self.mentions,
    #             'hashtags': self.hashtags, 'is_retweet': self.is_retweet, 'symbols': self.symbols,
    #             'is_protected': self.is_protected}
    #
    # def __repr__(self):
    #     return self.text


def add_engine_pidguard(engine):
    @event.listens_for(engine, "connect")
    def connect(dbapi_connection, connection_record):
        connection_record.info['pid'] = os.getpid()

    @event.listens_for(engine, "checkout")
    def checkout(dbapi_connection, connection_record, connection_proxy):
        pid = os.getpid()
        if connection_record.info['pid'] != pid:
            # substitute log.debug() or similar here as desired
            warnings.warn("Parent process %(orig)s forked (%(newproc)s) with an open "
                          "database connection, "
                          "which is being discarded and recreated." % {
                              "newproc": pid,
                              "orig": connection_record.info['pid']
                          })
            connection_record.connection = connection_proxy.connection = None
            raise exc.DisconnectionError("Connection record belongs to pid %s, "
                                         "attempting to check out in pid %s" %
                                         (connection_record.info['pid'], pid))


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


def fill(t, query=None, permno=None):
    Session = scoped_session(session_factory)
    session = Session()
    if session.query(Tweet).filter_by(tweet_id=t.id).first():
        session.close()
        Session.remove()
        return None
    data = {}
    data['permno'] = permno if permno else None
    data['user_location'] = t.user_location
    data['LikesCount'] = t.likes
    data['Website'] = t.website
    if query:
        data['QueryStartDate'] = query[2]
        data['QueryEndDate'] = query[3]
        data['Query'] = query[1]
        data['Keyword'] = query[1]
    data['TimeOfActivity'] = time.strftime('%H:%M:%S', time.localtime(t.unixtime))
    data['DateOfActivity'] = time.strftime('%d/%m/%Y', time.localtime(t.unixtime))
    data['tweet_id'] = t.id
    data['tweet_url'] = t.permalink
    data['UserID'] = t.user_id
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
    # pprint(data)
    # exit()

    if t.utc_offset:
        if t.utc_offset / 3600 >= 0:
            data['TimeZoneUTC'] = 'UTC+' + str(int(t.utc_offset / 3600))
        else:
            data['TimeZoneUTC'] = 'UTC' + str(int(t.utc_offset / 3600))
    else:
        data['TimeZoneUTC'] = None

    tokens = tokenize(t.text)
    cashtags = set([term.upper() for term in tokens if term.startswith('$') and len(term) > 1])
    hashtags = set([term.upper() for term in tokens if term.startswith('#') and len(term) > 1])
    # mentions = [term for term in tokens if term.startswith('@') and len(term) > 1]
    urls = [term for term in tokens if term.startswith('http') and len(term) > 4]

    if ISUSERPROFILE:
        pass

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
        user = User(
            user_id=data['UserID'],
            twitter_handle=data['UserScreenName'][:120],
            user_name=data['UserName'][:120],
            location=data['user_location'][:255] if data['user_location'] else None,
            date_joined=data['DateJoined'],
            timezone=data['TimeZoneUTC'],
            website=data['Website'][:255] if data['Website'] else None,
            user_intro=t.user_description[:255] if t.user_description else None,
            verified=data['is_verified'])
        if not session.query(UserCount).filter_by(user_id=data['UserID']).first():
            user_count = UserCount(
                follower=data['UserFollowersCount'],
                following=data['UserFollowingCount'],
                tweets=data['UserTweetsCount'],
                likes=data['NumberOfFavorites'],
                lists=t.user_listed_count)
            session.add(user_count)
            user.counts.append(user_count)
        try:
            session.add(user)
            session.flush()
            session.commit()
            logger.info('Inserted new user: %s', data['UserID'])
        except exc.IntegrityError as err:
            if re.search("duplicate key value violates unique constraint", err.args[0]):
                logger.warning('ROLLBACK USER %s', data['UserID'])
                session.rollback()
        except Exception as e:
            session.close()
            Session.remove()
            logger.exception('message')
            raise

    # twit = session.query(Tweet).filter_by(tweet_id=data['tweet_id']).first()
    # if not twit:
    twit = Tweet(
        tweet_id=data['tweet_id'],
        user_id=data['UserID'],
        date=datetime.strptime(data['DateOfActivity'], '%d/%m/%Y'),
        time=data['TimeOfActivity'],
        timezone=data['TimeZoneUTC'][:10] if data['TimeZoneUTC'] else None,
        retweet_status=data['Re_tweet'],
        text=data['Tweet'],
        reply_to=data['ReplyTweetID'],
        location=data['Location'][:255] if data['Location'] else None,
        permalink=data['tweet_url'][:255] if data['tweet_url'] else None,
        emoticon=','.join(t.emoji) if t.emoji else None
        )
    # print(twit.tweet_id, twit.reply_to)
    tweet_count = TweetCount(
        reply=data['NumberOfReplies'],
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

    try:
        session.add(twit)
        session.flush()
        session.commit()
        logger.info('Inserted new Tweet: %s %s', twit.tweet_id, twit.reply_to)
        # print('Inserted', twit.tweet_id, twit.reply_to)
    except exc.IntegrityError as err:
        if re.search('duplicate key value violates unique constraint', err.args[0]):
            logger.warning('ROLLBACK duplicate entry')
            session.rollback()
    except Exception as e:
        logger.exception('message')
        session.close()
        Session.remove()
        raise
    finally:
        session.close()
        Session.remove()
    # print(query, user.twitter_handle,t.date)
    return True


def reply(twitter_scraper, username, tweet_id):
    count = 0
    first = True
    j = {'has_more_items': False}
    url = 'https://twitter.com/' + username + '/status/' + str(tweet_id)
    while True:
        res = []
        r1 = twitter_scraper.page.load(url)
        if first and r1:
            res = re.findall('data-min-position="(.+?)"', r1, re.S)
        else:
            try:
                j = json.loads(r1)
                # res = []
                res.append(j['min_position'])
                r1 = j['items_html']
            except TypeError:
                logger.warning('Empty result')
            except Exception:
                logger.exception('message')
                break
        for t in twitter_scraper.cont(r1, ''):
            if fill(t):
                count += 1

        if len(res) > 0:
            data_min = res[0]
            if (first and re.search('show_more_button', r1, re.M) or (not first and j['has_more_items'])):
                # PyQuery(r1)('li.ThreadedConversation-moreReplies').attr('data-expansion-url')
                first = False
                url = 'https://twitter.com/i/' + username + '/conversation/' \
                    + str(tweet_id) \
                    + '?include_available_features=1&include_entities=1&max_position=' \
                    + data_min + '&reset_error_state=false'
                continue
        break
    return count


def reply_loader(n, user_queue, pg_dsn, proxy):
    twitter_scraper = tweet_api.TweetScraper(proxy, IS_PROFILE_SEARCH=None, logname='awam')
    logger.info('Scraper %s with Proxy %s started', n, proxy)
    # reply(session, twitter_scraper,'anatoliisharii',975426560424599552)
    while True:
        r = user_queue.get()
        if r:
            username, tweet_id = r
            c = reply(twitter_scraper, username, tweet_id)
            logger.info('%2s For tweet %s found %s new reply(s)', n, tweet_id, c)
        else:
            break


def check_tweet(user_queue, pg_dsn, proxy_list):
    Session = scoped_session(session_factory)
    session = Session()
    q = session.query(Tweet.tweet_id, Tweet.user_id) \
        .join(TweetCashtags) \
        .group_by(Tweet.tweet_id, Tweet.user_id) \
        .yield_per(200)
    for idx, tweet in enumerate(q.all()):
        user = session.query(User.twitter_handle).filter(User.user_id == tweet.user_id).first()
        user_queue.put((user.twitter_handle, tweet.tweet_id))

    # Send poisoned pill
    for s in proxy_list:
        user_queue.put(None)

    logger.info('Total checked %s tweets', idx)
    session.close()
    Session.remove()
    return


Base = declarative_base()
pg_config = {'username': settings.PG_USER, 'password': settings.PG_PASSWORD, 'database': settings.PG_DBNAME,
                'host': settings.DB_HOST}
pg_dsn = "postgresql+psycopg2://{username}:{password}@{host}:5432/{database}".format(**pg_config)
db_engine = create_engine(
    pg_dsn,
    connect_args={"application_name": 'reply_scraper:' + str(__name__)},
    pool_size=100,
    pool_recycle=300,
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
    log_file = 'reply_scrapper_' + str(datetime.now().strftime('%Y-%m-%d')) + '.log'
    logging.config.fileConfig('log.ini', defaults={'logfilename': log_file})
    return logging.getLogger(__name__)


# def get_cashtags_list():
#     q = Session.query(mvCashtags.cashtags) \
#         .group_by(mvCashtags.cashtags) \
#         .having(func.count(mvCashtags.cashtags).between(settings.CASHTAGS_MIN_COUNT, settings.CASHTAGS_MAX_COUNT))
#     fields = ['cashtag', 'count']
#     return [dict(zip(fields, d)) for d in q.all()]


logger = create_logger()
session_factory = sessionmaker(bind=db_engine, autoflush=False)
# Session = scoped_session(session_factory)

if __name__ == '__main__':
    try:
        command = sys.argv[1]
    except IndexError:
        command = ''

    if command == 'nolocation':
        ISLOCATION = False
    else:
        ISLOCATION = True

    reply_queue = multiprocessing.Queue(1000)
    pp = []
    p = Process(target=check_tweet, args=(reply_queue, pg_dsn, settings.proxy_list))
    p.start()
    pp.append(p)

    for idx, s in enumerate(settings.proxy_list):
        # reply_loader(idx + 1, reply_queue, pg_dsn, s)
        # exit()
        p = Process(target=reply_loader, args=(idx + 1, reply_queue, pg_dsn, s))
        p.start()
        pp.append(p)

    for p in pp:
        p.join()
