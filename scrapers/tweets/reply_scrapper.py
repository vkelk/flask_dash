import os.path
from multiprocessing import Process
import multiprocessing.dummy
import re
import sys
from dateutil import parser as dateparser
import settings
from sqlalchemy import *
from sqlalchemy import event, exc
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
import warnings
import os
import sqlalchemy.exc
import tweet_api
from pyquery import PyQuery
import json
import time
from datetime import datetime
from pprint import pprint

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
    emoticons_str,
    r'<[^>]+>',    # HTML tags
    r'(?:@[\w_]+)',    # @-mentions
    r"(?:\#\w*[a-zA-Z]+\w*)",    # hash-tags
    r"(?:\$[A-Za-z0-9]{1,5}\b)",    # cash-tags
    # URLs
    r'http[s]?://(?:[a-z]|[0-9]|[$-_@.&amp;+]|[!*\(\),]|(?:%[0-9a-f][0-9a-f]))+',
    r'(?:(?:\d+,?)+(?:\.?\d+)?)',    # numbers
    r"(?:[a-z][a-z'\-_]+[a-z])",    # words with - and '
    r'(?:[\w_]+)',    # other words
    r'(?:\S)'    # anything else
]
tokens_re = re.compile(r'(' + '|'.join(regex_str) + ')', re.VERBOSE | re.IGNORECASE)
emoticon_re = re.compile(r'^' + emoticons_str + '$', re.VERBOSE | re.IGNORECASE)


def tokenize(s):
    return tokens_re.findall(s)


def preprocess(s, lowercase=False):
    tokens = tokenize(s)
    if lowercase:
        tokens = [token if emoticon_re.search(token) else token.lower() for token in tokens]
    return tokens


def fill(session, t, query=None, permno=None):
    if session.query(Tweet).filter_by(tweet_id=t.id).first():
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

    tokens = preprocess(t.text)
    cashtags = set([term for term in tokens if term.startswith('$') and len(term) > 1])
    hashtags = set([term for term in tokens if term.startswith('#') and len(term) > 1])
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
        session.add(user)
        try:
            session.commit()

        except sqlalchemy.exc.IntegrityError as err:
            if re.search("duplicate key value violates unique constraint", err.args[0]):
                print('ROLLBACK USER')
                session.rollback()
        except Exception as e:
            print(e)
            raise

    twit = session.query(Tweet).filter_by(tweet_id=data['tweet_id']).first()
    if not twit:
        twit = Tweet(
            tweet_id=data['tweet_id'],
            date=datetime.strptime(data['DateOfActivity'], '%d/%m/%Y'),
            time=data['TimeOfActivity'],
            timezone=data['TimeZoneUTC'][:10] if data['TimeZoneUTC'] else None,
            retweet_status=data['Re_tweet'],
            text=data['Tweet'],
            reply_to=data['ReplyTweetID'],
            location=data['Location'][:255] if data['Location'] else None,
            permalink=data['tweet_url'] if data['tweet_url'] else None,
            emoticon=','.join(t.emoji) if t.emoji else None)
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
    user.tweets.append(twit)

    if not session.query(TweetCount).filter_by(tweet_id=int(data['tweet_id'])).first():
        twit.counts.append(tweet_count)
        session.add(tweet_count)
    else:
        i = 1
    session.add(twit)
    try:
        session.commit()
        print('Inserted', twit.tweet_id, twit.reply_to)
    except sqlalchemy.exc.IntegrityError as err:
        if re.search('duplicate key value violates unique constraint', err.args[0]):
            print('ROLLBACK common')
            session.rollback()
    except Exception as e:
        print(e)
        raise
    # print(query, user.twitter_handle,t.date)
    return True


def reply(session, twitter_scraper, username, tweet_id):
    count = 0
    first = True
    j = {'has_more_items': False}
    url = 'https://twitter.com/' + username + '/status/' + str(tweet_id)
    while True:
        r1 = twitter_scraper.page.load(url)
        if first:
            res = re.findall('data-min-position="(.+?)"', r1, re.S)
        else:
            try:
                j = json.loads(r1)
                res = []
                res.append(j['min_position'])
                r1 = j['items_html']
            except:
                break
        for t in twitter_scraper.cont(r1, ''):
            if fill(session, t):
                count += 1

        if res:
            data_min = res[0]
            if (
                    first and re.search('show_more_button', r1, re.M)
                    or (not first and j['has_more_items'])
            ):    # PyQuery(r1)('li.ThreadedConversation-moreReplies').attr('data-expansion-url')
                first = False
                url = 'https://twitter.com/i/' + username + '/conversation/' + str(
                    tweet_id
                ) + '?include_available_features=1&include_entities=1&max_position=' + data_min + '&reset_error_state=false'
                continue
        break
    return count


def reply_loader(n, user_queue, pg_dsn, proxy):
    db_engine = create_engine(pg_dsn, pool_size=1)
    add_engine_pidguard(db_engine)
    DstSession = sessionmaker(bind=db_engine, autoflush=False)
    session = DstSession()
    twitter_scraper = tweet_api.TweetScraper(proxy, IS_PROFILE_SEARCH=None, logname='awam')
    print('Scraper {} with Proxy {} started'.format(n, proxy))
    # reply(session, twitter_scraper,'anatoliisharii',975426560424599552)
    while True:
        r = user_queue.get()
        if r:
            username, tweet_id = r
            c = reply(session, twitter_scraper, username, tweet_id)
            print('{:4} For tweet {} found {:5} new reply'.format(n, tweet_id, c))
        else:
            break


def check_tweet(user_queue, pg_dsn, proxy_list):
    db_engine = create_engine(pg_dsn, pool_size=1)
    add_engine_pidguard(db_engine)
    DstSession = sessionmaker(bind=db_engine, autoflush=False)
    session = DstSession()
    username_list = []
    count = 0
    for idx, tweet in enumerate(session.query(Tweet).all()):
        tweet_id = tweet.tweet_id

        # Getting the user name from permalink
        username = re.findall('https://twitter.com/(.+?)/status', tweet.permalink)[0]
        user_queue.put((username, tweet_id))

    # Send poisoned pill
    for s in proxy_list:
        user_queue.put(None)

    print('Total checked {} tweets'.format(idx, count))
    return


Base = declarative_base()
pg_config = {
    'username': settings.PG_USER,
    'password': settings.PG_PASSWORD,
    'database': settings.PG_DBNAME,
    'host': settings.DB_HOST
}
pg_dsn = "postgresql+psycopg2://{username}:{password}@{host}:5432/{database}".format(**pg_config)
db_engine = create_engine(pg_dsn)
add_engine_pidguard(db_engine)
pg_meta = MetaData(bind=db_engine, schema="fintweet")
DstSession = sessionmaker(bind=db_engine, autoflush=False)


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
