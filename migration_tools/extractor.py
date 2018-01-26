import re
import concurrent.futures as cf
from sqlalchemy import *
from sqlalchemy.orm import sessionmaker, scoped_session
from sqlalchemy.ext.declarative import declarative_base
from pprint import pprint
from datetime import datetime
from application.db_config import pg_config

pg_dsn = "postgresql+psycopg2://{username}:{password}@{host}:5432/{database}".format(**pg_config)

Base = declarative_base()
db = create_engine(pg_dsn)
pg_meta = MetaData(bind=db, schema="fintweet")


class PgUsers(Base):
    __table__ = Table('user', pg_meta, autoload=True)


class PgUsersCount(Base):
    __table__ = Table('user_count', pg_meta, autoload=True)


class PgTweets(Base):
    __table__ = Table('tweet', pg_meta, autoload=True)


class PgTweetCounts(Base):
    __table__ = Table('tweet_count', pg_meta, autoload=True)


class PgTweetCashtags(Base):
    __table__ = Table('tweet_cashtags', pg_meta, autoload=True)


class PgTweetHashtags(Base):
    __table__ = Table('tweet_hashtags', pg_meta, autoload=True)


class PgTweetMentions(Base):
    __table__ = Table('tweet_mentions', pg_meta, autoload=True)


class PgTweetUrls(Base):
    __table__ = Table('tweet_url', pg_meta, autoload=True)


# Create a session to use the tables
session_factory = sessionmaker(db, autocommit=True, autoflush=True)
# DstSession = scoped_session(session_factory)
DstSession = sessionmaker(bind=db)

session = DstSession()

emoticons_str = r"""
    (?:
        [:=;] # Eyes
        [oO\-]? # Nose (optional)
        [D\)\]\(\]/\\OpP] # Mouth
    )"""

regex_str = [
    emoticons_str,
    r'<[^>]+>',  # HTML tags
    r'(?:@[\w_]+)',  # @-mentions
    r"(?:\#+[\w_]+[\w\'_\-]*[\w_]+)",  # hash-tags
    r"(?:\$+[a-zA-Z]+[\w\'_\-]*[\w_]+)",  # cash-tags
    r'http[s]?://(?:[a-z]|[0-9]|[$-_@.&amp;+]|[!*\(\),]|(?:%[0-9a-f][0-9a-f]))+',  # URLs

    r'(?:(?:\d+,?)+(?:\.?\d+)?)',  # numbers
    r"(?:[a-z][a-z'\-_]+[a-z])",  # words with - and '
    r'(?:[\w_]+)',  # other words
    r'(?:\S)'  # anything else
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


def extract(tweet):
    global i
    tokens = preprocess(tweet.text)
    cashtags = [term for term in tokens if term.startswith('$') and len(term) > 1]
    hashtags = [term for term in tokens if term.startswith('#')]
    mentions = [term for term in tokens if term.startswith('@')]
    urls = [term for term in tokens if term.startswith('http')]
    if len(cashtags) > 0:
        print(tweet.tweet_id)
        pprint(cashtags)
        pprint(hashtags)
        pprint(mentions)
        pprint(urls)
        i += 1


if __name__ == '__main__':
    tweets = session.query(PgTweets).filter(PgTweets.tweet_id > 1425962270).order_by(PgTweets.tweet_id.asc()).yield_per(
        20)
    i = 0
    for t in tweets:
        extract(t)
        if i > 10:
            exit()
    exit()
    with cf.ThreadPoolExecutor(max_workers=4) as executor:
        try:
            executor.map(extract, tweets)
        except BaseException as e:
            print(str(e))
            raise
    exit()
    # for my_tweet in srcssn.query(MyTweets).execution_options(stream_results=True).yield_per(20):
    #     pg_tweet = dstssn.query(PgTweets).filter_by(tweet_id=my_tweet.tweet_id).first()
    #     if pg_tweet is None:
    #         transfer_tweets(my_tweet)

    # tweets = srcssn.query(MyTweets).yield_per(200).enable_eagerloads(False)
    # with cf.ThreadPoolExecutor(max_workers=4) as executor:
    #     try:
    #         executor.map(transfer_tweets, tweets)
    #     except BaseException as e:
    #         print(str(e))
    #         raise
    # del tweets

    print("ALL DONE.")
