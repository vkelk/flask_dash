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
db = create_engine(pg_dsn, pool_size=100, max_overflow=0)
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
    __table__ = Table('tweet_cashtags_new', pg_meta, autoload=True)


class PgTweetHashtags(Base):
    __table__ = Table('tweet_hashtags', pg_meta, autoload=True)


class PgTweetMentions(Base):
    __table__ = Table('tweet_mentions', pg_meta, autoload=True)


class PgTweetUrls(Base):
    __table__ = Table('tweet_url', pg_meta, autoload=True)


# Create a session to use the tables
session_factory = sessionmaker(db, autocommit=True, autoflush=True)
# Session = scoped_session(session_factory)
Session = sessionmaker(bind=db)

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
    r"(?:\#+[\w_]+[\w\'_\-]*[\w_]+)",  # hash-tags
    # r"(?:\$+[a-zA-Z]+[\w\'_\-]*[\w_]+)",  # cash-tags
    r"(?:\$[a-zA-Z.:]{1,11}\b)",  # cash-tags
    r'http[s]?://(?:[a-z]|[0-9]|[$-_@.&amp;+]|[!*\(\),]|(?:%[0-9a-f][0-9a-f]))+',  # URLs

    r'(?:(?:\d+,?)+(?:\.?\d+)?)',  # numbers
    r"(?:[a-z][a-z'\-_]+[a-z])",  # words with - and '
    r'(?:[\w_]+)',  # other words
    r'(?:\S)'  # anything else
]

regex_cashtags = r"(\$[a-zA-Z]{1,7}(?:[.:]{1}[a-zA-Z]{1,7})?\b)"  # cash-tags

tokens_re = re.compile(regex_cashtags, re.VERBOSE | re.IGNORECASE)
emoticon_re = re.compile(r'^' + emoticons_str + '$', re.VERBOSE | re.IGNORECASE)


def tokenize(s):
    return tokens_re.findall(s)


def preprocess(s, lowercase=False, upercase=False):
    tokens = tokenize(s)
    if lowercase:
        tokens = [token if emoticon_re.search(token) else token.lower() for token in tokens]
    if upercase:
        tokens = [token.upper() for token in tokens]
    return tokens


def extract(tweet):
    global i
    tokens = preprocess(tweet.text, upercase=True)
    cashtags = set([term for term in tokens if term.startswith('$') and len(term) > 1])
    # hashtags = [term for term in tokens if term.startswith('#') and len(term) > 1]
    # mentions = [term for term in tokens if term.startswith('@') and len(term) > 1]
    # urls = [term for term in tokens if term.startswith('http') and len(term) > 4]
    for cashtag in cashtags:
        if len(cashtag) > 1 and len(cashtag) <= 11:
            process_cashtag(tweet.tweet_id, cashtag)
        # for hashtag in hashtags:
        #     process_hashtag(tweet.tweet_id, hashtag)
        # for mention in mentions:
        #     process_mention(tweet.tweet_id, mention)
        # for url in urls:
        #     process_url(tweet.tweet_id, url)


def process_cashtag(tweet_id, cashtag):
    subsession = Session()
    pg_cashtag = subsession.query(PgTweetCashtags).filter_by(tweet_id=tweet_id).filter_by(cashtags=cashtag).first()
    if pg_cashtag is None:
        try:
            item = PgTweetCashtags(
                tweet_id=tweet_id,
                cashtags=cashtag
            )
            subsession.add(item)
            subsession.commit()
            print(tweet_id, "-> Inserted cashtag", cashtag)
        except BaseException as e:
            print(str(e))
            raise


def process_hashtag(tweet_id, hashtag):
    subsession = Session()
    pg_hashtag = subsession.query(PgTweetHashtags).filter_by(tweet_id=tweet_id).filter_by(hashtags=hashtag).first()
    if pg_hashtag is None:
        try:
            item = PgTweetHashtags(
                tweet_id=tweet_id,
                hashtags=hashtag
            )
            subsession.add(item)
            subsession.commit()
            print(tweet_id, "-> Inserted cashtag", hashtag)
        except BaseException as e:
            print(str(e))
            raise


def process_mention(tweet_id, mention):
    subsession = Session()
    pg_mention = subsession.query(PgTweetMentions).filter_by(tweet_id=tweet_id).filter_by(mentions=mention).first()
    if pg_mention is None:
        try:
            item = PgTweetMentions(
                tweet_id=tweet_id,
                mentions=mention
            )
            subsession.add(item)
            subsession.commit()
            print(tweet_id, "-> Inserted mention", mention)
        except BaseException as e:
            print(str(e))
            raise


def process_url(tweet_id, url):
    subsession = Session()
    pg_url = subsession.query(PgTweetUrls).filter_by(tweet_id=tweet_id).filter_by(url=url).first()
    if pg_url is None:
        try:
            item = PgTweetUrls(
                tweet_id=tweet_id,
                url=url
            )
            subsession.add(item)
            subsession.commit()
            print(tweet_id, "-> Inserted url", url)
        except BaseException as e:
            print(str(e))
            raise


if __name__ == '__main__':
    session = Session()

    for t in session.query(PgTweets).filter(PgTweets.tweet_id > 1) \
            .order_by(PgTweets.tweet_id.asc()).yield_per(100):
        extract(t)

    # tweets = session.query(PgTweets).filter(PgTweets.tweet_id > 295453374990671872) \
    #     .order_by(PgTweets.tweet_id.asc()).yield_per(100)
    # with cf.ProcessPoolExecutor(max_workers=4) as executor:
    #     try:
    #         executor.map(extract, tweets)
    #     except BaseException as e:
    #         print(str(e))
    #         raise

    print("ALL DONE.")
