import concurrent.futures as cf
import csv
import logging
import logging.config
import os
import re
import sys
import time
from sqlalchemy import create_engine, MetaData, Table
from sqlalchemy.orm import sessionmaker, scoped_session
from sqlalchemy.ext.declarative import declarative_base
from pprint import pprint
from datetime import datetime
from db_config import pg_config

DIR_PATH = os.path.dirname(os.path.realpath(__file__))
LOG_DIR = os.path.join(DIR_PATH, 'logs')
CSV_DIR = os.path.join(os.path.dirname(DIR_PATH), 'files/company_ids')

if not os.path.exists(LOG_DIR):
    os.makedirs(LOG_DIR)

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
    r"(\$[a-zA-Z]{1,7}(?:[.:]{1}[a-zA-Z]{1,7})?\b)",  # cash-tags
    r'http[s]?://(?:[a-z]|[0-9]|[$-_@.&amp;+]|[!*\(\),]|(?:%[0-9a-f][0-9a-f]))+',  # URLs
    r'(?:(?:\d+,?)+(?:\.?\d+)?)',  # numbers
    r"(?:[a-z][a-z'\-_]+[a-z])",  # words with - and '
    r'(?:[\w_]+)',  # other words
    r'(?:\S)'  # anything else
]
tokens_re = re.compile(r'(' + '|'.join(regex_str) + ')', re.VERBOSE | re.IGNORECASE)
emoticon_re = re.compile(r'^' + emoticons_str + '$', re.VERBOSE | re.IGNORECASE)

pg_dsn = "postgresql+psycopg2://{username}:{password}@{host}:5432/{database}".format(**pg_config)
Base = declarative_base()
db_engine = create_engine(pg_dsn, pool_size=100, max_overflow=0)
db_meta = MetaData(bind=db_engine, schema="fintweet")
session_factory = sessionmaker(db_engine, autocommit=True, autoflush=True)
Session = scoped_session(session_factory)


# Database definition clases
class User(Base):
    __table__ = Table('user', db_meta, autoload=True)


class UserCount(Base):
    __table__ = Table('user_count', db_meta, autoload=True)


class Tweet(Base):
    __table__ = Table('tweet', db_meta, autoload=True)


class TweetCount(Base):
    __table__ = Table('tweet_count', db_meta, autoload=True)


class TweetCashtag(Base):
    __table__ = Table('tweet_cashtags', db_meta, autoload=True)


class TweetHashtag(Base):
    __table__ = Table('tweet_hashtags', db_meta, autoload=True)


class TweetMentions(Base):
    __table__ = Table('tweet_mentions', db_meta, autoload=True)


class TweetUrls(Base):
    __table__ = Table('tweet_url', db_meta, autoload=True)


def fname_exists(fname):
    session = Session()
    file_db = session.query(FileInfo).filter_by(filename=fname).first()
    Session.remove()
    if file_db is None:
        return False
    elif file_db.status == 'finished':
        return True
    return False


def add_user(row):
    user = User(
        user_id=row['UserID'],
        twitter_handle=row['UserScreenName'],
        user_name=row['UserName'],
        verified=row['isVerified'],
        is_company=True
    )
    return user


def add_user_count(row):
    FollowersCount = to_integer(row['UserFollowersCount'])
    FollowingCount = to_integer(row['UserFollowingCount'])
    TweetsCount = to_integer(row['UserTweetsCount'])
    # LikesCount = to_integer(row['FavoriteCount'])
    user_count = UserCount(
        user_id=row['UserID'],
        follower=FollowersCount,
        following=FollowingCount,
        tweets=TweetsCount
        # likes=LikesCount
    )
    return user_count


def add_tweet(row):
    try:
        date = datetime.strptime(row['Date'], '%d/%m/%Y')
    except BaseException as e:
        logger.warning(str(e))
        date = None
    tweet = Tweet(
        tweet_id=row['TweetID'],
        date=date,
        time=row['Time'],
        retweet_status=row['isRetweet'],
        text=row['Text'],
        user_id=row['UserID'],
        reply_to=row['ReplyTweetID'],
        permalink=row['Permalink']
    )
    return tweet


def add_tweet_count(row):
    Replies = to_integer(row['ReplyesCount'])
    Retweets = to_integer(row['RetweetCount'])
    Favorites = to_integer(row['FavoriteCount'])
    tweet_count = TweetCount(
        tweet_id=row['TweetID'],
        reply=Replies,
        retweet=Retweets,
        favorite=Favorites
    )
    return tweet_count


def add_tweet_cashtag(row, cashtag):
    tweet_cashtag = TweetCashtag(
        tweet_id=row['TweetID'],
        cashtags=cashtag
    )
    return tweet_cashtag


def add_tweet_hashtag(row, hashtag):
    tweet_hashtag = TweetHashtag(
        tweet_id=row['TweetID'],
        hashtags=hashtag
    )
    return tweet_hashtag


def add_tweet_mention(row, mention):
    mentions, mentions_id = mention
    tweet_mention = TweetMentions(
        tweet_id=row['TweetID'],
        mentions=mentions,
        user_id=mentions_id
    )
    return tweet_mention


def add_tweet_url(row, url):
    if len(url) > 255:
        url = url[:255]
    tweet_url = TweetUrls(
        tweet_id=row['TweetID'],
        url=url
    )
    return tweet_url


def add_file_info(fname, row, count):
    try:
        date_start = datetime.strptime(row['QueryStartDate'], '%Y-%m-%d')
    except BaseException as e:
        try:
            date_start = datetime.strptime(row['QueryStartDate'], '%m/%d/%Y')
        except BaseException as e:
            logger.warning(str(e))
            date_start = None
    try:
        date_end = datetime.strptime(row['QueryEndDate'], '%Y-%m-%d')
    except BaseException as e:
        try:
            date_end = datetime.strptime(row['QueryEndDate'], '%m/%d/%Y')
        except BaseException as e:
            logger.warning(str(e))
            date_end = None
        # date_end = None
    file_info = FileInfo(
        filename=fname,
        fileloc=os.path.join(CSV_DIR, fname),
        permno=row['permno'],
        query=row['Query'],
        keyword=row['Keyword'],
        query_date_start=date_start,
        query_date_end=date_end,
        status='working',
        last_line=count
    )
    return file_info


def create_logger(dirname, filename):
    date = time.strftime('%Y-%m-%d')
    log_filename = filename + '_' + str(date) + '.log'
    log_file = os.path.join(dirname, log_filename)
    log_file = log_file.replace('\\', '/')
    logging.config.fileConfig('log.ini', defaults={'logfilename': log_file})
    return logging.getLogger(__name__)


def to_integer(string):
    try:
        return int(string)
    except ValueError:
        return 0


def tokenize(s):
    return tokens_re.findall(s)


def preprocess(s, lowercase=False, upercase=False):
    tokens = tokenize(s)
    if lowercase:
        tokens = [token if emoticon_re.search(token) else token.lower() for token in tokens]
    if upercase:
        tokens = [token.upper() for token in tokens]
    return tokens


def process_tweet(row):
    session = Session()
    session.add(add_tweet(row))
    session.add(add_tweet_count(row))
    tokens = preprocess(row['Text'])
    cashtags = [term for term in tokens if term.startswith('$') and len(term) > 1]
    hashtags = [term for term in tokens if term.startswith('#') and len(term) > 1]
    # mentions = [term for term in tokens if term.startswith('@') and len(term) > 1]
    urls = [term for term in tokens if term.startswith('http') and len(term) > 4]
    if len(cashtags) > 0:
        for cashtag in cashtags:
            session.add(add_tweet_cashtag(row, cashtag.upper().strip()))
    if len(hashtags) > 0:
        for hashtag in hashtags:
            session.add(add_tweet_hashtag(row, hashtag.strip()))
    if len(urls) > 0:
        for url in urls:
            session.add(add_tweet_url(row, url.strip()))
    if len(row['Mentions_name'].strip()) > 0:
        mentions = row['Mentions_name'].splitlines()
        mentions_ids = row['Mentions_id'].splitlines()
        ment = zip(mentions, mentions_ids)
        for mention in ment:
            session.add(add_tweet_mention(row, mention))


def process_file(fname):
    global global_count
    file_location = os.path.join(CSV_DIR, fname)
    logger.info('Opening file %s...', fname)
    session = Session()
    session.execute('SET SESSION wait_timeout = 60;')
    db_file_info = session.query(FileInfo).filter_by(filename=fname).first()
    if db_file_info and db_file_info.status == 'finished':
        logger.info('File %s already in database', fname)
        return None

    with open(file_location, 'r', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile)
        count = 0
        if db_file_info:
            count = db_file_info.last_line
            logger.info('Continuing file %s from line %s', fname, count)
            for i in range(db_file_info.last_line):
                next(reader)
        for row in reader:
            try:
                count += 1
                global_count += 1
                # db_file_info = session.query(FileInfo).filter_by(filename=fname).first()
                if not db_file_info:
                    file_info = add_file_info(fname, row, count)
                    session.add(file_info)
                else:
                    db_file_info.last_line = count
                if global_count % 100 == 0:
                    global_td = datetime.now() - global_start
                    g_speed = global_count / global_td.total_seconds()
                    logger.info('Rows per second processed: %s', g_speed)
                # progress(fname, count, row)

                db_user = session.query(User).filter_by(user_id=row['UserID']).first()
                if not db_user:
                    session.add(add_user(row))
                    session.add(add_user_count(row))
                scinot = re.compile('[+\-]?(?:0|[1-9]\d*)(?:\.\d*)?(?:[eE][+\-]?\d+)?')
                if len(re.findall(scinot, row['TweetID'])) > 0:
                    row['TweetID'] = row['Permalink'].split('/')[-1]
                tweet_db = session.query(Tweet).filter_by(tweet_id=row['TweetID']).first()
                # permno_db = session.query(PermNo).filter_by(permno=row['permno']).first()
                if not tweet_db:
                    process_tweet(row)
                    if len(row['Mentions_name'].strip()) > 0:
                        mentions = row['Mentions_name'].splitlines()
                        mentions_ids = row['Mentions_id'].splitlines()
                        ment = zip(mentions, mentions_ids)
                        for mention in ment:
                            session.add(add_tweet_mention(row, mention))
            except BaseException as e:
                logger.error(str(e))
                raise
        # db_file_info = session.query(FileInfo).filter_by(filename=fname).first()
    if db_file_info.status == 'working':
        try:
            # db_fi = session.query(FileInfo).filter_by(id=db_file_info.id)
            # print(111)
            # print(db_fi.id)
            # print(222)
            db_file_info.counts = count
            db_file_info.status = 'finished'
            # session.add(db_file_info)
            # session.commit()
            # session.flush()
            # db_file_info.update({"counts": count, "status": "finished"})
            # session.update(db_file_info)
            # session.commit()
            print(1111)
        except BaseException as e:
            logger.error(str(e))
            raise
    # Session.remove()


if __name__ == '__main__':
    logger = create_logger(LOG_DIR, 'company_ids')
    logger.info('LOGGER STARTED')

    files = [f for f in os.listdir(CSV_DIR) if f.endswith('.csv')]
    logger.info('Found %s "csv" files in dir %s', len(files), CSV_DIR)
    global_count = 0
    global_start = datetime.now()

    process_file(files[0])
    exit()

    with cf.ThreadPoolExecutor() as executor:
        try:
            executor.map(process_file, tuple(files))
        except BaseException as e:
            logger.error(str(e))
            raise

    Session.remove()
    logger.info('ALL FILES IMPORTED')
    sys.exit()
