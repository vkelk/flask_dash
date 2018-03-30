import csv
import re
import concurrent.futures as cf
import os
import logging
import logging.config
import sys
import time
from datetime import datetime

from fintweet import TOKENS_RE
from fintweet.models import (User, UserCount, Tweet, TweetCashtags, TweetHashtags, TweetCount,
                             TweetMentions, TweetUrl, ScopedSession, FileInfo)

DIR_PATH = os.path.dirname(os.path.realpath(__file__))
LOG_DIR = os.path.join(DIR_PATH, 'logs')
CSV_DIR = os.path.join(DIR_PATH, 'files/company_ids')

if not os.path.exists(LOG_DIR):
    os.makedirs(LOG_DIR)


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


def add_file_info(fname, row=None, count=None):
    session = ScopedSession()
    file_info = FileInfo(
        filename=fname, fileloc=os.path.join(CSV_DIR, fname), status='working', last_line=count)
    try:
        session.add(file_info)
        session.commit()
    except Exception as err:
        print(type(err), err)
        raise
    return file_info


def add_user(row):
    session = ScopedSession()
    user = User(
        user_id=row['UserID'],
        twitter_handle=row['UserScreenName'],
        user_name=row['UserName'],
        verified=row['isVerified'])
    # is_company=True)
    try:
        session.add(user)
        session.commit()
    except Exception as err:
        print(type(err), err)
        raise
    return user


def add_user_count(row):
    session = ScopedSession()
    FollowersCount = to_integer(row['UserFollowersCount'])
    FollowingCount = to_integer(row['UserFollowingCount'])
    TweetsCount = to_integer(row['UserTweetsCount'])
    # LikesCount = to_integer(row['FavoriteCount'])
    user_count = UserCount(
        user_id=row['UserID'],
        follower=FollowersCount,
        following=FollowingCount,
        tweets=TweetsCount)
    # likes=LikesCount)
    try:
        session.add(user_count)
        session.commit()
    except Exception as err:
        print(type(err), err)
        raise
    return user_count


def add_tweet(row):
    session = ScopedSession()
    try:
        date = datetime.strptime(row['Date'], '%d/%m/%Y')
    except BaseException as e:
        logger.warning(str(e))
        date = None
    try:
        reply_to = int(row['ReplyTweetID'])
    except ValueError:
        reply_to = None
    # print(row['ReplyTweetID'], reply_to)
    tweet = Tweet(
        tweet_id=row['TweetID'],
        date=date,
        time=row['Time'],
        retweet_status=row['isRetweet'],
        text=row['Text'],
        user_id=row['UserID'],
        reply_to=reply_to,
        permalink=row['Permalink'])
    try:
        session.add(tweet)
        session.commit()
    except Exception as err:
        print(type(err), err)
        raise
    return tweet


def add_tweet_count(row):
    session = ScopedSession()
    Replies = to_integer(row['ReplyesCount'])
    Retweets = to_integer(row['RetweetCount'])
    Favorites = to_integer(row['FavoriteCount'])
    tweet_count = TweetCount(
        tweet_id=row['TweetID'], reply=Replies, retweet=Retweets, favorite=Favorites)
    try:
        session.add(tweet_count)
        session.commit()
    except Exception as err:
        print(type(err), err)
        raise
    return tweet_count


def add_tweet_cashtag(row, cashtag):
    session = ScopedSession()
    tweet_cashtag = TweetCashtags(tweet_id=row['TweetID'], cashtags=cashtag)
    try:
        session.add(tweet_cashtag)
        session.commit()
    except Exception as err:
        print(type(err), err)
        raise
    return tweet_cashtag


def add_tweet_hashtag(row, hashtag):
    session = ScopedSession()
    tweet_hashtag = TweetHashtags(tweet_id=row['TweetID'], hashtags=hashtag)
    try:
        session.add(tweet_hashtag)
        session.commit()
    except Exception as err:
        print(type(err), err)
        raise
    return tweet_hashtag


def add_tweet_mention(row, mention):
    session = ScopedSession()
    mentions, mentions_id = mention
    tweet_mention = TweetMentions(tweet_id=row['TweetID'], mentions=mentions, user_id=mentions_id)
    try:
        session.add(tweet_mention)
        session.commit()
    except Exception as err:
        print(type(err), err)
        raise
    return tweet_mention


def add_tweet_url(row, url):
    session = ScopedSession()
    if len(url) > 255:
        url = url[:255]
    tweet_url = TweetUrl(tweet_id=row['TweetID'], url=url)
    try:
        session.add(tweet_url)
        session.commit()
    except Exception as err:
        print(type(err), err)
        raise
    return tweet_url


def tokenize(s):
    return TOKENS_RE.findall(s)


def preprocess(s, lowercase=False, upercase=False):
    tokens = tokenize(s)
    if lowercase:
        tokens = [token if TOKENS_RE.search(token) else token.lower() for token in tokens]
    if upercase:
        tokens = [token.upper() for token in tokens]
    return tokens


def process_tweet(row):
    session = ScopedSession()
    session.add(add_tweet(row))
    session.add(add_tweet_count(row))
    tokens = preprocess(row['Text'])
    cashtags = set([term for term in tokens if term.startswith('$') and len(term) > 1])
    hashtags = set([term for term in tokens if term.startswith('#') and len(term) > 1])
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
    session = ScopedSession()
    db_file_info = session.query(FileInfo).filter_by(filename=fname).first()
    if db_file_info and db_file_info.status == 'finished':
        logger.info('File %s already in database', fname)
        return None

    with open(file_location, 'r', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile)
        count = 0
        if db_file_info:
            count = db_file_info.last_line if db_file_info.last_line else 0
            logger.info('Continuing file %s from line %s', fname, count)
            if count and count > 0:
                for i in range(db_file_info.last_line):
                    next(reader)
        else:
            db_file_info = add_file_info(fname)
        for row in reader:
            try:
                count += 1
                global_count += 1
                db_file_info.last_line = count
                if global_count % 100 == 0:
                    global_td = datetime.now() - global_start
                    g_speed = global_count / global_td.total_seconds()
                    logger.info('Rows per second processed: %s', g_speed)

                db_user = session.query(User).filter_by(user_id=row['UserID']).first()
                if not db_user:
                    session.add(add_user(row))
                    session.add(add_user_count(row))
                scinot = re.compile('[+\-]?(?:0|[1-9]\d*)(?:\.\d*)?(?:[eE][+\-]?\d+)?')
                if len(re.findall(scinot, row['TweetID'])) > 0:
                    row['TweetID'] = row['Permalink'].split('/')[-1]
                tweet_db = session.query(Tweet).filter_by(tweet_id=row['TweetID']).first()
                if not tweet_db:
                    process_tweet(row)
                    if len(row['Mentions_name'].strip()) > 0:
                        mentions = row['Mentions_name'].splitlines()
                        mentions_ids = row['Mentions_id'].splitlines()
                        ment = zip(mentions, mentions_ids)
                        for mention in ment:
                            session.add(add_tweet_mention(row, mention))
                session.add(db_file_info)
                session.commit()
            except Exception as e:
                logger.error(str(e))
                raise
    if db_file_info.status == 'working':
        try:
            db_file_info.counts = count
            db_file_info.status = 'finished'
            session.add(db_file_info)
        except BaseException as e:
            logger.error(str(e))
            raise


if __name__ == '__main__':
    logger = create_logger(LOG_DIR, 'company_ids')
    logger.info('LOGGER STARTED')

    files = [f for f in os.listdir(CSV_DIR) if f.endswith('.csv')]
    logger.info('Found %s "csv" files in dir %s', len(files), CSV_DIR)
    global_count = 0
    global_start = datetime.now()

    # for file in files:
    #     print(file)

    # process_file(files[0])
    # exit()

    with cf.ThreadPoolExecutor(max_workers=10) as executor:
        try:
            executor.map(process_file, tuple(files))
        except BaseException as e:
            logger.error(str(e))
            raise

    logger.info('ALL FILES IMPORTED')
    sys.exit()
