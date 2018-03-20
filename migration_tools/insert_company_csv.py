import concurrent.futures as cf
import csv
import logging 
import logging.config
import os
import re
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
pg_dsn = "postgresql+psycopg2://{username}:{password}@{host}:5432/{database}".format(**pg_config)

Base = declarative_base()
db_engine = create_engine(pg_dsn, pool_size=100, max_overflow=0)
db_meta = MetaData(bind=db_engine, schema="fintweet")
session_factory = sessionmaker(db_engine, autocommit=True, autoflush=True)
Session = scoped_session(session_factory)


class PgUsers(Base):
    __table__ = Table('user', db_meta, autoload=True)


class PgUsersCount(Base):
    __table__ = Table('user_count', db_meta, autoload=True)


class PgTweets(Base):
    __table__ = Table('tweet', db_meta, autoload=True)


class PgTweetCounts(Base):
    __table__ = Table('tweet_count', db_meta, autoload=True)


class PgTweetCashtags(Base):
    __table__ = Table('tweet_cashtags', db_meta, autoload=True)


class PgTweetHashtags(Base):
    __table__ = Table('tweet_hashtags', db_meta, autoload=True)


class PgTweetMentions(Base):
    __table__ = Table('tweet_mentions', db_meta, autoload=True)


class PgTweetUrls(Base):
    __table__ = Table('tweet_url', db_meta, autoload=True)


def create_logger(dirname, filename):
    date = time.strftime('%Y-%m-%d')
    log_filename = filename + '_' + str(date) + '.log'
    log_file = os.path.join(dirname, log_filename)
    log_file = log_file.replace('\\', '/')
    logging.config.fileConfig('log.ini', defaults={'logfilename': log_file})
    return logging.getLogger(__name__)


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
                    session.add(add_tweet(row))
                    session.add(add_tweet_count(row))
                    if len(row['Symbols'].strip()) > 0:
                        cashtags = row['Symbols'].split('\n')
                        for cashtag in cashtags:
                            cashtag = cashtag.strip()
                            session.add(add_tweet_cashtag(row, cashtag))
                    if len(row['Hashtags'].strip()) > 0:
                        hashtags = row['Hashtags'].split(' ')
                        for hashtag in hashtags:
                            hashtag = hashtag.strip()
                            session.add(add_tweet_hashtag(row, hashtag))
                    if len(row['Mentions_name'].strip()) > 0:
                        mentions = row['Mentions_name'].splitlines()
                        mentions_ids = row['Mentions_id'].splitlines()
                        ment = zip(mentions, mentions_ids)
                        for mention in ment:
                            session.add(add_tweet_mention(row, mention))
                    if len(row['Urls'].strip()) > 0:
                        urls = row['Urls'].split(',')
                        for url in urls:
                            url = url.strip()
                            session.add(add_tweet_url(row, url))
                    # session.add(add_permno(row))
                # elif not permno_db:
                #     if len(row['CashtagSymbols'].strip()) > 0:
                #         cashtags = row['CashtagSymbols'].split('\n')
                #         for cashtag in cashtags:
                #             cashtag = cashtag.strip()
                #             session.add(add_tweet_cashtag(row, cashtag))
                #     if len(row['Hashtags'].strip()) > 0:
                #         hashtags = row['Hashtags'].split(' ')
                #         for hashtag in hashtags:
                #             hashtag = hashtag.strip()
                #             session.add(add_tweet_hashtag(row, hashtag))
                #     session.add(add_permno(row))
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