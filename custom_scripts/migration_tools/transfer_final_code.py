import concurrent.futures as cf
from sqlalchemy import *
from sqlalchemy.orm import sessionmaker, scoped_session
from sqlalchemy.ext.declarative import declarative_base
from pprint import pprint
from datetime import datetime

PG_DBNAME = ''
PG_USER = ''
PG_PASSWORD = ''
MY_DBNAME = ''
MY_USER = ''
MY_PASWORD = ''
DB_HOST = ''

my_config = {'username': MY_USER, 'password': MY_PASWORD, 'database': MY_DBNAME, 'host': DB_HOST}
pg_config = {'username': PG_USER, 'password': PG_PASSWORD, 'database': PG_DBNAME, 'host': DB_HOST}

final_code = "mysql://{username}:{password}@{host}:3306/{database}".format(**my_config)
pg_dsn = "postgresql+psycopg2://{username}@{host}:5432/{database}".format(**pg_config)

Base = declarative_base()
src = create_engine(final_code, pool_recycle=180)
dst = create_engine(pg_dsn)
pg_meta = MetaData(bind=dst, schema="fintweet")
my_meta = MetaData(bind=src)


# Reflect destination tables

class MyUsers(Base):
    __table__ = Table('user', my_meta, autoload=True)


class MyUsersCount(Base):
    __table__ = Table('user_count', my_meta, autoload=True)


class MyTweets(Base):
    __table__ = Table('tweet', my_meta, autoload=True)


class MyTweetCounts(Base):
    __table__ = Table('tweet_count', my_meta, autoload=True)


class MyTweetCashtags(Base):
    __table__ = Table('tweet_cashtags', my_meta, autoload=True)


class MyTweetHashtags(Base):
    __table__ = Table('tweet_hashtags', my_meta, autoload=True)


class MyTweetMentions(Base):
    __table__ = Table('tweet_mentions', my_meta, autoload=True)


class MyTweetUrls(Base):
    __table__ = Table('tweet_url', my_meta, autoload=True)


# class MyPermno(Base):
#     __table__ = Table('permno', my_meta, autoload=True)


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
SrcSession = sessionmaker(bind=src)
session_factory = sessionmaker(dst, autocommit=True, autoflush=True)
# DstSession = scoped_session(session_factory)
DstSession = sessionmaker(bind=dst)

srcssn = SrcSession()
dstssn = DstSession()


def transfer_user(row):
    try:
        dstssn = DstSession()
        print("Inserting user:", row.user_id)
        item = PgUsers(
            user_id=row.user_id,
            twitter_handle=row.twitter_handle,
            user_name=row.user_name,
            location=row.location,
            date_joined=row.date_joined,
            timezone=row.timezone,
            website=row.website,
            user_intro=row.user_intro,
            verified=row.verified
        )
        dstssn.add(item)
        dstssn.commit()

    except BaseException as e:
        print(str(e))
        raise


def transfer_user_count(row):
    try:
        dstssn = DstSession()
        print("Inserting count for user_id:", row.user_id)
        item = PgUsersCount(
            user_id=row.user_id,
            follower=row.follower,
            following=row.following,
            tweets=row.tweets,
            likes=row.likes
        )
        dstssn.add(item)
        dstssn.commit()

    except BaseException as e:
        print(str(e))
        raise


def transfer_tweets(row):
    try:
        dstssn = DstSession()
        print("Inserting tweet with tweet_id:", row.tweet_id)
        item = PgTweets(
            tweet_id=row.tweet_id,
            date=row.date,
            time=row.time,
            timezone=row.timezone,
            retweet_status=row.retweet_status,
            text=row.text,
            location=row.location,
            user_id=row.user_id,
            emoticon=row.emoticon
        )
        dstssn.add(item)
        dstssn.commit()

    except BaseException as e:
        print(str(e))
        raise


def transfer_tweet_counts(row):
    try:
        dstssn = DstSession()
        print("Inserting tweet count with tweet_id:", row.tweet_id)
        item = PgTweetCounts(
            tweet_id=row.tweet_id,
            reply=row.reply,
            retweet=row.retweet,
            favorite=row.favorite
        )
        dstssn.add(item)
        dstssn.commit()

    except BaseException as e:
        print(str(e))
        raise


def transfer_tweet_cashtags(row):
    try:
        dstssn = DstSession()
        print("Inserting cashtags with tweet_id:", row.tweet_id)
        item = PgTweetCashtags(
            tweet_id=row.tweet_id,
            cashtags=row.cashtags
        )
        dstssn.add(item)
        dstssn.commit()

    except BaseException as e:
        print(str(e))
        raise


def transfer_tweet_hashtags(row):
    try:
        dstssn = DstSession()
        print("Inserting hashtags with tweet_id:", row.tweet_id)
        item = PgTweetHashtags(
            tweet_id=row.tweet_id,
            hashtags=row.hashtags
        )
        dstssn.add(item)
        dstssn.commit()

    except BaseException as e:
        print(str(e))
        raise


def transfer_tweet_mentions(row):
    try:
        dstssn = DstSession()
        print("Inserting mentions with tweet_id:", row.tweet_id)
        item = PgTweetMentions(
            tweet_id=row.tweet_id,
            mentions=row.mentions,
            user_id=row.user_id
        )
        dstssn.add(item)
        dstssn.commit()

    except BaseException as e:
        print(str(e))
        raise


def transfer_tweet_urls(row):
    try:
        dstssn = DstSession()
        print("Inserting urls with tweet_id:", row.tweet_id)
        item = PgTweetUrls(
            tweet_id=row.tweet_id,
            url=row.url,
            link=row.link
        )
        dstssn.add(item)
        dstssn.commit()

    except BaseException as e:
        print(str(e))
        raise


if __name__ == '__main__':
    # for my_user in srcssn.query(MyUsers).execution_options(stream_results=True).yield_per(200):
    #     pg_user=dstssn.query(PgUsers).filter_by(user_id=my_user.user_id).first()
    #     if pg_user is None:
    #         transfer_user(my_user)
    #
    # for my_user_count in srcssn.query(MyUsersCount).execution_options(stream_results=True).yield_per(200):
    #     pg_user_count=dstssn.query(PgUsersCount).filter_by(user_id=my_user_count.user_id).first()
    #     if pg_user_count is None:
    #         transfer_user_count(my_user_count)

    # for my_tweet in srcssn.query(MyTweets).execution_options(stream_results=True).yield_per(20):
    # pg_tweet=dstssn.query(PgTweets).filter_by(tweet_id=my_tweet.tweet_id).first()
    # if pg_tweet is None:
    # transfer_tweets(my_tweet)

    # for my_cashtag in srcssn.query(MyTweetCashtags).filter(MyTweetCashtags.tweet_id>749853111913558016).order_by(MyTweetCashtags.tweet_id.asc()).execution_options(stream_results=True).yield_per(50):
    # pg_cashtag=dstssn.query(PgTweetCashtags)\
    # .filter_by(tweet_id=my_cashtag.tweet_id)\
    # .filter_by(cashtags=my_cashtag.cashtags)\
    # .first()
    # if pg_cashtag is None:
    # transfer_tweet_cashtags(my_cashtag)
    # else:
    # print('Cashtag entry', pg_cashtag.cashtags, 'for tweet_id', pg_cashtag.tweet_id, 'exsists')

    # for my_hashtag in srcssn.query(MyTweetHashtags).filter(MyTweetHashtags.tweet_id>803779963203657728).order_by(MyTweetHashtags.tweet_id.asc()).execution_options(stream_results=True).yield_per(10):
    # pg_hashtag=dstssn.query(PgTweetHashtags)\
    # .filter_by(tweet_id=my_hashtag.tweet_id)\
    # .filter_by(hashtags=my_hashtag.hashtags)\
    # .first()
    # if pg_hashtag is None:
    # transfer_tweet_hashtags(my_hashtag)
    # else:
    # print('Hashtag entry', pg_hashtag.hashtags, 'for tweet_id', pg_hashtag.tweet_id, 'exsists')

    # for my_mentions in srcssn.query(MyTweetMentions).filter(MyTweetMentions.tweet_id>629296708963319808).order_by(MyTweetMentions.tweet_id.asc()).execution_options(stream_results=True).yield_per(20):
    # pg_mentions=dstssn.query(PgTweetMentions)\
    # .filter_by(tweet_id=my_mentions.tweet_id)\
    # .filter_by(mentions=my_mentions.mentions)\
    # .first()
    # if pg_mentions is None:
    # transfer_tweet_mentions(my_mentions)
    # else:
    # print('Mentions entry', pg_mentions.mentions, 'for tweet_id', pg_mentions.tweet_id, 'exsists')

    # for my_tweet_count in srcssn.query(MyTweetCounts).filter(MyTweetCounts.tweet_id>768752069830250500).order_by(MyTweetCounts.tweet_id.asc()).execution_options(stream_results=True).yield_per(20):
    # pg_tweet_count=dstssn.query(PgTweetCounts).filter_by(tweet_id=my_tweet_count.tweet_id).first()
    # if pg_tweet_count is None:
    # transfer_tweet_counts(my_tweet_count)
    # else:
    # print('Count entry for tweet_id', pg_tweet_count.tweet_id, 'exsists')

    for my_urls in srcssn.query(MyTweetUrls).filter(MyTweetUrls.tweet_id > 649640455542452224).order_by(
            MyTweetUrls.tweet_id.asc()).execution_options(stream_results=True).yield_per(20):
        pg_urls = dstssn.query(PgTweetUrls) \
            .filter_by(tweet_id=my_urls.tweet_id) \
            .filter_by(url=my_urls.url) \
            .first()
        if pg_urls is None:
            transfer_tweet_urls(my_urls)
        else:
            print('Url entry', pg_urls.url, 'for tweet_id', pg_urls.tweet_id, 'exsists')

    # tweets = srcssn.query(MyTweets).yield_per(200).enable_eagerloads(False)
    # with cf.ThreadPoolExecutor(max_workers=4) as executor:
    #     try:
    #         executor.map(transfer_tweets, tweets)
    #     except BaseException as e:
    #         print(str(e))
    #         raise
    # del tweets
    #
    # tweet_counts = srcssn.query(MyTweetCounts).yield_per(1000).enable_eagerloads(False)
    # with cf.ThreadPoolExecutor(max_workers=4) as executor:
    #     try:
    #         executor.map(transfer_tweet_counts, tweet_counts)
    #     except BaseException as e:
    #         print(str(e))
    #         raise
    # del tweet_counts
    #
    # tweet_cashtags = srcssn.query(MyTweetCashtags).yield_per(1000).enable_eagerloads(False)
    # with cf.ThreadPoolExecutor(max_workers=4) as executor:
    #     try:
    #         executor.map(transfer_tweet_cashtags, tweet_cashtags)
    #     except BaseException as e:
    #         print(str(e))
    #         raise
    # del tweet_cashtags
    #
    # tweet_hashtags = srcssn.query(MyTweetHashtags).yield_per(1000).enable_eagerloads(False)
    # with cf.ThreadPoolExecutor(max_workers=4) as executor:
    #     try:
    #         executor.map(transfer_tweet_hashtags, tweet_hashtags)
    #     except BaseException as e:
    #         print(str(e))
    #         raise
    # del tweet_hashtags
    #
    # tweet_mentions = srcssn.query(MyTweetMentions).yield_per(1000).enable_eagerloads(False)
    # with cf.ThreadPoolExecutor(max_workers=4) as executor:
    #     try:
    #         executor.map(transfer_tweet_mentions, tweet_mentions)
    #     except BaseException as e:
    #         print(str(e))
    #         raise
    # del tweet_mentions
    #
    # tweet_urls = srcssn.query(MyTweetUrls).yield_per(1000).enable_eagerloads(False)
    # with cf.ThreadPoolExecutor(max_workers=4) as executor:
    #     try:
    #         executor.map(transfer_tweet_urls, tweet_urls)
    #     except BaseException as e:
    #         print(str(e))
    #         raise

    print("ALL DONE.")
