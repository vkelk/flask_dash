import concurrent.futures as cf
import MySQLdb
import json, re
from sqlalchemy import *
from sqlalchemy.orm import sessionmaker, scoped_session
from sqlalchemy.ext.declarative import declarative_base
from pprint import pprint
from datetime import datetime

PG_DBNAME = ''
PG_USER = ''
PG_PASSWORD = ''
MY_DBNAME = 'stocktwits'
MY_USER = ''
MY_PASWORD = ''
DB_HOST = ''

my_config = {'username': MY_USER, 'password': MY_PASWORD, 'database': MY_DBNAME, 'host': DB_HOST}
pg_config = {'username': PG_USER, 'password': PG_PASSWORD, 'database': PG_DBNAME, 'host': DB_HOST}

stocktwits = "mysql://{username}:{password}@{host}:3306/{database}".format(**my_config)
pg_dsn = "postgresql+psycopg2://{username}@{host}:5432/{database}".format(**pg_config)

Base = declarative_base()
src = create_engine(stocktwits, pool_recycle=180, pool_size=100)
dst = create_engine(pg_dsn, pool_recycle=180, pool_size=100)
pg_meta = MetaData(bind=dst, schema="stocktwits")
my_meta = MetaData(bind=src)


# Reflect source tables
class MyUsers(Base):
    __table__ = Table('user', my_meta, autoload=True)


class MyUsersCount(Base):
    __table__ = Table('user_count', my_meta, autoload=True)


class MyIdeas(Base):
    __table__ = Table('ideas', my_meta, autoload=True)


class MyIdeasCounts(Base):
    __table__ = Table('ideas_count', my_meta, autoload=True)


class MyIdesUrls(Base):
    __table__ = Table('ideas_url', my_meta, autoload=True)


class MyReply(Base):
    __table__ = Table('reply', my_meta, autoload=True)


# Destination PostgreSQL defintions
class PgUsers(Base):
    __table__ = Table('user', pg_meta, autoload=True)


class PgUsersCount(Base):
    __table__ = Table('user_count', pg_meta, autoload=True)


class PgUserStrategy(Base):
    __table__ = Table('user_strategy', pg_meta, autoload=True)


class PgIdeas(Base):
    __table__ = Table('ideas', pg_meta, autoload=True)


class PgIdeasCounts(Base):
    __table__ = Table('ideas_count', pg_meta, autoload=True)


class PgIdeasCashtags(Base):
    __table__ = Table('idea_cashtags', pg_meta, autoload=True)


class PgIdeasHashtags(Base):
    __table__ = Table('idea_hashtags', pg_meta, autoload=True)


class PgIdeasUrls(Base):
    __table__ = Table('ideas_url', pg_meta, autoload=True)


class PgReply(Base):
    __table__ = Table('reply', pg_meta, autoload=True)


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
        new_user = PgUsers(
            user_id=row.user_id,
            user_handle=row.user_handle,
            user_name=row.user_name,
            date_joined=row.date_joined,
            website=row.website,
            source=row.source,
            user_topmentioned=row.user_topmentioned,
            location=row.location,
            verified=row.verified
        )
        # pprint(new_user)
        dstssn.add(new_user)
        dstssn.commit()
        if len(row.user_strategy) < 200:
            usr_st_dict = json.loads(row.user_strategy)
            assets_frequently_traded = json.dumps(usr_st_dict['assets_frequently_traded'])
            new_strategy = PgUserStrategy(
                user_id=row.user_id,
                assets_frequently_traded=assets_frequently_traded,
                approach=usr_st_dict['approach'],
                holding_period=usr_st_dict['holding_period'],
                experience=usr_st_dict['experience']
            )
            # pprint(new_strategy)
            dstssn.add(new_strategy)
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
            followers=row.followers,
            following=row.following,
            watchlist_count=row.watchlist_count,
            watchlist_stocks=row.watchlist_stocks,
            ideas=row.ideas
        )
        dstssn.add(item)
        dstssn.commit()

    except BaseException as e:
        print(str(e))
        raise


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


def process_cashtag(ideas_id, cashtag):
    subsession = DstSession()
    pg_cashtag = subsession.query(PgIdeasCashtags).filter_by(ideas_id=ideas_id).filter_by(cashtag=cashtag).first()
    if pg_cashtag is None:
        try:
            item = PgIdeasCashtags(
                ideas_id=ideas_id,
                cashtag=cashtag
            )
            subsession.add(item)
            subsession.commit()
            print(ideas_id, "-> Inserted cashtag", cashtag)
        except BaseException as e:
            print(str(e))
            raise
        finally:
            subsession.close()


def process_hashtag(ideas_id, hashtag):
    subsession = DstSession()
    pg_hashtag = subsession.query(PgIdeasHashtags).filter_by(ideas_id=ideas_id).filter_by(hashtag=hashtag).first()
    if pg_hashtag is None:
        try:
            item = PgIdeasHashtags(
                ideas_id=ideas_id,
                hashtag=hashtag
            )
            subsession.add(item)
            subsession.commit()
            print(ideas_id, "-> Inserted hashtag", hashtag)
        except BaseException as e:
            print(str(e))
            raise
        finally:
            subsession.close()


def process_url(ideas_id, url):
    subsession = DstSession()
    pg_url = subsession.query(PgIdeasUrls).filter_by(ideas_id=ideas_id).filter_by(url=url).first()
    if pg_url is None:
        try:
            item = PgIdeasUrls(
                ideas_id=ideas_id,
                url=url
            )
            subsession.add(item)
            subsession.commit()
            print(ideas_id, "-> Inserted url", url)
        except BaseException as e:
            print(str(e))
            raise
        finally:
            subsession.close()


def transfer_ideas(row):
    try:
        dstssn = DstSession()
        print("Inserting idea with idea_id:", row.ideas_id)
        idea = PgIdeas(
            ideas_id=row.ideas_id,
            user_id=row.user_id,
            permno=row.permno,
            date=row.date,
            time=row.time,
            replied=row.replied,
            text=row.text,
            sentiment=row.sentiment,
            cashtags_other=row.cashtags_other
        )
        dstssn.add(idea)
        dstssn.commit()
        # pprint(idea.text)

        tokens = preprocess(idea.text)
        cashtags = [term for term in tokens if term.startswith('$') and len(term) > 1]
        hashtags = [term for term in tokens if term.startswith('#') and len(term) > 1]
        urls = [term for term in tokens if term.startswith('http') and len(term) > 4]
        if len(cashtags) > 0:
            for cashtag in cashtags:
                process_cashtag(idea.ideas_id, cashtag)
            for hashtag in hashtags:
                process_hashtag(idea.ideas_id, hashtag)
            for url in urls:
                process_url(idea.ideas_id, url)

    except BaseException as e:
        print(str(e))
        raise
    finally:
        dstssn.close()


def transfer_idea_counts(row):
    try:
        dstssn = DstSession()
        print("Inserting idea count with ideas_is:", row.ideas_is)
        item = PgIdeasCounts(
            ideas_is=row.ideas_is,
            replies=row.replies,
            likes=row.likes
        )
        dstssn.add(item)
        dstssn.commit()

    except BaseException as e:
        print(str(e))
        raise
    finally:
        dstssn.close()


def transfer_replys(row):
    try:
        dstssn = DstSession()
        print("Inserting replys with replay_id:", row.replay_id)
        item = PgReply(
            replay_id=row.replay_id,
            ideas_id=row.ideas_id,
            date=row.date,
            time=row.time,
            reply_userid=row.reply_userid,
            text=row.text
        )
        dstssn.add(item)
        dstssn.commit()

    except BaseException as e:
        print(str(e))
        raise
    finally:
        dstssn.close()


if __name__ == '__main__':
    i = 0
    # for my_user in srcssn.query(MyUsers).filter(MyUsers.user_id >= 508128)\
    #         .order_by(MyUsers.user_id.asc()).execution_options(stream_results=True).yield_per(100):
    #     pg_user = dstssn.query(PgUsers).filter_by(user_id=my_user.user_id).first()
    #     if pg_user is None:
    #         transfer_user(my_user)
    #
    # for my_ucount in srcssn.query(MyUsersCount).filter(MyUsersCount.user_id >= 508128)\
    #         .order_by(MyUsersCount.user_id.asc()).execution_options(stream_results=True).yield_per(100):
    #     pg_user_count = dstssn.query(PgUsersCount).filter_by(user_id=my_ucount.user_id).first()
    #     if pg_user_count is None:
    #         transfer_user_count(my_ucount)

    for my_idea in srcssn.query(MyIdeas).filter(MyIdeas.ideas_id >= 6244221) \
            .order_by(MyIdeas.ideas_id.asc()).execution_options(stream_results=True).yield_per(20):
        pg_idea = dstssn.query(PgIdeas).filter_by(ideas_id=my_idea.ideas_id).first()
        if pg_idea is None:
            transfer_ideas(my_idea)

    for my_idea_count in srcssn.query(MyIdeasCounts) \
            .order_by(MyIdeasCounts.ideas_id.asc()).execution_options(stream_results=True).yield_per(20):
        pg_idea_count = dstssn.query(PgIdeasCounts).filter_by(ideas_id=my_idea_count.ideas_id).first()
        if pg_idea_count is None:
            transfer_idea_counts(my_idea_count)

    for my_reply in srcssn.query(MyReply) \
            .order_by(MyReply.replay_id.asc()).execution_options(stream_results=True).yield_per(20):
        pg_reply = dstssn.query(PgReply).filter_by(replay_id=my_reply.replay_id).first()
        if pg_reply is None:
            transfer_replys(my_reply)

    print("ALL DONE.")
