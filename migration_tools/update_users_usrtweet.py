import concurrent.futures as cf
import MySQLdb
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

mydrive = "mysql://{username}:{password}@{host}:3306/mydrive55".format(**my_config)
pg_dsn = "postgresql+psycopg2://{username}@{host}:5432/{database}".format(**pg_config)

Base = declarative_base()
src = create_engine(mydrive)
dst = create_engine(pg_dsn)
pg_meta = MetaData(bind=dst, schema="fintweet")
my_meta = MetaData(bind=src)


# Reflect destination tables

class MyUsers(Base):
    __table__ = Table('user', my_meta, autoload=True)


class MyUsersCount(Base):
    __table__ = Table('user_count', my_meta, autoload=True)


class PgUsers(Base):
    __table__ = Table('user', pg_meta, autoload=True)


class PgUsersCount(Base):
    __table__ = Table('user_count', pg_meta, autoload=True)


# Create a session to use the tables
SrcSession = sessionmaker(bind=src)
session_factory = sessionmaker(dst, autocommit=True, autoflush=True)
# DstSession = scoped_session(session_factory)
DstSession = sessionmaker(bind=dst)

srcssn = SrcSession()
dstssn = DstSession()


def update_user(row):
    try:
        dstssn = DstSession()
        print("Inserting user:", row.UserScreenName)
        item = PgUsers(
            user_id=row.user_id,
            twitter_handle=row.UserScreenName,
            location=row.location,
            date_joined=row.date_joined,
            website=row.website,
            verified=row.is_verified
        )
        dstssn.add(item)
        dstssn.commit()

    except BaseException as e:
        print(str(e))
        raise


def update_user_count(row):
    try:
        dstssn = DstSession()
        print("Inserting count for user_id:", row.user_id)
        item = PgUsersCount(
            user_id=row.user_id,
            follower=row.FollowersCount,
            following=row.FollowingCount,
            tweets=row.TweetsCount,
            likes=row.LikesCount
        )
        dstssn.add(item)
        dstssn.commit()

    except BaseException as e:
        print(str(e))
        raise


if __name__ == '__main__':
    # for my_user in srcssn.query(MyUsers).execution_options(stream_results=True).yield_per(200):
    #     pg_user = dstssn.query(PgUsers).filter_by(user_id=my_user.user_id).first()
    #     uflag = False
    #     if pg_user.location is None and my_user.location:
    #         pg_user.location = my_user.location
    #         uflag = True
    #     if pg_user.date_joined is None and my_user.date_joined:
    #         pg_user.date_joined = my_user.date_joined
    #         uflag = True
    #     if pg_user.website is None and my_user.website:
    #         pg_user.website = my_user.website
    #         uflag = True
    #     if uflag:
    #         print("Updating user_id", pg_user.user_id)
    #         dstssn.commit()

    for my_user_count in srcssn.query(MyUsersCount).execution_options(stream_results=True).yield_per(200):
        pg_user_count = dstssn.query(PgUsersCount).filter_by(user_id=my_user_count.user_id).first()
        uflag = False
        if pg_user_count.follower is None or pg_user_count.follower < my_user_count.FollowersCount:
            pg_user_count.follower = my_user_count.FollowersCount
            uflag = True
        if pg_user_count.following is None or pg_user_count.following < my_user_count.FollowingCount:
            pg_user_count.following = my_user_count.FollowingCount
            uflag = True
        if pg_user_count.tweets is None or pg_user_count.tweets < my_user_count.TweetsCount:
            pg_user_count.tweets = my_user_count.TweetsCount
            uflag = True
        if pg_user_count.likes is None or pg_user_count.likes < my_user_count.LikesCount:
            pg_user_count.likes = my_user_count.LikesCount
            uflag = True
        if uflag:
            print("Updating user_id", pg_user_count.user_id)
            dstssn.commit()

    print("ALL DONE.")
