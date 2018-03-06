from sqlalchemy import *
from sqlalchemy.orm import sessionmaker, scoped_session
from sqlalchemy.ext.declarative import declarative_base

pg_dsn = "postgresql+psycopg2://postgres:Ec3dt8Xiw3IjJ9tIYFIz@localhost:5432/tweets"
Base = declarative_base()
db = create_engine(pg_dsn, pool_size=100, max_overflow=0)
pg_meta = MetaData(bind=db, schema="fintweet")
Session = sessionmaker(bind=db)


class Tweets(Base):
    __table__ = Table('tweet', pg_meta, autoload=True)


class TweetSentiment(Base):
    __table__ = Table('tweet_sentiment', pg_meta, autoload=True)


session = Session()
tweets = {}  # EMPTY DICTIONARY

for t in session.query(Tweets).order_by(Tweets.tweet_id.asc()).yield_per(100):
    s = session.query(TweetSentiment).filter(TweetSentiment.tweet_id == t.tweet_id).one()
    if not s:
        s = TweetSentiment()
    print()

    extract(t)


con = p.connect("dbname='tweets' user='postgres' password='Ec3dt8Xiw3IjJ9tIYFIz' host='localhost'")
cur = con.cursor()
cur.execute(
    "select tweet_id, text from fintweet.tweet limit 100000 offset " + str(processed - 100000))  # query executed
rows = cur.fetchall()  # results stored in rows
i = 0;
for entry in rows:  # iterating selected data to set tweet_id corresponing to sentiment
    tweets[entry[0]] = svm_predicted[i];
    i = i + 1;
for key in tweets:  # inserting tweet_id and its coressponding sentiment

    # query = "INSERT INTO fintweet.tweet_sentiment(tweet_id,sentiment) VALUES(" + str(key) + " , '" + tweets[key]+"')"
    query = "UPDATE fintweet.tweet_sentiment SET sentiment = '" + tweets[key] + "' WHERE tweet_id = " + str(key) + ";"
    # print(query)
    cur.execute(query);  # query executed but in "INSERT" case we have to comit changes to let it update the darabase

con.commit()  # commiting changes to database.