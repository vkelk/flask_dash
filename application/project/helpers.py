import os, re
import pandas as pd
from pprint import pprint
from application.project.models import *
from application.fintweet.models import *


def slugify(s):
    return re.sub('[^\w]+', '-', s).lower()


def dataframe_from_file(filename):
    ext = os.path.splitext(filename)[1]
    if ext in ['.xls', '.xlsx']:
        df = pd.read_excel(filename)
        df.columns = [slugify(col) for col in df.columns]
        df["mean pre event"] = ""
        df["median pre event"] = ""
        df["total pre event"] = ""
        df["bullish pre event"] = ""
        df["bearish pre event"] = ""
        df["positive pre event"] = ""
        df["negative pre event"] = ""
        # df["sentiment pre event"] = ""
        # df["std pre event"] = ""
        df["mean during event"] = ""
        df["median during event"] = ""
        df["total during event"] = ""
        df["bullish during event"] = ""
        df["bearish during event"] = ""
        df["positive during event"] = ""
        df["negative during event"] = ""
        # df["sentiment during event"] = ""
        # df["std during event"] = ""
        df["mean post event"] = ""
        df["median post event"] = ""
        df["total post event"] = ""
        df["bullish post event"] = ""
        df["bearish post event"] = ""
        df["positive post event"] = ""
        df["negative post event"] = ""
        # df["sentiment post event"] = ""
        # df["std post event"] = ""
        # df["pct change"] = ""
        df["users pre event"] = ""
        df["users during event"] = ""
        df["users post event"] = ""
        df["bullish users pre event"] = ""
        df["bearish users pre event"] = ""
        df["bullish users during event"] = ""
        df["bearish users during event"] = ""
        df["bullish users post event"] = ""
        df["bearish users post event"] = ""
        return df
    # TODO: Create import from CSV
    return None


def get_data_from_query(c_tag, estimation):
    q = db.session \
        .query(Tweet.date, db.func.count(Tweet.tweet_id).label("count")) \
        .join(TweetCashtag) \
        .filter(TweetCashtag.cashtags == c_tag) \
        .filter(Tweet.date >= estimation['pre_start']) \
        .filter(Tweet.date <= estimation['post_end']) \
        .group_by(Tweet.date).order_by(Tweet.date)
    # print(q)
    return [r._asdict() for r in q.all()]


def get_tweets_from_event_period(c_tag, period_start, period_end):
    q = db.session \
        .query(Tweet.tweet_id) \
        .join(TweetCashtag) \
        .filter(TweetCashtag.cashtags == c_tag) \
        .filter(Tweet.date >= period_start) \
        .filter(Tweet.date <= period_end) \
        .order_by(Tweet.date)
    # print(q)
    return q.all()


def insert_event_tweets(event):
    for t in db.session.query(Tweet.tweet_id).join(TweetCashtag) \
            .filter(TweetCashtag.cashtags == event.text) \
            .filter(Tweet.date >= event.event_pre_start) \
            .filter(Tweet.date <= event.event_pre_end) \
            .order_by(Tweet.date).yield_per(100):
        event_tweet = EventTweets.query.filter(EventTweets.event_uuid == event.uuid) \
            .filter(EventTweets.tweet_id == t.tweet_id).first()
        if not event_tweet:
            event_tweet = EventTweets(event.uuid, "pre_event", t.tweet_id)
            db.session.add(event_tweet)
            db.session.commit()

    for t in db.session.query(Tweet.tweet_id).join(TweetCashtag) \
            .filter(TweetCashtag.cashtags == event.text) \
            .filter(Tweet.date >= event.event_start) \
            .filter(Tweet.date <= event.event_end) \
            .order_by(Tweet.date).yield_per(100):
        event_tweet = EventTweets.query.filter(EventTweets.event_uuid == event.uuid) \
            .filter(EventTweets.tweet_id == t.tweet_id).first()
        if not event_tweet:
            event_tweet = EventTweets(event.uuid, "on_event", t.tweet_id)
            db.session.add(event_tweet)
            db.session.commit()

    for t in db.session.query(Tweet.tweet_id).join(TweetCashtag) \
            .filter(TweetCashtag.cashtags == event.text) \
            .filter(Tweet.date >= event.event_post_start) \
            .filter(Tweet.date <= event.event_post_end) \
            .order_by(Tweet.date).yield_per(100):
        event_tweet = EventTweets.query.filter(EventTweets.event_uuid == event.uuid) \
            .filter(EventTweets.tweet_id == t.tweet_id).first()
        if not event_tweet:
            event_tweet = EventTweets(event.uuid, "post_event", t.tweet_id)
            db.session.add(event_tweet)
            db.session.commit()

    return None


def count_sentiment(cashtag, start, end):
    filters = {"cashtag": cashtag, "start": start, "end": end}
    q = "select sum(case when s.sentiment = 'Bullish' then 1 else 0 end) as bullish, sum(case when s.sentiment = 'Bearish' then 1 else 0 end) as bearish, " \
        "sum (case when s.tone = 'positive' then 1 else 0 end) as positive, sum (case when s.tone = 'negative' then 1 else 0 end) as negative " \
        "from fintweet.tweet_cashtags c join fintweet.tweet t on t.tweet_id = c.tweet_id join fintweet.tweet_sentiment s on c.tweet_id = s.tweet_id " \
        "where c.cashtags='{cashtag}' and t.date >='{start}' and t.date <='{end}'".format(**filters)
    result = db.session.execute(q).fetchone()
    # print(result.keys(), result)
    return result


def count_users_sentimet(cashtag, start, end):
    filters = {"cashtag": cashtag, "start": start, "end": end}
    q = "SELECT COUNT(distinct t.user_id) as users, " \
        "COUNT(distinct t.user_id) filter (WHERE s.sentiment = 'Bullish') as bullish, " \
        "COUNT(distinct t.user_id) filter (WHERE s.sentiment = 'Bearish') as bearish " \
        "FROM fintweet.tweet_cashtags c " \
        "JOIN fintweet.tweet t ON t.tweet_id = c.tweet_id " \
        "JOIN fintweet.tweet_sentiment s ON c.tweet_id = s.tweet_id " \
        "WHERE c.cashtags='{cashtag}' AND t.date >='{start}' AND t.date <='{end}'".format(**filters)
    result = db.session.execute(q).fetchone()
    # print(result.keys(), result)
    return result
