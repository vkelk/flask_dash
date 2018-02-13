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
        df["total pre event"] = ""
        df["median pre event"] = ""
        df["mean pre event"] = ""
        df["std pre event"] = ""
        df["bullish pre event"] = ""
        df["bearish pre event"] = ""
        df["sentiment pre event"] = ""
        df["total during event"] = ""
        df["median during event"] = ""
        df["mean during event"] = ""
        df["std during event"] = ""
        df["bullish during event"] = ""
        df["bearish during event"] = ""
        df["sentiment during event"] = ""
        df["total post event"] = ""
        df["median post event"] = ""
        df["mean post event"] = ""
        df["std post event"] = ""
        df["bullish post event"] = ""
        df["bearish post event"] = ""
        df["sentiment post event"] = ""
        df["pct change"] = ""
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
    pre_tweets = get_tweets_from_event_period(event.text, event.event_pre_start, event.event_pre_end)
    for t in pre_tweets:
        event_tweet = EventTweets.query.filter(EventTweets.event_uuid == event.uuid) \
            .filter(EventTweets.tweet_id == t.tweet_id).first()
        if not event_tweet:
            event_tweet = EventTweets(event.uuid, "pre_event", t.tweet_id)
            db.session.add(event_tweet)
            db.session.commit()
    pre_tweets = None
    onevent_tweets = get_tweets_from_event_period(event.text, event.event_start, event.event_end)
    for t in onevent_tweets:
        event_tweet = EventTweets.query.filter(EventTweets.event_uuid == event.uuid) \
            .filter(EventTweets.tweet_id == t.tweet_id).first()
        if not event_tweet:
            event_tweet = EventTweets(event.uuid, "on_event", t.tweet_id)
            db.session.add(event_tweet)
            db.session.commit()
    onevent_tweets = None
    post_tweets = get_tweets_from_event_period(event.text, event.event_post_start, event.event_post_end)
    for t in post_tweets:
        event_tweet = EventTweets.query.filter(EventTweets.event_uuid == event.uuid) \
            .filter(EventTweets.tweet_id == t.tweet_id).first()
        if not event_tweet:
            event_tweet = EventTweets(event.uuid, "on_event", t.tweet_id)
            db.session.add(event_tweet)
            db.session.commit()
    return None
