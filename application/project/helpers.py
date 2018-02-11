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
        df["total during event"] = ""
        df["median during event"] = ""
        df["mean during event"] = ""
        df["total post event"] = ""
        df["median post event"] = ""
        df["mean post event"] = ""
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
    onevent_tweets = get_tweets_from_event_period(event.text, event.event_start, event.event_end)
    post_tweets = get_tweets_from_event_period(event.text, event.event_post_start, event.event_post_end)
    # objects = []
    for ev_per, tweets in {"pre_event": pre_tweets, "on_event": onevent_tweets, "post_event": post_tweets}.items():
        for t in tweets:
            event_tweet = EventTweets.query.filter(EventTweets.event_uuid == event.uuid) \
                .filter(EventTweets.tweet_id == t.tweet_id).first()
            if not event_tweet:
                # print(t.tweet_id)
                event_tweet = EventTweets(event.uuid, ev_per, t.tweet_id)
                db.session.add(event_tweet)
                db.session.commit()
    return None
