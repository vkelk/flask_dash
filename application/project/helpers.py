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

    # pre_tweets = get_tweets_from_event_period(event.text, event.event_pre_start, event.event_pre_end)
    # for t in pre_tweets:
    #     event_tweet = EventTweets.query.filter(EventTweets.event_uuid == event.uuid) \
    #         .filter(EventTweets.tweet_id == t.tweet_id).first()
    #     if not event_tweet:
    #         event_tweet = EventTweets(event.uuid, "pre_event", t.tweet_id)
    #         db.session.add(event_tweet)
    #         db.session.commit()
    # pre_tweets = None

    return None


def count_sentiment(event):
    '''
    select sum(case when s.sentiment = 'Bullish' then 1 else 0 end) as bulls, sum(case when s.sentiment = 'Bearish' then 1 else 0 end) as bears from fintweet.tweet_cashtags c join fintweet.tweet t on t.tweet_id = c.tweet_id join fintweet.tweet_sentiment s on c.tweet_id = s.tweet_id where c.cashtags='$SAIC' and t.date >='2015-03-01' and t.date <='2015-03-04'  limit 1;
    :param event:
    :return:
    '''
