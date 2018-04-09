import json
import concurrent.futures as cf
from datetime import datetime
from pprint import pprint

from lxml import html

from fintweet import preprocess
from fintweet.models import Session, ScopedSession, Tweet, User, TweetMentions
from fintweet.scrapers import tweet_api

session = Session()


def get_empty_replyto():
    tweets = session.query(Tweet).filter(Tweet.reply_to == None) \
        .execution_options(stream_results=True).yield_per(10)
    return tweets


def main_worker(dbt, proxy=None):
    global global_count
    global global_time
    s = ScopedSession()
    page = tweet_api.Page()
    resp = page.load(dbt.permalink)
    doc = html.fromstring(resp)
    tweets = doc.find_class('tweet')
    if tweets and len(tweets) > 0:
        for t in tweets:
            if len(t.xpath('./@data-is-reply-to')) > 0 and t.xpath('./@data-is-reply-to')[0]:
                tweet = s.query(Tweet).filter(Tweet.tweet_id == dbt.tweet_id).first()
                tweet.reply_to = t.xpath('./@data-conversation-id')[0]
                try:
                    s.add(tweet)
                    s.commit()
                    print(tweet.tweet_id, tweet.permalink, tweet.reply_to)
                    global_count += 1
                    if global_count % 1000 == 0:
                        print('Average updates per sec:',
                              global_count / ((datetime.now() - global_time).total_seconds()))
                except Exception as e:
                    print(type(e), str(e))
                    raise


if __name__ == '__main__':
    time_start = datetime.now()
    global_count = 0
    global_time = datetime.now()
    for t in get_empty_replyto():
        main_worker(t)
    total_time = datetime.now() - time_start
    print(total_time)
    exit()

    with cf.ThreadPoolExecutor(max_workers=10) as executor:
        executor.map(main_worker, get_empty_replyto())

    total_time = datetime.now() - time_start
    print('ALL DONE in', total_time)
