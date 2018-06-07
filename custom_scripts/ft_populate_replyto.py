import concurrent.futures as cf
from datetime import datetime
from pprint import pprint
from itertools import cycle
import random

from lxml import html
from sqlalchemy import or_

from fintweet import preprocess
from fintweet.models import Session, ScopedSession, Tweet, User, TweetMentions
from fintweet.scrapers import tweet_api
from fintweet.settings import proxy_list

session = ScopedSession()


def get_empty_replyto(max_workers=10):
    session = ScopedSession()
    tweets = session.query(Tweet).filter(Tweet.reply_to == None)
    limit = 12
    offset = 0
    while True:
        r = False
        for elem in tweets.limit(limit).offset(offset):
            r = True
            yield elem
        offset += limit
        if not r:
            break
    # return tweets


def cleanup_mentions(tweet):
    s = ScopedSession()
    tweet_mentions = s.query(TweetMentions).filter(TweetMentions.tweet_id == tweet.tweet_id).all()
    if len(tweet_mentions) > 0:
        for tm in tweet_mentions:
            # print(tm.id, tm.mentions)
            if tm.mentions[0] == '@':
                tm.mentions = tm.mentions[1:]
                try:
                    s.add(tm)
                    s.commit()
                    print('Updated mention', tm.mentions)
                except Exception as e:
                    print(type(e), str(e))
                    raise


def main_worker(dbt, proxy=None):
    # print(dbt.permalink, proxy)
    global global_count
    global global_time
    updated = False
    s = ScopedSession()
    page = tweet_api.Page(proxy=proxy)
    try:
        resp = page.load(dbt.permalink)
    except Exception as e:
        print(type(e), str(e))
        return None
    doc = html.fromstring(resp)
    tweets = doc.find_class('tweet')
    del page
    del resp
    if tweets and len(tweets) > 0:
        for t in tweets:
            if len(t.xpath('./@data-is-reply-to')) > 0 and t.xpath('./@data-is-reply-to')[0]:
                tweet = s.query(Tweet).filter(Tweet.tweet_id == dbt.tweet_id).first()
                tweet.reply_to = int(t.xpath('./@data-conversation-id')[0])
                if tweet.reply_to != tweet.tweet_id:
                    try:
                        s.add(tweet)
                        s.commit()
                        updated = True
                        print('Updated reply_to', tweet.tweet_id, tweet.permalink, tweet.reply_to)
                        global_count += 1
                        del doc
                        if global_count % 1000 == 0:
                            print('Average updates per sec:',
                                  global_count / ((datetime.now() - global_time).total_seconds()))
                    except Exception as e:
                        print(type(e), str(e))
                        raise
    if dbt.reply_to == 0 and not updated:
        tweet = s.query(Tweet).filter(Tweet.tweet_id == dbt.tweet_id).first()
        tweet.reply_to = None
        try:
            s.add(tweet)
            s.commit()
            global_count += 1
            print('NULLed reply_to on', tweet.tweet_id)
            if global_count % 1000 == 0:
                print('Average updates per sec:',
                      global_count / ((datetime.now() - global_time).total_seconds()))
        except Exception as e:
            print(type(e), str(e))
            raise
    cleanup_mentions(dbt)
    # exit()


if __name__ == '__main__':
    time_start = datetime.now()
    global_count = 0
    global_time = datetime.now()
    max_workers = len(proxy_list) / 4 if len(proxy_list) > 40 else 10
    # for t in get_empty_replyto(max_workers):
    #     main_worker(t, random.choice(proxy_list))
    # total_time = datetime.now() - time_start
    # print(total_time)
    # exit()

    with cf.ThreadPoolExecutor(max_workers=12) as executor:
        executor.map(main_worker, get_empty_replyto(12), cycle(proxy_list))

    total_time = datetime.now() - time_start
    print('ALL DONE in', total_time)
