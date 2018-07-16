import re
import concurrent.futures as cf
import threading
# from pprint import pprint
# import time

# from sqlalchemy.expression import in_
from sqlalchemy.orm import sessionmaker, scoped_session
# from sqlalchemy.sql.expression import func

from fintweet.models import db_engine, Session, Tweet, TweetCashtags, TweetHashtags, TweetMentions, User, TweetUrl, TweetDeleted

DELETE_WITH_TAGS = True
regex_str = [
    r'(?:@[\w_]+)',  # @-mentions
    r"(?:\#[a-zA-Z0-9]\w+)",  # hash-tags
    r"(?:\B\$[A-Za-z][A-Za-z0-9]{0,4}\b)",  # cash-tags
    r'(?:(?:(?:http[s?]?:\/\/)?(?:pic\.twitter\.com\/\w+)))',  # pics
    r'(?:http[s]?:\/\/(?:\w+|[$-_@.&amp;+]|[!*\(\),]|(?:%[0-9a-f][0-9a-f]))+)',  # URLs
    r'(?:(?:\d+,?)+(?:\.?\d+)?)',  # numbers
    r"(?:\b[\w'-_]+\b)",  # other words
    # r'(?:\S)'  # anything else
]
tokens_re = re.compile(r'(' + '|'.join(regex_str) + ')', re.VERBOSE | re.IGNORECASE)


def tokenize(s):
    return tokens_re.findall(s)


def page_query(q):
    offset = 0
    while True:
        r = False
        for elem in q.limit(1000).offset(offset):
            r = True
            yield elem
        offset += 1000
        print(offset)
        if not r:
            break


def get_word_counts(tokens):
    non_latin_count = 0
    latin_count = 0
    for term in set(tokens):
        # print(term)
        if term.startswith('$') or term.startswith('#') or term.startswith('@') or term.startswith('http'):
            tokens.remove(term)
            continue
        if term.isdigit():
            tokens.remove(term)
            continue
        try:
            term_utf = term.encode(encoding='utf-8')
            term_ascii = term_utf.decode('ascii')
            # term.encode(encoding='utf-8').decode('ascii')
            latin_count += 1
        except UnicodeDecodeError:
            non_latin_count += 1
            tokens.remove(term)
        except Exception as e:
            print(type(e), str(e))
            print('term', term)
            raise
    return latin_count, non_latin_count


def get_tags_count(tokens):
    tags_count = 0
    for term in set(tokens):
        if term.startswith('$') or term.startswith('#') or term.startswith('@') or term.startswith('http'):
            tokens.remove(term)
            tags_count += 1
    return tags_count


def remove_tweet(t):
    global non_latin_tweets
    try:
        session = Session()
        tweet = session.query(Tweet).filter(Tweet.tweet_id == t.tweet_id).first()
        deleted = TweetDeleted(
            tweet_id=t.tweet_id,
            date=t.date,
            time=t.time,
            timezone=t.timezone,
            retweet_status=t.retweet_status,
            text=t.text,
            location=t.location,
            user_id=t.user_id,
            emoticon=t.emoticon,
            reply_to=t.reply_to,
            permalink=t.permalink
            )
        session.add(deleted)
        session.delete(tweet)
        session.commit()
        with threadLock:
            non_latin_tweets += 1
        print('Deleted', t.tweet_id, t.text)
    except Exception as e:
        print('Could not remove', t.tweet_id)
        print(type(e), str(e))
        raise
    finally:
        session.close()



def phase_one(t):   
    tokens = tokenize(t.text)
    tokens_count = len(tokens)
    tags_count = get_tags_count(tokens)
    latin_count, non_latin_count = get_word_counts(tokens)
    # print(latin_count, non_latin_count)
    # exit()
    if non_latin_count > latin_count:
        if tags_count > 0 and DELETE_WITH_TAGS:
            # print('Will delete', t.text)
            # print()
            remove_tweet(t)
    return [latin_count, non_latin_count]


if __name__ == '__main__':
    session = Session()
    threadLock = threading.Lock()
    non_latin_tweets = 0
    try:
        print('Will remove non-latin tweets that have more non-latin words in the text than latin words')
        tweets = session.query(Tweet) \
            .outerjoin(TweetCashtags) \
            .filter(TweetCashtags.tweet_id.is_(None)) \
            .filter(Tweet.date >= '2012-01-01') \
            .filter(Tweet.date <= '2016-12-31') \
            .filter(Tweet.text.op('~')('[^[:ascii:]]'))
        # tweets = session.query(Tweet).filter(Tweet.text.op('~')('[^\x00-\x7F]+'))
        # tweets = session.query(Tweet).limit(15000)
        for i, t in enumerate(tweets.execution_options(stream_results=True).yield_per(200)):
            counts = phase_one(t)
            # print(t.text)
            # if i % 10000 == 0:
                # print(i, t.tweet_id, t.text, t.user_id, counts)
                # print(t.text)
                # exit()
    except Exception as e:
        print(type(e), str(e))
        raise
    print('Deleted Non latin tweets', non_latin_tweets)
    exit()

    # tweets = session.query(Tweet)
    print('Will delete non-latin tweets that are not replys and have more non-latin words than latin')
    tweets = page_query(session.query(Tweet.tweet_id, Tweet.text) \
        .filter(Tweet.tweet_id > 0) \
        .filter(Tweet.reply_to == None) \
        .order_by(Tweet.tweet_id))
    with cf.ThreadPoolExecutor(max_workers=16) as executor:
        try:
            executor.map(phase_one, tweets)
        except Exception as e:
            print('ThreadPool', type(e), str(e))
            raise
    print('Non latin tweets', non_latin_tweets)
    print("ALL DONE.")
