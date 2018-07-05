import re
import concurrent.futures as cf
import threading
# from pprint import pprint
# import time

# from sqlalchemy.expression import in_
from sqlalchemy.orm import sessionmaker, scoped_session
# from sqlalchemy.sql.expression import func

from fintweet.models import db_engine, Session, Tweet, TweetCashtags, TweetHashtags, TweetMentions, User, TweetUrl

DELETE_WITH_TAGS = False
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


def delete_tweet(t):
    global non_latin_tweets
    session = Session()
    tweet = session.query(Tweet).filter(Tweet.tweet_id == t.tweet_id).first()
    session.delete(tweet)
    session.commit()
    with threadLock:
        non_latin_tweets += 1


def process(t):
    tokens = tokenize(t.text)
    tokens_count = len(tokens)
    tags_count = get_tags_count(tokens)
    latin_count, non_latin_count = get_word_counts(tokens)
    print(latin_count, non_latin_count)
    if non_latin_count > latin_count:
        if tags_count > 0 and DELETE_WITH_TAGS:
            print('Deleting', t.text)
            print()
            # delete_tweet(t)


if __name__ == '__main__':
    session = Session()
    threadLock = threading.Lock()
    non_latin_tweets = 0
    try:
        tweets = session.query(Tweet).filter(Tweet.reply_to.in_([None, 0]))
        # tweets = session.query(Tweet).limit(15000)
        for t in tweets.execution_options(stream_results=True).yield_per(200):
            process(t)
    except Exception as e:
        print(type(e), str(e))
        raise
    print('Non latin tweets', non_latin_tweets)
    exit()

    # tweets = session.query(Tweet)
    tweets = page_query(session.query(Tweet.tweet_id, Tweet.text) \
        .filter(Tweet.tweet_id > 0) \
        .filter(Tweet.reply_to == None) \
        .order_by(Tweet.tweet_id))
    with cf.ThreadPoolExecutor(max_workers=16) as executor:
        try:
            executor.map(process, tweets)
        except Exception as e:
            print('ThreadPool', type(e), str(e))
            raise
    print('Non latin tweets', non_latin_tweets)
    print("ALL DONE.")
