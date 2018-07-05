import re
import concurrent.futures as cf
import sys
# from pprint import pprint
# import time

from sqlalchemy import or_
from sqlalchemy.orm import sessionmaker, scoped_session
# from sqlalchemy.sql.expression import func

from fintweet.models import db_engine, Session, Tweet, TweetCashtags, TweetHashtags, TweetMentions, User, TweetUrl

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


def is_english(text):
    tokens = tokenize(text)
    # print(text)
    non_latin_count = 0
    latin_count = 0
    total_count = 0
    for term in set(tokens):
        # print(term)
        if term.startswith('$') or term.startswith('#') or term.startswith('@') or term.startswith('http') \
            or term.isdigit():
            tokens.remove(term)
            total_count += 1
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
        finally:
            total_count += 1
    words_count = latin_count + non_latin_count
    
    # print(word_population)
    # if word_population < 0.5:
        # print(text)
        # print(tokens)
    if words_count > 0:
        if latin_count/words_count <= 0.8:
            print(text)
            print(tokens)
            print('Latin population', latin_count / words_count)
            print('Word population', words_count / total_count)
            # exit()
        # print(englislatin/total_count*100)



if __name__ == '__main__':
    session = Session()
    try:
        # tweets = session.query(Tweet)
        tweets = session.query(Tweet).limit(15000)
        for t in tweets.execution_options(stream_results=True).yield_per(200):
            english = is_english(t.text)
            
    except Exception as e:
        print(type(e), str(e))
        raise
    exit()

    tweets = page_query(session.query(Tweet.tweet_id, Tweet.text) \
        .filter(Tweet.tweet_id > 0).order_by(Tweet.tweet_id))
    with cf.ThreadPoolExecutor(max_workers=16) as executor:
        try:
            executor.map(extract, tweets)
        except Exception as e:
            print('ThreadPool', type(e), str(e))
            raise

    print("ALL DONE.")
