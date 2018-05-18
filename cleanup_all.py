import re
import concurrent.futures as cf
from pprint import pprint
# import time

# from sqlalchemy import Table
# from sqlalchemy.sql.expression import func

from fintweet.models import Session, ScopedSession, Tweet

regex_str = [
    # emoticons_str,
    r'<[^>]+>',  # HTML tags
    r'(?:@[\w_]+)',  # @-mentions
    r"(?:\#\w*[a-zA-Z]+\w*)",  # hash-tags
    r"(?:\$[A-Za-z][A-Za-z0-9]{1,4}\b)",  # cash-tags
    r'(?:(?:(?:http[s?]?:\/\/)?(?:pic\.twitter\.com\/\w+)))',  # pics
    r'(?:http[s]?:\/\/(?:\w+|[$-_@.&amp;+]|[!*\(\),]|(?:%[0-9a-f][0-9a-f]))+)',  # URLs
    # r'(?:(?:\d+,?)+(?:\.?\d+)?)',  # numbers
    # r"(?:[a-z][a-z'\-_]+[a-z])",  # words with - and '
    # r'(?:[\w_]+)',  # other words
    # r'(?:\S)'  # anything else
]
tokens_re = re.compile(r'(' + '|'.join(regex_str) + ')', re.VERBOSE | re.IGNORECASE)


def tokenize(s):
    return tokens_re.findall(s)


def preprocess(s, lowercase=False, upercase=False):
    tokens = tokenize(s)
    if upercase:
        tokens = [token.upper() for token in tokens]
    return tokens


def extract(tweet):
    global i
    tokens = preprocess(tweet.text)
    return tokens
    exit()
    # cashtags = set([term for term in tokens if term.startswith('$') and len(term) > 1])
    # hashtags = set([term for term in tokens if term.startswith('#') and len(term) > 1])
    # mentions = [term for term in tokens if term.startswith('@') and len(term) > 1]
    # urls = [term for term in tokens if term.startswith('http') and len(term) > 4]
    for hashtag in hashtags:
        if hashtag and len(hashtag) > 1 and len(hashtag) <= 120:
            process_hashtag(tweet.tweet_id, hashtag)
        # for hashtag in hashtags:
        #     process_hashtag(tweet.tweet_id, hashtag)
        # for mention in mentions:
        #     process_mention(tweet.tweet_id, mention)
        # for url in urls:
        #     process_url(tweet.tweet_id, url)


def process_cashtag(tweet_id, cashtag):
    subsession = Session()
    pg_cashtag = subsession.query(PgTweetCashtags).filter_by(tweet_id=tweet_id).filter_by(
        cashtags=cashtag).first()
    if pg_cashtag is None:
        try:
            item = PgTweetCashtags(tweet_id=tweet_id, cashtags=cashtag)
            subsession.add(item)
            subsession.commit()
            print(tweet_id, "-> Inserted cashtag", cashtag)
        except BaseException as e:
            print(str(e))
            raise


def process_hashtag(tweet_id, hashtag):
    subsession = ScopedSession()
    pg_hashtag = subsession.query(TweetHashtagsNew).filter_by(tweet_id=tweet_id).filter_by(
        hashtags=hashtag).first()
    if pg_hashtag is None:
        try:
            item = TweetHashtagsNew(tweet_id=tweet_id, hashtags=hashtag)
            subsession.add(item)
            subsession.commit()
            print(tweet_id, "-> Inserted cashtag", hashtag)
        except Exception as e:
            print(type(e), str(e))
            raise


def process_mention(tweet_id, mention):
    subsession = Session()
    pg_mention = subsession.query(PgTweetMentions).filter_by(tweet_id=tweet_id).filter_by(
        mentions=mention).first()
    if pg_mention is None:
        try:
            item = PgTweetMentions(tweet_id=tweet_id, mentions=mention)
            subsession.add(item)
            subsession.commit()
            print(tweet_id, "-> Inserted mention", mention)
        except BaseException as e:
            print(str(e))
            raise


def process_url(tweet_id, url):
    subsession = Session()
    pg_url = subsession.query(PgTweetUrls).filter_by(tweet_id=tweet_id).filter_by(url=url).first()
    if pg_url is None:
        try:
            item = PgTweetUrls(tweet_id=tweet_id, url=url)
            subsession.add(item)
            subsession.commit()
            print(tweet_id, "-> Inserted url", url)
        except BaseException as e:
            print(str(e))
            raise


if __name__ == '__main__':
    try:
        session = Session()
        tweets = session.query(Tweet).filter(Tweet.text.op('~')('[#$@]\s\w+')).limit(5)
        # tweets = session.query(Tweet).limit(5)
        for t in tweets.yield_per(100):
            print(t.text, t.permalink)
            print(extract(t))
            t.text = re.sub(r'\$ (?P<s>[A-Za-z][A-Za-z0-9]{0,4}\b)', '$\g<s>', t.text)
            t.text = re.sub(r'\# (?P<s>\w*[a-zA-Z]+\w*)', '#\g<s>', t.text)
            t.text = re.sub(r'\@ (?P<s>[\w_]+)', '@\g<s>', t.text)
            try:
                s = ScopedSession()
                t2 = s.query(Tweet).filter(Tweet.tweet_id == t.tweet_id).first()
                s.add(t2)
                s.commit()
            except Exception as e:
                print(type(e), str(e))
                raise
            print(extract(t))
    except Exception as e:
        print(type(e), str(e))
        raise
    exit()

    
    tweets = session.query(Tweet).filter(Tweet.tweet_id > 1) \
        .order_by(Tweet.tweet_id.asc()).yield_per(100)
    with cf.ProcessPoolExecutor(max_workers=4) as executor:
        try:
            executor.map(extract, tweets)
        except Exception as e:
            print(type(e), str(e))
            raise

    print("ALL DONE.")
