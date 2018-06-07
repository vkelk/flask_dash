import re
import concurrent.futures as cf
# from pprint import pprint
# import time

# from sqlalchemy import Table
# from sqlalchemy.sql.expression import func

from fintweet.models import Session, ScopedSession, Tweet, TweetCashtags, TweetHashtags, TweetMentions, User, TweetUrl

regex_str = [
    # emoticons_str,
    # r'<[^>]+>',  # HTML tags
    r'(?:@[\w_]+)',  # @-mentions
    r"(?:\#\w+)",  # hash-tags
    r"(?:\B\$[A-Za-z][A-Za-z0-9]{0,4}\b)",  # cash-tags
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


def extract(tweet_id):
    subsession = ScopedSession()
    tweet = subsession.query(Tweet).filter_by(tweet_id=tweet_id).first()
    tokens = preprocess(tweet.text)
    cashtags = set([term.upper() for term in tokens if term.startswith('$') and len(term) > 1])
    hashtags = set([term.upper() for term in tokens if term.startswith('#') and len(term) > 1])
    mentions = set([term for term in tokens if term.startswith('@') and len(term) > 1])
    urls = set([term for term in tokens if term.startswith('http') and len(term) > 4])
    for cashtag in cashtags:
        if cashtag and len(cashtag) > 1 and len(cashtag) <= 120:
            process_cashtag(tweet.tweet_id, cashtag)
    for hashtag in hashtags:
        if hashtag and len(hashtag) > 1 and len(hashtag) <= 120:
            process_hashtag(tweet.tweet_id, hashtag)
    for mention in mentions:
        if mention and len(mention) > 1 and len(mention) <= 120:
            process_mention(tweet.tweet_id, mention)
    for url in urls:
        if url and len(url) > 1 and len(url) <= 255:
            process_url(tweet.tweet_id, url)
    # ScopedSession.remove()


def process_cashtag(tweet_id, cashtag):
    subsession = ScopedSession()
    pg_cashtag = subsession.query(TweetCashtags) \
        .filter_by(tweet_id=tweet_id) \
        .filter_by(cashtags=cashtag).first()
    if pg_cashtag is None:
        try:
            item = TweetCashtags(tweet_id=tweet_id, cashtags=cashtag)
            subsession.add(item)
            subsession.commit()
            print(tweet_id, "-> Inserted cashtag", cashtag)
        except Exception as e:
            print(type(e), str(e))
            raise
    # ScopedSession.remove()


def process_hashtag(tweet_id, hashtag):
    subsession = ScopedSession()
    pg_hashtag = subsession.query(TweetHashtags) \
        .filter_by(tweet_id=tweet_id) \
        .filter_by(hashtags=hashtag).first()
    pg_hashtag_lower = subsession.query(TweetHashtags) \
        .filter_by(tweet_id=tweet_id) \
        .filter_by(hashtags=hashtag.lower()).first()
    if pg_hashtag is None and pg_hashtag_lower is None:
        try:
            item = TweetHashtags(tweet_id=tweet_id, hashtags=hashtag)
            subsession.add(item)
            subsession.commit()
            print(tweet_id, "-> Inserted hashtag", hashtag)
        except Exception as e:
            print(type(e), str(e))
            raise
    # ScopedSession.remove()


def process_mention(tweet_id, mention):
    mention = mention.replace('@', '')
    subsession = ScopedSession()
    pg_mention = subsession.query(TweetMentions) \
        .filter_by(tweet_id=tweet_id) \
        .filter_by(mentions=mention).first()
    if pg_mention is None:
        user = subsession.query(User).filter_by(twitter_handle=mention).first()
        if user is None:
            return None
        try:
            item = TweetMentions(tweet_id=tweet_id, mentions=mention, user_id=user.user_id)
            subsession.add(item)
            subsession.commit()
            print(tweet_id, "-> Inserted mention", mention)
        except Exception as e:
            print(type(e), str(e))
            raise
    # ScopedSession.remove()


def process_url(tweet_id, url):
    subsession = ScopedSession()
    pg_url = subsession.query(TweetUrl).filter_by(tweet_id=tweet_id).filter_by(url=url).first()
    if pg_url is None:
        try:
            item = TweetUrl(tweet_id=tweet_id, url=url)
            subsession.add(item)
            subsession.commit()
            print(tweet_id, "-> Inserted url", url)
        except Exception as e:
            print(type(e), str(e))
            raise
    # ScopedSession.remove()


def clean_text(tweet_id):
    subsession = ScopedSession()
    try:
        t2 = subsession.query(Tweet).filter(Tweet.tweet_id == tweet_id).first()
        t2.text = re.sub(r'\$ (?P<s>[A-Za-z][A-Za-z0-9]{0,4}\b)', '$\g<s>', t2.text)
        t2.text = re.sub(r'\# (?P<s>\w*[a-zA-Z]+\w*)', '#\g<s>', t2.text)
        t2.text = re.sub(r'\@ (?P<s>[\w_]+)', '@\g<s>', t2.text)
        subsession.add(t2)
        subsession.commit()
    except Exception as e:
        print(type(e), str(e))
    # ScopedSession.remove()
    return t2.tweet_id


def main_worker(t):
    print(t.text, t.permalink)
    tweet_id = clean_text(t.tweet_id)
    extract(tweet_id)


if __name__ == '__main__':
    session = Session()
    # try:
    #     tweets = session.query(Tweet).filter(Tweet.text.op('~')('[#$@]\s\w+'))
    #     # tweets = session.query(Tweet).limit(5)
    #     for t in tweets.yield_per(100):
    #         print(t.text, t.permalink)
    #         tweet_id = clean_text(t.tweet_id)
    #         extract(tweet_id)
    # except Exception as e:
    #     print(type(e), str(e))
    #     raise
    # exit()
  
    tweets = session.query(Tweet).filter(Tweet.text.op('~')('[#$@]\s\w+')).yield_per(100)
    with cf.ThreadPoolExecutor(max_workers=16) as executor:
        try:
            executor.map(main_worker, tweets)
        except Exception as e:
            print('ThreadPool', type(e), str(e))
            raise

    print("ALL DONE.")
