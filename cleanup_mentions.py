import re
import concurrent.futures as cf
# import time

from sqlalchemy import Table
# from sqlalchemy.sql.expression import func

from fintweet.models import Base, fintweet_meta, Session, ScopedSession, Tweet

RE_HASHTAG_VALID = re.compile(r'(?:\#\w*[a-zA-Z]+\w*)', re.VERBOSE | re.IGNORECASE)


class TweetHashtagsNew(Base):
    __table__ = Table('tweet_hashtags_new', fintweet_meta, autoload=True)


def tokenize(s):
    return RE_HASHTAG_VALID.findall(s)


def preprocess(s, lowercase=False, upercase=False):
    tokens = tokenize(s)
    if upercase:
        tokens = [token.upper() for token in tokens]
    return tokens


def extract(tweet):
    global i
    tokens = preprocess(tweet.text, upercase=True)
    # cashtags = set([term for term in tokens if term.startswith('$') and len(term) > 1])
    hashtags = set([term for term in tokens if term.startswith('#') and len(term) > 1])
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
        for t in session.query(Tweet).filter(Tweet.tweet_id > 1)\
                .filter(Tweet.text.like("%#%")) \
                .order_by(Tweet.tweet_id.asc()).yield_per(100):
            # print(t.text)
            extract(t)
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
