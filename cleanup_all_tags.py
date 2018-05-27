import re
import concurrent.futures as cf
# from pprint import pprint
# import time

from sqlalchemy import or_
# from sqlalchemy.sql.expression import func

from fintweet.models import Session, ScopedSession, Tweet, TweetCashtags, TweetHashtags, TweetMentions, User, TweetUrl

regex_str = [
    r'(?:@[\w_]+)',  # @-mentions
    r"(?:\#\w+)",  # hash-tags
    r"(?:\B\$[A-Za-z][A-Za-z0-9]{0,4}\b)",  # cash-tags
    r'(?:(?:(?:http[s?]?:\/\/)?(?:pic\.twitter\.com\/\w+)))',  # pics
    r'(?:http[s]?:\/\/(?:\w+|[$-_@.&amp;+]|[!*\(\),]|(?:%[0-9a-f][0-9a-f]))+)',  # URLs
]
tokens_re = re.compile(r'(' + '|'.join(regex_str) + ')', re.VERBOSE | re.IGNORECASE)


def tokenize(s):
    return tokens_re.findall(s)


# def preprocess(s, lowercase=False, upercase=False):
#     tokens = tokenize(s)
#     if upercase:
#         tokens = [token.upper() for token in tokens]
#     return tokens


def extract(tweet):
    # subsession = ScopedSession()
    # tweet = subsession.query(Tweet).filter_by(tweet_id=tweet_id).first()
    # print(tweet.tweet_id)
    tokens = tokenize(tweet.text)
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
        if url and len(url) > 4 and len(url) <= 255:
            process_url(tweet.tweet_id, url)
    # ScopedSession.remove()


def process_cashtag(tweet_id, cashtag):
    subsession = ScopedSession()
    pg_cashtags = subsession.query(TweetCashtags) \
        .filter_by(tweet_id=tweet_id).all()
    if pg_cashtags is None:
        try:
            item = TweetCashtags(tweet_id=tweet_id, cashtags=cashtag)
            subsession.add(item)
            subsession.commit()
            print(tweet_id, "-> Inserted cashtag", cashtag)
            # ScopedSession.remove()
            return item.tweet_id
        except Exception as e:
            print(type(e), str(e))
            raise
    for db_ctag in pg_cashtags:
        if db_ctag.cashtags == cashtag:
            continue
        if db_ctag.cashtags.upper() == cashtag:
            item = subsession.query(TweetCashtags).filter_by(id=db_ctag.id).first()
            # item = TweetCashtags(tweet_id=tweet_id, cashtags=cashtag)
            item.cashtags = cashtag
            subsession.add(item)
            subsession.commit()
            print(tweet_id, "-> Updated cashtag", cashtag)
            # ScopedSession.remove()
            return item.tweet_id
    # ScopedSession.remove()


def process_hashtag(tweet_id, hashtag):
    subsession = ScopedSession()
    pg_hashtags = subsession.query(TweetHashtags) \
        .filter_by(tweet_id=tweet_id).all()
    if pg_hashtags is None:
        try:
            item = TweetHashtags(tweet_id=tweet_id, hashtags=hashtag)
            subsession.add(item)
            subsession.commit()
            print(tweet_id, "-> Inserted hashtag", hashtag)
            # ScopedSession.remove()
            return item.tweet_id
        except Exception as e:
            print(type(e), str(e))
            raise
    for db_htag in pg_hashtags:
        if db_htag.hashtags == hashtag:
            continue
        if db_htag.hashtags.upper() == hashtag:
            item = subsession.query(TweetHashtags).filter_by(id=db_htag.id).first()
            # item = TweetCashtags(tweet_id=tweet_id, cashtags=cashtag)
            item.hashtags = hashtag
            subsession.add(item)
            subsession.commit()
            print(tweet_id, "-> Updated hashtag", hashtag)
            # ScopedSession.remove()
            return item.tweet_id
    # ScopedSession.remove()


def process_mention(tweet_id, mention):
    mention = mention.replace('@', '')
    subsession = ScopedSession()
    pg_mention = subsession.query(TweetMentions) \
        .filter_by(tweet_id=tweet_id) \
        .filter_by(mentions=mention).first()
    if pg_mention is None:
        mention2 = '@' + mention
        user = subsession.query(User).filter(or_(
            User.twitter_handle == mention,
            User.twitter_handle == mention2)).first()
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


if __name__ == '__main__':
    session = Session()
    # try:
    #     tweets = session.query(Tweet)
    #     # tweets = session.query(Tweet).limit(5)
    #     for t in tweets.execution_options(stream_results=True).yield_per(200):
    #         extract(t)
    # except Exception as e:
    #     print(type(e), str(e))
    #     raise
    # exit()

    tweets = page_query(session.query(Tweet))
    with cf.ThreadPoolExecutor(max_workers=16) as executor:
        try:
            executor.map(extract, tweets)
        except Exception as e:
            print('ThreadPool', type(e), str(e))
            raise

    print("ALL DONE.")
