import re
import concurrent.futures as cf
# import time

from sqlalchemy import Table
# from sqlalchemy.sql.expression import func

from stocktwits import preprocess
from stocktwits.models import Base, stocktwits_meta, Session, ScopedSession, Ideas

RE_HASHTAG_VALID = re.compile(r'(?:\#\w*[a-zA-Z]+\w*)', re.VERBOSE | re.IGNORECASE)


class IdeaHashtagsNew(Base):
    __table__ = Table('idea_hashtags_new', stocktwits_meta, autoload=True)


class IdeaCashtagsNew(Base):
    __table__ = Table('idea_cashtags_new', stocktwits_meta, autoload=True)


class IdeaUrlsNew(Base):
    __table__ = Table('idea_urls_new', stocktwits_meta, autoload=True)


# def tokenize(s):
#     return RE_HASHTAG_VALID.findall(s)


# def preprocess(s, lowercase=False, upercase=False):
#     tokens = tokenize(s)
#     if upercase:
#         tokens = [token.upper() for token in tokens]
#     return tokens


def extract(idea):
    global i
    tokens = preprocess(idea.text)
    cashtags = set([term.upper() for term in tokens if term.startswith('$') and len(term) > 1])
    hashtags = set([term for term in tokens if term.startswith('#') and len(term) > 1])
    urls = set([term for term in tokens if term.startswith('http') and len(term) > 4])
    for cashtag in cashtags:
        if cashtag and len(cashtag) > 1 and len(cashtag) <= 120:
            process_cashtag(idea.ideas_id, cashtag)
    for hashtag in hashtags:
        if hashtag and len(hashtag) > 1 and len(hashtag) <= 120:
            process_hashtag(idea.ideas_id, hashtag)
    for url in urls:
        if cashtag and len(cashtag) > 4 and len(cashtag) <= 255:
            process_url(idea.ideas_id, url)


def process_cashtag(ideas_id, cashtag):
    subsession = Session()
    pg_cashtag = subsession.query(IdeaCashtagsNew).filter_by(ideas_id=ideas_id).filter_by(cashtag=cashtag).first()
    if pg_cashtag is None:
        try:
            item = IdeaCashtagsNew(ideas_id=ideas_id, cashtag=cashtag)
            subsession.add(item)
            subsession.commit()
            print(ideas_id, "-> Inserted cashtag", cashtag)
        except Exception as e:
            print(type(e), str(e))
            raise


def process_hashtag(ideas_id, hashtag):
    subsession = ScopedSession()
    pg_hashtag = subsession.query(IdeaHashtagsNew).filter_by(ideas_id=ideas_id).filter_by(hashtag=hashtag).first()
    if pg_hashtag is None:
        try:
            item = IdeaHashtagsNew(ideas_id=ideas_id, hashtag=hashtag)
            subsession.add(item)
            subsession.commit()
            print(ideas_id, "-> Inserted hashtag", hashtag)
        except Exception as e:
            print(type(e), str(e))
            raise


def process_url(ideas_id, url):
    subsession = ScopedSession()
    pg_url = subsession.query(IdeaUrlsNew).filter_by(ideas_id=ideas_id).filter_by(url=url).first()
    if pg_url is None:
        try:
            item = IdeaUrlsNew(ideas_id=ideas_id, url=url)
            subsession.add(item)
            subsession.commit()
            print(ideas_id, "-> Inserted url", url)
        except Exception as e:
            print(type(e), str(e))
            raise


if __name__ == '__main__':
    try:
        session = Session()
        for t in session.query(Ideas).filter(Ideas.ideas_id > 1)\
                .filter(Ideas.text.like("%#%")) \
                .order_by(Ideas.ideas_id.asc()).yield_per(100):
            # print(t.text)
            extract(t)
    except Exception as e:
        print(type(e), str(e))
        raise
    exit()

    ideas = session.query(Ideas).filter(Ideas.ideas_id > 1) \
        .order_by(Ideas.ideas_id.asc()).yield_per(100)
    with cf.ProcessPoolExecutor(max_workers=4) as executor:
        try:
            executor.map(extract, ideas)
        except Exception as e:
            print(type(e), str(e))
            raise

    print("ALL DONE.")
