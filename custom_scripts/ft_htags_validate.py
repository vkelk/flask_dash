import re
import concurrent.futures as cf
import time

from sqlalchemy import or_
from sqlalchemy.sql.expression import func

from fintweet.models import TweetCashtags, Session, ScopedSession

RE_HASHTAG_VALID = re.compile(r'(?:\B#\w*[a-zA-Z]+\w*)', re.VERBOSE | re.IGNORECASE)


def get_big_cashtags(chars=6):
    query = session.query(TweetCashtags.cashtags) \
        .filter(or_(
            func.length(TweetCashtags.cashtags) > chars,
            TweetCashtags.cashtags.like("%.%"),
            TweetCashtags.cashtags.like("%:%")))\
        .group_by(TweetCashtags.cashtags).yield_per(10)
    return query


def cashtag_regclean(text):
    re_search = RE_HASHTAG_VALID.search(text)
    if re_search:
        return re_search.group(0)
    return None


def main_worker(cashtag):
    rx = cashtag_regclean(cashtag.cashtags)
    if cashtag.cashtags == rx:
        return None
    elif rx is None:
        print('Removing hashtag {}...'.format(cashtag.cashtags))
        s = ScopedSession()
        s.query(TweetCashtags)\
            .filter(TweetCashtags.cashtags == cashtag.cashtags)\
            .delete()
        s.commit()
    else:
        # TODO: update cashtag
        print('Updating {} -> {}...'.format(cashtag.cashtags, rx))
        s = ScopedSession()
        s.query(TweetCashtags)\
            .filter(TweetCashtags.cashtags == cashtag.cashtags)\
            .update({TweetCashtags.cashtags: rx})
        s.commit()


session = Session()

if __name__ == '__main__':
    time_start = time.time()
    # for cashtag in get_big_cashtags(6):
    #     main_worker(cashtag)
    # exit()

    with cf.ThreadPoolExecutor(max_workers=10) as executor:
        executor.map(main_worker, get_big_cashtags(6))

    total_time = time.time() - time_start
    print('ALL DONE in %3f sec' % total_time)
