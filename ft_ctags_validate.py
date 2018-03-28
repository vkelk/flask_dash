import re
import concurrent.futures as cf
import time

from sqlalchemy.sql.expression import func

from fintweet.models import TweetCashtags, Session

RE_CASHTAG_VALID = re.compile(r'\$[A-Za-z0-9]{1,5}\b', re.VERBOSE | re.IGNORECASE)


def get_big_cashtags(chars=6):
    query = session.query(TweetCashtags).filter(
        (func.length(TweetCashtags.cashtags) > chars)).yield_per(100)
    return query


def cashtag_regclean(text):
    re_search = RE_CASHTAG_VALID.search(text)
    if re_search:
        return re_search.group(0)
    return None


def main_worker(cashtag):
    rx = cashtag_regclean(cashtag.cashtags)
    if cashtag.cashtags == rx:
        return None
    elif rx is None:
        # TODO: remove cashtag
        pass
    else:
        # TODO: update cashtag
        pass


session = Session()

if __name__ == '__main__':
    time_start = time.time()
    # for cashtag in get_big_cashtags(6):
    # main_worker(cashtag)

    with cf.ThreadPoolExecutor(max_workers=8) as executor:
        executor.map(main_worker, get_big_cashtags(6))

    total_time = time.time() - time_start
    print('ALL DONE in %3f sec' % total_time)
