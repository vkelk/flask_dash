import concurrent.futures as cf
from datetime import datetime, timedelta
import logging
import logging.config
from pprint import pprint

from sqlalchemy import text, func
from sqlalchemy.orm import sessionmaker, scoped_session
from sqlalchemy.sql.expression import true

import settings
from count_helper import db_engine, TradingDays, convert_date, mvCashtags, FintweetCounts, FintweetCountTweets


FREQUENCY = 15  # minutes


def get_tweet_ids(c):
    ScopedSession = scoped_session(sessionmaker(bind=db_engine))
    session = ScopedSession()
    datetime_start = convert_date(c['period_start'])
    datetime_end = datetime_start + timedelta(minutes=FREQUENCY)
    # logger.debug('Start date: %s, end date: %s', datetime_start, datetime_end)
    # print(datetime_start, datetime_end)
    tweets = session.query(mvCashtags.tweet_id) \
        .filter(mvCashtags.cashtags == c['cashtag']) \
        .filter(mvCashtags.datetime.between(datetime_start, datetime_end))
    try:
        data = {
            'tweet_ids': [t[0] for t in tweets.all()],
            'date': c['period_start'],
            # 'day_status': c['day_status'],
            'cashtag': c['cashtag']
            }
    except Exception:
        logger.exception('message')
    finally:
        session.close()
        ScopedSession.remove()
    return data


def get_trading_periods(c):
    days = []
    last_date_time = c['date_from']
    while last_date_time <= c['date_to']:
        days.append(last_date_time)
        last_date_time = last_date_time + timedelta(minutes=FREQUENCY)
        # print(last_date_time)
    logger.info('Generated %s periods', len(days))
    return days


def get_tweet_list(c):
    '''
    Input dict should follow this format
    conditions = {
        'cashtag': string,
        'date_from': datetime_object,
        'date_to': datetime_object,
        'day_status': string: 'all', 'trading', 'non-trading'
        'date_joined': date_string,
        'followers': integer,
        'following': integer,
    }
    '''
    logger.info('Getting tweet list for %s', c['cashtag'])
    # ScopedSession = scoped_session(sessionmaker(bind=db_engine))
    # session = ScopedSession()
    # date_delta = c['date_to'] - c['date_from']
    periods = get_trading_periods(c)
    # trading_days = session.query(TradingDays.date) \
    #     .filter(TradingDays.is_trading == true()) \
    #     .filter(TradingDays.date.between(c['date_from'], c['date_to']))
    # tdays_list = [d[0] for d in trading_days.all()]
    # session.close()
    result = []
    for time_frame in periods:
        cn = c.copy()
        cn['period_start'] = time_frame
        # if c['day_status'] in ['trading', 'all'] and time_frame.date() in tdays_list:
        #     cn['day_status'] = 'trading'
        # elif c['day_status'] in ['non-trading', 'all'] and time_frame.date() not in tdays_list:
        #     cn['day_status'] = 'non-trading'
        # else:
        #     logger.debug('Skipping date %s. Do not apply the %s contition', time_frame.date(), settings.days)
        #     continue
        result.append(cn)
    logger.info('Mapped conditions to periods')
    res_list = []
    full_list = []
    # single-thread loop
    logger.debug('Running single-thread loop')
    for i in result:
        tw = get_tweet_ids(i)
        if len(tw['tweet_ids']) > 0:
            tw['tweets_count'] = len(tw['tweet_ids'])
            full_list.extend(tw['tweet_ids'])
            res_list.append(tw)
            print(tw)
    exit()
    logger.debug('Running multi-thread loop')
    with cf.ThreadPoolExecutor(max_workers=32) as executor:
        future_to_tweet = {executor.submit(get_tweet_ids, i): i for i in result}
        for future in cf.as_completed(future_to_tweet):
            try:
                tw = future.result()
                if len(tw['tweet_ids']) > 0:
                    tw['tweets_count'] = len(tw['tweet_ids'])
                    full_list.extend(tw['tweet_ids'])
                    res_list.append(tw)
                    print(tw)
            except Exception:
                logger.exception('message')
        logger.info('Completed tweet list for %s', c['cashtag'])
    return res_list, full_list


def create_logger():
    log_file = 'counts_pop_' + str(datetime.now().strftime('%Y-%m-%d')) + '.log'
    logging.config.fileConfig('log.ini', defaults={'logfilename': log_file})
    return logging.getLogger(__name__)


def insert_count_data(t):
    uuid = t[0].strip('$') + ''.join(i for i in str(t[1]) if i.isdigit())
    count = FintweetCounts(uuid=uuid, cashtag=t[0], period=t[1], tweets=len(t[2]))
    try:
        ScopedSession = scoped_session(sessionmaker(bind=db_engine))
        session = ScopedSession()
        session.add(count)
        session.commit()
    except Exception:
        logger.exception('message')
        raise
    for tweet_id in t[2]:
        tweet_ids = FintweetCountTweets(uuid=uuid, tweet_id=tweet_id)
        try:
            session.add(tweet_ids)
            session.commit()
        except Exception:
            logger.exception('message')
            raise
        finally:
            session.close()
            ScopedSession.remove()
    logger.info('Inserted UUID %s', uuid)


def page_query(q):
    offset = 0
    while True:
        r = False
        for elem in q.limit(2000).offset(offset):
            r = True
            yield elem
        offset += 2000
        print(offset)
        if not r:
            break


logger = create_logger()


if __name__ == '__main__':
    logger.info('*** Script started')
    ScopedSession = scoped_session(sessionmaker(bind=db_engine))
    session = ScopedSession()
    query = "SELECT cashtags, to_timestamp(FLOOR ((EXTRACT ('epoch' FROM datetime)/900))*900) AT TIME ZONE 'UTC' AS interval, \
    ARRAY_AGG(tweet_id) as tweet_ids \
    FROM fintweet.mv_cashtags \
    GROUP BY cashtags, interval"
    # tweets = session.execute(query)
    interval = func.to_timestamp(func.floor((func.extract('epoch', mvCashtags.datetime)/900))*900) \
        .op('AT TIME ZONE')('UTC').label('interval')
    sa_query = session.query(mvCashtags.cashtags, interval, func.array_agg(mvCashtags.tweet_id).label('tweet_ids')) \
        .group_by(mvCashtags.cashtags).group_by('interval')
    # for t in page_query(sa_query):
    #     insert_count_data(t)
    # exit()
    with cf.ThreadPoolExecutor(max_workers=16) as executor:
        try:
            executor.map(insert_count_data, page_query(sa_query.execution_options(stream_results=True)))
        except Exception:
            logger.exception('message')
            raise
    print("ALL DONE.")
