import concurrent.futures as cf
from datetime import datetime
import logging
import logging.config
import os
import random
import sys

from sqlalchemy import func, desc

from fintweet.models import TweetCashtags, CompanySec, ScopedSession
from fintweet.scrapers.secedgar import search
from fintweet.settings import proxy_list

DIR_PATH = os.path.dirname(os.path.realpath(__file__))
LOG_DIR = os.path.join(DIR_PATH, 'logs')

if not os.path.exists(LOG_DIR):
    os.makedirs(LOG_DIR)


def create_logger(dirname, filename):
    date = datetime.now().date()
    log_filename = filename + '_' + str(date) + '.log'
    log_file = os.path.join(dirname, log_filename)
    log_file = log_file.replace('\\', '/')
    logging.config.fileConfig('log.ini', defaults={'logfilename': log_file})
    return logging.getLogger(__name__)


def get_cashtags():
    session = ScopedSession()
    query = session.query(TweetCashtags.cashtags, func.count(TweetCashtags.cashtags).label('count')) \
        .group_by(TweetCashtags.cashtags).having(func.count(TweetCashtags.cashtags) > 100) \
        .order_by(desc('count')).yield_per(10)
    return query


def main_worker(row):
    cashtag = row[0]
    ticker = cashtag.strip('$')
    session = ScopedSession()
    company = session.query(CompanySec).filter(CompanySec.ticker == ticker).first()
    if company:
        logger.info('Company {} with ticker {} already exists'.format(company.name, company.ticker))
        return None
    proxy = random.choice(proxy_list)
    result = search(ticker, proxy)
    try:
        company = CompanySec(
            ticker=ticker, name=result['CompanyName'], cik=result['CIK'], sik=result['SIK'])
        session.add(company)
        session.commit()
        logger.info('Inserted company {} with ticker {}'.format(company.name, company.ticker))
    except Exception as e:
        # print(type(e), e)
        logger.error(str(type(e) + ' ' + e))
        raise


if __name__ == '__main__':
    logger = create_logger(LOG_DIR, 'company_ids')
    logger.info('LOGGER STARTED')

    global_count = 0
    global_start = datetime.now()

    # for row in get_cashtags():
    #     main_worker(row)
    # exit()

    with cf.ThreadPoolExecutor(max_workers=10) as executor:
        try:
            executor.map(main_worker, get_cashtags())
        except BaseException as e:
            logger.error(str(e))
            raise

    logger.info('ALL Done.')
    sys.exit()
