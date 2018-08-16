import argparse
import concurrent.futures as cf
from datetime import datetime
import logging
import logging.config
import os
import re
import sys

import pandas as pd
import psycopg2
import psycopg2.extras

import settings

pg_config = {
    'user': settings.PG_USER,
    'password': settings.PG_PASSWORD,
    'host': settings.DB_HOST,
    'dbname': settings.PG_DBNAME}


def create_logger():
    log_file = 'panama_' + str(datetime.now().strftime('%Y-%m-%d')) + '.log'
    logging.config.fileConfig('log.ini', defaults={'logfilename': log_file}, disable_existing_loggers=False)
    return logging.getLogger(__name__)


def slugify(s):
    return re.sub('[^\w]+', '-', s).lower()


def dataframe_from_file(filename):
    try:
        ext = os.path.splitext(filename)[1]
        if ext in ['.xls', '.xlsx']:
            df = pd.read_excel(filename, encoding='utf-8')
            df.columns = [slugify(col.strip()) for col in df.columns]
            if 'cashtag' in df.columns and 'gvkey' in df.columns:
                return df
            else:
                logger.warning('The input file does not contains wanted header')
                print(df.columns)
        # TODO: Create import from CSV
    except Exception:
        logger.error('Cannot open filename %s', filename)
        # logger.exception('message')
        exit()
    return None


def get_counts(cashtags):
    q = """
        SELECT
            cashtags,
            sum(case when datetime::DATE IN (SELECT DATE FROM dashboard.trading_days) 
                then 1 else 0 end) as trading,
            sum(case when datetime::DATE NOT IN (SELECT DATE FROM dashboard.trading_days) 
                then 1 else 0 end) as nontrading,
            sum(case when datetime::DATE IN (SELECT DATE FROM dashboard.trading_days) 
                AND datetime::time < '09:00:00' then 1 else 0 end) as pretrading,
            sum(case when datetime::DATE IN (SELECT DATE FROM dashboard.trading_days) 
                AND datetime::time > '16:00:00' then 1 else 0 end) as posttrading,
            sum(1) as total
        FROM
            mv_cashtags
        WHERE
            cashtags in %s 
        GROUP BY cashtags
        """
    # q = "SELECT id, status, filename, date_string FROM file_info WHERE url = %s or filename = %s"
    try:
        cur = cnx.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute(q, (cashtags,))
        results = cur.fetchall()
        cnx.commit()
    except psycopg2.Error as err:
        results = None
        logger.error(err)
        cnx.rollback()
    finally:
        cur.close()
    return results


logger = create_logger()

try:
    cnx = psycopg2.connect(**pg_config)
    cur = cnx.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute("SET SEARCH_PATH = %s" % 'fintweet')
    cnx.commit()
    logger.info('Connected to database')
except psycopg2.Error as err:
    logger.error(err)
finally:
    cur.close()

if __name__ == '__main__':
    logger.info('*** Script started')
    cashtags_list = ('$AAPL', '$FB', '$MSFT')
    results = get_counts(cashtags_list)
    print(results)



'''
SELECT
	cashtags,
	sum(case when datetime :: DATE IN ( SELECT DATE FROM dashboard.trading_days ) then 1 else 0 end) as trading,
	sum(case when datetime :: DATE NOT IN ( SELECT DATE FROM dashboard.trading_days ) then 1 else 0 end) as nontrading,
	sum(case when datetime :: DATE IN ( SELECT DATE FROM dashboard.trading_days ) AND datetime::time < '09:00:00' then 1 else 0 end) as pretrading,
	sum(case when datetime :: DATE IN ( SELECT DATE FROM dashboard.trading_days ) AND datetime::time > '16:00:00' then 1 else 0 end) as posttrading,
	sum(1) as total
FROM
	mv_cashtags 
WHERE
	cashtags in ('$FB', '$AAPL') 
GROUP BY cashtags;
'''