import argparse
import concurrent.futures as cf
from datetime import datetime
import logging
import logging.config
import os
import re
import sys

import pandas as pd

import settings
from panama_helper import *


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


logger = create_logger()

if __name__ == '__main__':
    logger.info('*** Script started')
    df_in = dataframe_from_file(settings.input_file_name)
    if df_in is None or df_in.empty:
        logger.error('Could not load imput parameters from file %s', settings.input_file_name)
        logger.error('The input file should contain two columns: "gvkey" and "cashtag"')
        exit()
    else:
        logger.info('Loaded data from %s', settings.input_file_name)
    df_output = pd.DataFrame()
    index2 = 0
    for index, row in df_in.iterrows():
        conditions = {
            'cashtag': row['cashtag'],
            'date_from': datetime.strptime(settings.date_from, '%Y-%m-%d %H:%M:%S'),
            'date_to': datetime.strptime(settings.date_to, '%Y-%m-%d %H:%M:%S'),
        }
        with cf.ThreadPoolExecutor(max_workers=16) as executor:
            try:
                future_to_tweet = {executor.submit(load_counts_v2, t): t for t in get_cashtag_periods(conditions)}
                logger.info('Starting count process for %s tweet list', row['cashtag'])
                for future in cf.as_completed(future_to_tweet):
                    i += 1

