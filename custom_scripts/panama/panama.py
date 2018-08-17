from datetime import datetime
import logging
import logging.config
import os
import re

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
            if 'cashtag' in df.columns:
                return df
            else:
                logger.warning('The input file does not contains wanted header')
                print(df.columns)
        # TODO: Create import from CSV
    except Exception:
        logger.error('Cannot open filename %s', filename)
        exit()
    return None


def get_ft_counts(t):
    q = """
        SELECT
            cashtags,
            sum(case when datetime::DATE IN (SELECT DATE FROM dashboard.trading_days)
                then 1 else 0 end) as trading,
            sum(case when datetime::DATE NOT IN (SELECT DATE FROM dashboard.trading_days)
                then 1 else 0 end) as nontrading,
            sum(case when datetime::DATE IN (SELECT DATE FROM dashboard.trading_days)
                AND datetime::time < '09:30:00' then 1 else 0 end) as pretrading,
            sum(case when datetime::DATE IN (SELECT DATE FROM dashboard.trading_days)
                AND datetime::time > '16:00:00' then 1 else 0 end) as posttrading,
            sum(1) as total,
            min(case when datetime::DATE IN (SELECT DATE FROM dashboard.trading_days)
                then datetime::DATE end) as trading_first,
            max(case when datetime::DATE IN (SELECT DATE FROM dashboard.trading_days)
                then datetime::DATE end) as trading_last,
            min(case when datetime::DATE not IN (SELECT DATE FROM dashboard.trading_days)
                then datetime::DATE end) as nontrading_first,
            max(case when datetime::DATE not IN (SELECT DATE FROM dashboard.trading_days)
                then datetime::DATE end) as nontrading_last,
            min(case when datetime::DATE IN (SELECT DATE FROM dashboard.trading_days) AND datetime::time < '09:30:00'
                then datetime::DATE end) as pretrading_first,
            max(case when datetime::DATE IN (SELECT DATE FROM dashboard.trading_days) AND datetime::time < '09:30:00'
                then datetime::DATE end) as pretrading_last,
            min(case when datetime::DATE IN (SELECT DATE FROM dashboard.trading_days) AND datetime::time > '16:00:00'
                then datetime::DATE end) as posttrading_first,
            max(case when datetime::DATE IN (SELECT DATE FROM dashboard.trading_days) AND datetime::time > '16:00:00'
                then datetime::DATE end) as posttrading_last
        FROM mv_cashtags WHERE datetime::date < '2017-01-01' AND cashtags = %s GROUP BY cashtags
        """
    # q = "SELECT id, status, filename, date_string FROM file_info WHERE url = %s or filename = %s"
    try:
        cur = cnx.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute("SET SEARCH_PATH = %s" % 'fintweet')
        cur.execute(q, (t['cashtag'],))
        results = cur.fetchone()
        cnx.commit()
    except psycopg2.Error as err:
        results = None
        logger.error(err)
        cnx.rollback()
    finally:
        cur.close()
    if results:
        t['tweets_trade'] = results['trading']
        t['tweets_nontrade'] = results['nontrading']
        t['tweets_pre'] = results['pretrading']
        t['tweets_post'] = results['posttrading']
        t['tot_tweets'] = results['total']
        t['trade_firstdate'] = results['trading_first']
        t['trade_lastdate'] = results['trading_last']
        t['nontrade_firstdate'] = results['nontrading_first']
        t['nontrade_lastdate'] = results['nontrading_last']
        t['pre_firstdate'] = results['pretrading_first']
        t['pre_lastdate'] = results['pretrading_last']
        t['post_firstdate'] = results['posttrading_first']
        t['post_lastdate'] = results['posttrading_last']
    return t


def get_ft_days(t):
    q_trading = """
        SELECT	datetime::date FROM mv_cashtags WHERE cashtags = %s
        AND datetime::date in (SELECT date from dashboard.trading_days) GROUP BY datetime::DATE
    """
    q_nontrading = """
        SELECT	datetime::date FROM mv_cashtags WHERE cashtags = %s
        AND datetime::date not in (SELECT date from dashboard.trading_days) GROUP BY datetime::DATE
    """
    q_pretrading = """
        SELECT	datetime::date FROM mv_cashtags WHERE cashtags = %s
        AND datetime::date in (SELECT date from dashboard.trading_days)
        AND datetime::time < '09:30:00' GROUP BY datetime::DATE
    """
    q_posttrading = """
        SELECT	datetime::date FROM mv_cashtags WHERE cashtags = %s
        AND datetime::date in (SELECT date from dashboard.trading_days)
        AND datetime::time > '16:00:00' GROUP BY datetime::DATE
    """
    q_total = "SELECT datetime::date FROM mv_cashtags WHERE cashtags = %s  GROUP BY datetime::DATE"
    try:
        cur = cnx.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute("SET SEARCH_PATH = %s" % 'fintweet')
        cur.execute(q_trading, (t['cashtag'],))
        t['tot_obs_trade'] = cur.rowcount
        cur.execute(q_nontrading, (t['cashtag'],))
        t['tot_obs_non_trading_days'] = cur.rowcount
        cur.execute(q_pretrading, (t['cashtag'],))
        t['tot_obs_pre'] = cur.rowcount
        cur.execute(q_posttrading, (t['cashtag'],))
        t['tot_obs_post'] = cur.rowcount
        cur.execute(q_total, (t['cashtag'],))
        t['total_obs'] = cur.rowcount
        cnx.commit()
    except psycopg2.Error as err:
        logger.error(err)
        cnx.rollback()
    finally:
        cur.close()
    return t


def get_ft_data(t):
    t = get_ft_counts(t)
    t = get_ft_days(t)
    return t


def get_st_counts(t):
    q = """
        SELECT
            cashtag,
            sum(case when datetime::DATE IN (SELECT DATE FROM dashboard.trading_days)
                then 1 else 0 end) as trading,
            sum(case when datetime::DATE NOT IN (SELECT DATE FROM dashboard.trading_days)
                then 1 else 0 end) as nontrading,
            sum(case when datetime::DATE IN (SELECT DATE FROM dashboard.trading_days)
                AND datetime::time < '09:30:00' then 1 else 0 end) as pretrading,
            sum(case when datetime::DATE IN (SELECT DATE FROM dashboard.trading_days)
                AND datetime::time > '16:00:00' then 1 else 0 end) as posttrading,
            sum(1) as total,
            min(case when datetime::DATE IN (SELECT DATE FROM dashboard.trading_days)
                then datetime::DATE end) as trading_first,
            max(case when datetime::DATE IN (SELECT DATE FROM dashboard.trading_days)
                then datetime::DATE end) as trading_last,
            min(case when datetime::DATE not IN (SELECT DATE FROM dashboard.trading_days)
                then datetime::DATE end) as nontrading_first,
            max(case when datetime::DATE not IN (SELECT DATE FROM dashboard.trading_days)
                then datetime::DATE end) as nontrading_last,
            min(case when datetime::DATE IN (SELECT DATE FROM dashboard.trading_days) AND datetime::time < '09:30:00'
                then datetime::DATE end) as pretrading_first,
            max(case when datetime::DATE IN (SELECT DATE FROM dashboard.trading_days) AND datetime::time < '09:30:00'
                then datetime::DATE end) as pretrading_last,
            min(case when datetime::DATE IN (SELECT DATE FROM dashboard.trading_days) AND datetime::time > '16:00:00'
                then datetime::DATE end) as posttrading_first,
            max(case when datetime::DATE IN (SELECT DATE FROM dashboard.trading_days) AND datetime::time > '16:00:00'
                then datetime::DATE end) as posttrading_last
        FROM mv_cashtags WHERE datetime::date < '2017-01-01' AND cashtag = %s GROUP BY cashtag
        """
    # q = "SELECT id, status, filename, date_string FROM file_info WHERE url = %s or filename = %s"
    try:
        cur = cnx.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute("SET SEARCH_PATH = %s" % 'stocktwits')
        cur.execute(q, (t['cashtag'],))
        results = cur.fetchone()
        cnx.commit()
    except psycopg2.Error as err:
        results = None
        logger.error(err)
        cnx.rollback()
    finally:
        cur.close()
    if results:
        t['tweets_trade'] = results['trading']
        t['tweets_nontrade'] = results['nontrading']
        t['tweets_pre'] = results['pretrading']
        t['tweets_post'] = results['posttrading']
        t['tot_tweets'] = results['total']
        t['trade_firstdate'] = results['trading_first']
        t['trade_lastdate'] = results['trading_last']
        t['nontrade_firstdate'] = results['nontrading_first']
        t['nontrade_lastdate'] = results['nontrading_last']
        t['pre_firstdate'] = results['pretrading_first']
        t['pre_lastdate'] = results['pretrading_last']
        t['post_firstdate'] = results['posttrading_first']
        t['post_lastdate'] = results['posttrading_last']
    return t


def get_st_days(t):
    q_trading = """
        SELECT	datetime::date FROM mv_cashtags WHERE cashtag = %s
        AND datetime::date in (SELECT date from dashboard.trading_days) GROUP BY datetime::DATE
    """
    q_nontrading = """
        SELECT	datetime::date FROM mv_cashtags WHERE cashtag = %s
        AND datetime::date not in (SELECT date from dashboard.trading_days) GROUP BY datetime::DATE
    """
    q_pretrading = """
        SELECT	datetime::date FROM mv_cashtags WHERE cashtag = %s
        AND datetime::date in (SELECT date from dashboard.trading_days)
        AND datetime::time < '09:30:00' GROUP BY datetime::DATE
    """
    q_posttrading = """
        SELECT	datetime::date FROM mv_cashtags WHERE cashtag = %s
        AND datetime::date in (SELECT date from dashboard.trading_days)
        AND datetime::time > '16:00:00' GROUP BY datetime::DATE
    """
    q_total = "SELECT datetime::date FROM mv_cashtags WHERE cashtag = %s  GROUP BY datetime::DATE"
    try:
        cur = cnx.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute("SET SEARCH_PATH = %s" % 'stocktwits')
        cur.execute(q_trading, (t['cashtag'],))
        t['tot_obs_trade'] = cur.rowcount
        cur.execute(q_nontrading, (t['cashtag'],))
        t['tot_obs_non_trading_days'] = cur.rowcount
        cur.execute(q_pretrading, (t['cashtag'],))
        t['tot_obs_pre'] = cur.rowcount
        cur.execute(q_posttrading, (t['cashtag'],))
        t['tot_obs_post'] = cur.rowcount
        cur.execute(q_total, (t['cashtag'],))
        t['total_obs'] = cur.rowcount
        cnx.commit()
    except psycopg2.Error as err:
        logger.error(err)
        cnx.rollback()
    finally:
        cur.close()
    return t


def get_st_data(t):
    t = get_st_counts(t)
    t = get_st_days(t)
    return t


def populate_df_output(df_output, index, t):
    df_output.at[index, 'cashtag'] = t['cashtag']
    df_output.at[index, 'database'] = t['database']
    df_output.at[index, 'tweets_trade'] = t['tweets_trade']
    df_output.at[index, 'tweets_nontrade'] = t['tweets_nontrade']
    df_output.at[index, 'tweets_pre'] = t['tweets_pre']
    df_output.at[index, 'tweets_post'] = t['tweets_post']
    df_output.at[index, 'tot_tweets'] = t['tot_tweets']
    df_output.at[index, 'tot_obs_trade'] = t['tot_obs_trade']
    df_output.at[index, 'tot_obs_pre'] = t['tot_obs_pre']
    df_output.at[index, 'tot_obs_post'] = t['tot_obs_post']
    df_output.at[index, 'tot_obs_non_trading_days'] = t['tot_obs_non_trading_days']
    df_output.at[index, 'total_obs'] = t['total_obs']
    df_output.at[index, 'trade_firstdate'] = str(t['trade_firstdate'])
    df_output.at[index, 'trade_lastdate'] = str(t['trade_lastdate'])
    df_output.at[index, 'nontrade_firstdate'] = str(t['nontrade_firstdate'])
    df_output.at[index, 'nontrade_lastdate'] = str(t['nontrade_lastdate'])
    df_output.at[index, 'pre_firstdate'] = str(t['pre_firstdate'])
    df_output.at[index, 'pre_lastdate'] = str(t['pre_lastdate'])
    df_output.at[index, 'post_firstdate'] = str(t['post_firstdate'])
    df_output.at[index, 'post_lastdate'] = str(t['post_lastdate'])
    return df_output


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
    df_in = dataframe_from_file(settings.input_file_name)
    if df_in is None or df_in.empty:
        logger.error('Could not load imput parameters from file %s', settings.input_file_name)
        logger.error('The input file should contain two columns: "gvkey" and "cashtag"')
        exit()
    else:
        logger.info('Loaded data from %s', settings.input_file_name)
    df_ft_output = pd.DataFrame()
    df_st_output = pd.DataFrame()
    index2 = 0
    for index, row in df_in.iterrows():
        ft = {
            'cashtag': row['cashtag'],
            'database': 'tweet'
        }
        st = {
            'cashtag': row['cashtag'],
            'database': 'stocktwits'
        }
        ft = get_ft_data(ft)
        # ft['database'] = 'tweet'
        st = get_st_data(st)
        # st['database'] = 'stocktwits'
        df_ft_output = populate_df_output(df_ft_output, index2, ft)
        df_st_output = populate_df_output(df_st_output, index2, st)
        index2 += 1
    if df_ft_output is None or df_ft_output.empty:
        logger.warning('Twitter results are empty.')
    else:
        print(df_ft_output)
        df_ft_output.to_stata('tweet_output.dta', write_index=False)
    if df_st_output is None or df_st_output.empty:
        logger.warning('Twitter results are empty.')
    else:
        print(df_st_output)
        df_st_output.to_stata('stocktwits_output.dta', write_index=False)
    logger.info('Process succesfuly finished.')
