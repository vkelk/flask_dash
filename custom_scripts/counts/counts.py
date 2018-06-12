import concurrent.futures as cf
from datetime import datetime, timedelta
import logging
import logging.config
import os
import re

import pandas as pd
from sqlalchemy.orm import sessionmaker, scoped_session
from sqlalchemy.sql.expression import true

import settings
from count_helper import db_engine, TradingDays, load_counts, get_tweet_ids


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
                print(df.columns)
        # TODO: Create import from CSV
    except Exception:
        logger.error('Cannot open filename %s', filename)
        logger.exception('message')
        raise
    return None


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
    ScopedSession = scoped_session(sessionmaker(bind=db_engine))
    date_delta = c['date_to'] - c['date_from']
    trading_days = ScopedSession.query(TradingDays.date) \
        .filter(TradingDays.is_trading == true()) \
        .filter(TradingDays.date.between(c['date_from'], c['date_to']))
    days_list = [d[0] for d in trading_days.all()]
    result = []
    for i in range(date_delta.days + 1):
        cn = c.copy()
        date_input = (c['date_from'] + timedelta(days=i))
        cn['date_input'] = date_input
        cn['date'] = date_input
        if c['day_status'] in ['trading', 'all'] and date_input.date() in days_list:
            cn['day_status'] = 'trading'
        elif c['day_status'] in ['non-trading', 'all'] and date_input.date() not in days_list:
            cn['day_status'] = 'non-trading'
        else:
            continue
        result.append(cn)
    res_list = []
    full_list = []
    with cf.ThreadPoolExecutor(max_workers=32) as executor:
        future_to_tweet = {executor.submit(get_tweet_ids, i): i for i in result}
        for future in cf.as_completed(future_to_tweet):
            try:
                tw = future.result()
                tw['tweets_count'] = len(tw['tweet_ids'])
                full_list.extend(tw['tweet_ids'])
                res_list.append(tw)
            except Exception:
                logger.exception('message')
        logger.info('Completed tweet list for %s', c['cashtag'])
    return res_list, full_list


def download_hashtags(hashtags_map):
    try:
        df_hashtags = pd.DataFrame(hashtags_map)
        df_hashtags.sort_values(by=['cashtag', 'count'], ascending=[True, False], inplace=True)
        file_hashtags = 'output_hashtags.dta'
        df_hashtags.to_stata(file_hashtags, write_index=False)
        logger.info('Hashtags output file is saved')
    except Exception:
        logger.error('Hashtags output file is not saved')
        logger.exception('message')


def download_users(users_map):
    try:
        df_users = pd.DataFrame(users_map)
        df_users.sort_values(by=['cashtag', 'tweet_counts'], ascending=[True, False], inplace=True)
        file_users = 'output_users.dta'
        df_users.to_stata(file_users, write_index=False)
        logger.info('Users output file is saved')
    except Exception:
        logger.error('Users output file is not saved')
        logger.exception('message')


def download_mentions(mentions_map):
    try:
        df_mentions = pd.DataFrame(mentions_map)
        df_mentions.sort_values(by=['cashtag', 'count'], ascending=[True, False], inplace=True)
        file_mentions = 'output_mentions.dta'
        df_mentions.to_stata(file_mentions, write_index=False)
        logger.info('Mentions output file is saved')
    except Exception:
        logger.error('Mentions output file is not saved')
        logger.exception('message')


def create_logger():
    log_file = 'counts_' + str(datetime.now().strftime('%Y-%m-%d')) + '.log'
    logging.config.fileConfig('log.ini', defaults={'logfilename': log_file})
    return logging.getLogger(__name__)


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
    users_map = []
    mentions_map = []
    hashtags_map = []

    for index, row in df_in.iterrows():
        users = {}
        mentions = {}
        hashtags = {}
        conditions = {
            'cashtag': row['cashtag'],
            'date_from': datetime.strptime(settings.date_from + ' ' + settings.time_from, '%Y-%m-%d %H:%M:%S'),
            'date_to': datetime.strptime(settings.date_to + ' ' + settings.time_to, '%Y-%m-%d %H:%M:%S'),
            'day_status': settings.days,
            'date_joined': settings.date_joined,
            'followers': settings.followers,
            'following': settings.following,
        }
        tweet_list, full_tweet_list = get_tweet_list(conditions)
        with cf.ThreadPoolExecutor(max_workers=32) as executor:
            logger.info('Starting count process for tweet lists')
            future_to_tweet = {executor.submit(load_counts, t): t for t in tweet_list}
            for future in cf.as_completed(future_to_tweet):
                try:
                    t = future.result()
                    df_output.at[index2, 'gvkey'] = str(row['gvkey'])
                    df_output.at[index2, 'database'] = 'twitter'
                    df_output.at[index2, 'day_status'] = t['day_status']
                    df_output.at[index2, 'date'] = str(t['date'])
                    df_output.at[index2, 'cashtag'] = t['cashtag']
                    df_output.at[index2, 'tweets'] = str(t['tweets_count'])
                    df_output.at[index2, 'retweets'] = str(t['retweets'])
                    df_output.at[index2, 'replies'] = str(t['replies'])
                    df_output.at[index2, 'users'] = str(t['users'])
                    df_output.at[index2, 'mentions'] = str(t['mentions'])
                    df_output.at[index2, 'hashtags'] = str(t['hashtags'])
                    index2 += 1
                    for di in t['users_list']:
                        if di['user_id'] in users.keys():
                            users[di['user_id']]['tweet_counts'] = users[di['user_id']]['tweet_counts'] + di['counts']
                        else:
                            users[di['user_id']] = {
                                'twitter_handle': di['twiiter_handle'],
                                'tweet_counts': di['counts'],
                                'date_joined': di['date_joined'],
                                'location': di['location']
                            }
                    for di in t['mentions_list']:
                        if di['mention'] in mentions.keys():
                            mentions[di['mention']] = mentions[di['mention']] + di['counts']
                        else:
                            mentions[di['mention']] = di['counts']
                    if len(t['hashtags_list']) > 0:
                        for di in t['hashtags_list']:
                            if di['hashtag'] in hashtags.keys():
                                hashtags[di['hashtag']] = hashtags[di['hashtag']] + di['counts']
                            else:
                                hashtags[di['hashtag']] = di['counts']
                except Exception:
                    logger.exception('message')
            logger.info('Finished count process for tweet lists')
        for k, v in hashtags.items():
            d = {'gvkey': row['gvkey'], 'cashtag': t['cashtag'],
                'hashtag': k.encode('latin-1', 'ignore').decode('latin-1'), 'count': v}
            hashtags_map.append(d)
        for k, v in mentions.items():
            d = {'gvkey': row['gvkey'], 'cashtag': t['cashtag'], 'mention': k, 'count': v}
            mentions_map.append(d)
        for k, v in users.items():
            d = {'gvkey': row['gvkey'], 'cashtag': t['cashtag'], 'user': k,
                'twitter_handle': str(v['twitter_handle']).encode('latin-1', 'ignore').decode('latin-1'),
                'tweet_counts': v['tweet_counts'],
                'date_joined': str(v['date_joined']),
                'location': str(v['location']).encode('latin-1', 'ignore').decode('latin-1')}
            users_map.append(d)
    df_output.sort_values(by=['cashtag', 'date'], ascending=[True, True], inplace=True)
    df_output.to_stata('output.dta', write_index=False)
    logger.info('Output file is saved')
    if settings.download_hashtags:
        download_hashtags(hashtags_map)
    if settings.download_users:
        download_users(users_map)
    if settings.download_mentions:
        download_mentions(mentions_map)
    
    logger.info('Process succesfuly finished.')
