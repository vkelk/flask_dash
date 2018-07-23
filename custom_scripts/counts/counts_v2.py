import concurrent.futures as cf
from datetime import datetime, timedelta
import logging
import logging.config
import os
import re
import sys

import pandas as pd

import settings
from count_helper import get_cashtag_periods, load_counts


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
        raise


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
    logging.config.fileConfig('log.ini', defaults={'logfilename': log_file}, disable_existing_loggers=False)
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
    if settings.frequency is None:
        logger.error('Could not read frequency setting. Will setup for default 6 hours ')
        settings.frequency = 6
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
        period_list = get_cashtag_periods(conditions)
        with cf.ThreadPoolExecutor(max_workers=48) as executor:
            logger.info('Starting count process for %s tweet list', row['cashtag'])
            try:
                future_to_tweet = {executor.submit(load_counts, t): t for t in period_list}
                for future in cf.as_completed(future_to_tweet):
                    t = future.result()
                    logger.debug('Got full results for %s %s', t['cashtag'], t['date'])
                    df_output.at[index2, 'gvkey'] = str(row['gvkey'])
                    df_output.at[index2, 'cashtag'] = t['cashtag']
                    df_output.at[index2, 'database'] = 'twitter'
                    df_output.at[index2, 'date'] = str(t['date'].date())
                    df_output.at[index2, 'day_status'] = t['day_status']
                    if settings.frequency:
                        period_str = str(t['date'].time()) + ' - ' \
                            + str((t['date'] + timedelta(hours=settings.frequency)).time())
                        df_output.at[index2, 'time'] = period_str
                    df_output.at[index2, 'tweets'] = str(t['tweets_count'])
                    df_output.at[index2, 'retweets'] = str(t['retweets'])
                    df_output.at[index2, 'replies'] = str(t['replies'])
                    df_output.at[index2, 'favorites'] = str(t['favorites'])
                    df_output.at[index2, 'totalTweets'] = str(t['retweets'] + t['tweets_count'])
                    df_output.at[index2, 'mentions'] = str(t['mentions'])
                    df_output.at[index2, 'hashtags'] = str(t['hashtags'])
                    df_output.at[index2, 'users'] = str(t['users'])
                    df_output.at[index2, 'user_followers'] = str(t['user_followers'])
                    df_output.at[index2, 'tweet_velocity'] = (t['retweets'] + t['tweets_count']) / (settings.frequency * 60)
                    # df_output.at[index2, 'users_retweet'] = str(t['users_retweet'])
                    # df_output.at[index2, 'users_total'] = str((t['users']) + t['users_retweet'])
                    df_output.at[index2, 'datetime'] = str(t['date'])
                    index2 += 1

                    if settings.download_users:
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

                    if settings.download_mentions:
                        for di in t['mentions_list']:
                            if di['mention'] in mentions.keys():
                                mentions[di['mention']] = mentions[di['mention']] + di['counts']
                            else:
                                mentions[di['mention']] = di['counts']

                    if settings.download_hashtags and len(t['hashtags_list']) > 0:
                        for di in t['hashtags_list']:
                            if di['hashtag'] in hashtags.keys():
                                hashtags[di['hashtag']] = hashtags[di['hashtag']] + di['counts']
                            else:
                                hashtags[di['hashtag']] = di['counts']
            except Exception:
                logger.exception('message')
                sys.exit()
            logger.info('Finished count process for tweet lists')

        if settings.download_hashtags:
            for k, v in hashtags.items():
                d = {'gvkey': row['gvkey'], 'cashtag': t['cashtag'],
                     'hashtag': k.encode('latin-1', 'ignore').decode('latin-1'), 'count': v}
                hashtags_map.append(d)

        if settings.download_mentions:
            for k, v in mentions.items():
                d = {'gvkey': row['gvkey'], 'cashtag': t['cashtag'], 'mention': k, 'count': v}
                mentions_map.append(d)

        if settings.download_users:
            for k, v in users.items():
                d = {'gvkey': row['gvkey'], 'cashtag': t['cashtag'], 'user': k,
                     'twitter_handle': str(v['twitter_handle']).encode('latin-1', 'ignore').decode('latin-1'),
                     'tweet_counts': v['tweet_counts'],
                     'date_joined': str(v['date_joined']),
                     'location': str(v['location']).encode('latin-1', 'ignore').decode('latin-1')}
                users_map.append(d)
    df_output.sort_values(by=['cashtag', 'datetime'], ascending=[True, True], inplace=True)
    df_output.to_stata('output.dta', write_index=False)
    pd.set_option('display.expand_frame_repr', False)
    print(df_output)
    logger.info('Output file is saved')
    # print(df_output)
    if settings.download_hashtags:
        download_hashtags(hashtags_map)
    if settings.download_users:
        download_users(users_map)
    if settings.download_mentions:
        download_mentions(mentions_map)

    logger.info('Process succesfuly finished.')
