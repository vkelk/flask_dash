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
from count_helper import get_cashtag_periods, load_counts_v2, get_st_cashtag_periods, load_stocktwits_counts


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


def download_hashtags(hashtags_map, prefix=None):
    try:
        df_hashtags = pd.DataFrame(hashtags_map)
        df_hashtags.sort_values(by=['cashtag', 'count'], ascending=[True, False], inplace=True)
        file_hashtags = str(prefix) + '_output_hashtags.dta'
        df_hashtags.to_stata(file_hashtags, write_index=False)
        logger.info('Hashtags output file is saved')
    except Exception:
        logger.error('Hashtags output file is not saved')
        logger.exception('message')
        raise


def download_users(users_map, prefix=None):
    try:
        df_users = pd.DataFrame(users_map)
        df_users.sort_values(by=['cashtag', 'tweet_counts'], ascending=[True, False], inplace=True)
        file_users = str(prefix) + '_output_users.dta'
        df_users.to_stata(file_users, write_index=False)
        logger.info('Users output file is saved')
    except Exception:
        logger.error('Users output file is not saved')
        logger.exception('message')


def download_mentions(mentions_map, prefix=None):
    try:
        df_mentions = pd.DataFrame(mentions_map)
        df_mentions.sort_values(by=['cashtag', 'count'], ascending=[True, False], inplace=True)
        file_mentions = str(prefix) + '_output_mentions.dta'
        df_mentions.to_stata(file_mentions, write_index=False)
        logger.info('Mentions output file is saved')
    except Exception:
        logger.error('Mentions output file is not saved')
        logger.exception('message')


def create_logger():
    log_file = 'counts_' + str(datetime.now().strftime('%Y-%m-%d')) + '.log'
    logging.config.fileConfig('log.ini', defaults={'logfilename': log_file}, disable_existing_loggers=False)
    return logging.getLogger(__name__)


def populate_df_output(df_output, index2, t):
    df_output.at[index2, 'gvkey'] = str(t['gvkey'])
    df_output.at[index2, 'cashtag'] = t['cashtag']
    df_output.at[index2, 'database'] = t['database']
    df_output.at[index2, 'date'] = str(t['date'])
    df_output.at[index2, 'day_status'] = t['day_status']
    df_output.at[index2, 'tweets'] = str(t['tweets_count'])
    df_output.at[index2, 'retweets'] = str(t['retweets'])
    df_output.at[index2, 'replies'] = str(t['replies'])
    df_output.at[index2, 'favorites'] = str(t['favorites'])
    df_output.at[index2, 'totalTweets'] = str(t['retweets'] + t['tweets_count'])
    df_output.at[index2, 'mentions'] = str(t['mentions'])
    df_output.at[index2, 'hashtags'] = str(t['hashtags'])
    df_output.at[index2, 'users'] = str(t['users'])
    df_output.at[index2, 'user_followers'] = str(t['user_followers'])
    df_output.at[index2, 'tweet_velocity'] = (t['retweets'] + t['tweets_count']) / t['hours']
    df_output.at[index2, 'datetime'] = t['date']
    return df_output


logger = create_logger()


if __name__ == '__main__':
    logger.info('*** Script started')
    parser = argparse.ArgumentParser(description='Processing counts from database.')
    parser.add_argument(
        '-p',
        '--period',
        nargs=1,
        help='Select trading period from options [trading, pre_market, post_market, non_trading, all]',
        required=True
        )
    args = parser.parse_args()
    if args.period[0] not in ['trading', 'pre_market', 'post_market', 'non_trading', 'all']:
        logger.error('Select trading period from options [trading, pre_market, post_market, non_trading, all]')
        parser.print_help()
        print(args.period)
        exit()
    if args.period[0] == 'trading':
        time_from = '09:30:00'
        time_to = '16:00:00'
    elif args.period[0] == 'pre_market':
        time_from = '00:00:00'
        time_to = '09:30:00'
    elif args.period[0] == 'post_market':
        time_from = '16:00:00'
        time_to = '23:59:59'
    elif args.period[0] == 'non_trading':
        time_from = '00:00:00'
        time_to = '23:59:59'
    else:
        time_from = '00:00:00'
        time_to = '23:59:59'

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
            'date_from': datetime.strptime(settings.date_from + ' ' + time_from, '%Y-%m-%d %H:%M:%S'),
            'date_to': datetime.strptime(settings.date_to + ' ' + time_to, '%Y-%m-%d %H:%M:%S'),
            'day_status': args.period[0],
            'date_joined': settings.date_joined,
            'followers': settings.followers,
            'following': settings.following,
        }
        # period_list = get_cashtag_periods(conditions)
        # if len(period_list) == 0:
        #    continue
        # logger.info('Found %s periods for cashtag %s', len(period_list), row['cashtag'])
        # period_items = len(period_list)
        i = 0
        # for i, t in enumerate(get_cashtag_periods(conditions)):
        #     sys.stdout.write("\r[%d]" % i)
        #     sys.stdout.flush()
        #     t['gvkey'] = row['gvkey']
        #     t['cashtag'] = row['cashtag']
        #     df_output = populate_df_output(df_output, index2, t)
        #     index2 += 1
        # if df_output is None or df_output.empty:
        #     logger.warning('Output results are empty. Exiting...')
        #     sys.exit()
        # df_output.sort_values(by=['cashtag', 'datetime'], ascending=[True, True], inplace=True)
        # df_output.to_stata(str(args.period[0]) + '_output.dta', write_index=False)
        # pd.set_option('display.expand_frame_repr', False)
        # print(df_output)
        # logger.info('Process succesfuly finished.')
        # sys.exit()

        # Tweets
        if settings.database in ['twitter', 'all']:
            users_map = []
            mentions_map = []
            hashtags_map = []
            users = {}
            mentions = {}
            hashtags = {}
            with cf.ThreadPoolExecutor(max_workers=16) as executor:
                try:
                    future_to_tweet = {executor.submit(load_counts_v2, t): t for t in get_cashtag_periods(conditions)}
                    logger.info('Starting count process for %s tweet list', row['cashtag'])
                    for future in cf.as_completed(future_to_tweet):
                        i += 1
                        # print(i)
                        # progress = i / period_items * 100
                        sys.stdout.write("\r[%d]" % i)
                        sys.stdout.flush()
                        t = future.result()
                        del future
                        t['gvkey'] = row['gvkey']
                        t['cashtag'] = row['cashtag']
                        t['database'] = 'twitter'
                        df_output = populate_df_output(df_output, index2, t)
                        index2 += 1

                        if settings.download_users and len(t['users_list']) > 0:
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
                        else:
                            t['users_list'] = None

                        if settings.download_mentions and len(t['mentions_list']):
                            for di in t['mentions_list']:
                                if di['mention'] in mentions.keys():
                                    mentions[di['mention']] = mentions[di['mention']] + di['counts']
                                else:
                                    mentions[di['mention']] = di['counts']
                        else:
                            t['mentions_list'] = None

                        if settings.download_hashtags and len(t['hashtags_list']) > 0:
                            for di in t['hashtags_list']:
                                if di['hashtag'] in hashtags.keys():
                                    hashtags[di['hashtag']] = hashtags[di['hashtag']] + di['counts']
                                else:
                                    hashtags[di['hashtag']] = di['counts']
                        else:
                            t['hashtags_list'] = None
                    future_to_tweet = None
                except Exception:
                    logger.exception('message')
                    sys.exit()
                logger.info('Finished count process for tweet lists')

            if settings.download_hashtags and len(hashtags) > 0:
                for k, v in hashtags.items():
                    d = {'gvkey': row['gvkey'], 'cashtag': row['cashtag'],
                         'hashtag': k.encode('latin-1', 'ignore').decode('latin-1'), 'count': v}
                    hashtags_map.append(d)
                download_hashtags(hashtags_map, 'twitter')
                del hashtags_map

            if settings.download_mentions and len(mentions) > 0:
                for k, v in mentions.items():
                    d = {'gvkey': row['gvkey'], 'cashtag': row['cashtag'], 'mention': k, 'count': v}
                    mentions_map.append(d)
                download_mentions(mentions_map, 'twitter')
                del mentions_map

            if settings.download_users and len(users) > 0:
                for k, v in users.items():
                    d = {'gvkey': row['gvkey'], 'cashtag': row['cashtag'], 'user': k,
                         'twitter_handle': str(v['twitter_handle']).encode('latin-1', 'ignore').decode('latin-1'),
                         'tweet_counts': v['tweet_counts'],
                         'date_joined': str(v['date_joined']),
                         'location': str(v['location']).encode('latin-1', 'ignore').decode('latin-1')}
                    users_map.append(d)
                download_users(users_map, 'twitter')
                del users_map

        # Stocktwits
        if settings.database in ['stocktwits', 'all']:
            users_map = []
            mentions_map = []
            hashtags_map = []
            users = {}
            mentions = {}
            hashtags = {}
            with cf.ThreadPoolExecutor(max_workers=16) as executor:
                try:
                    future_to_tweet = {executor.submit(load_stocktwits_counts, t): t for t in get_st_cashtag_periods(conditions)}
                    logger.info('Starting count process for %s ideas list', row['cashtag'])
                    for future in cf.as_completed(future_to_tweet):
                        i += 1
                        sys.stdout.write("\r[%d]" % i)
                        sys.stdout.flush()
                        t = future.result()
                        del future
                        t['gvkey'] = row['gvkey']
                        t['cashtag'] = row['cashtag']
                        t['database'] = 'stocktwits'
                        df_output = populate_df_output(df_output, index2, t)
                        index2 += 1

                        if settings.download_users and len(t['users_list']) > 0:
                            for di in t['users_list']:
                                if di['user_id'] in users.keys():
                                    users[di['user_id']]['tweet_counts'] = users[di['user_id']]['tweet_counts'] + di['counts']
                                else:
                                    users[di['user_id']] = {
                                        'user_handle': di['user_handle'],
                                        'tweet_counts': di['counts'],
                                        'date_joined': di['date_joined'],
                                        'location': di['location']
                                    }
                        else:
                            t['users_list'] = None

                        if settings.download_hashtags and len(t['hashtags_list']) > 0:
                            for di in t['hashtags_list']:
                                if di['hashtag'] in hashtags.keys():
                                    hashtags[di['hashtag']] = hashtags[di['hashtag']] + di['counts']
                                else:
                                    hashtags[di['hashtag']] = di['counts']
                        else:
                            t['hashtags_list'] = None
                    future_to_tweet = None
                except Exception:
                    logger.exception('message')
                logger.info('Finished count process for stocktwits lists')

            if settings.download_hashtags and len(hashtags) > 0:
                for k, v in hashtags.items():
                    d = {'gvkey': row['gvkey'], 'cashtag': row['cashtag'],
                         'hashtag': k.encode('latin-1', 'ignore').decode('latin-1'), 'count': v}
                    hashtags_map.append(d)
                download_hashtags(hashtags_map, 'stocktwits')
                del hashtags_map

            if settings.download_users and len(users) > 0:
                for k, v in users.items():
                    d = {'gvkey': row['gvkey'], 'cashtag': row['cashtag'], 'user': k,
                         'user_handle': str(v['user_handle']).encode('latin-1', 'ignore').decode('latin-1'),
                         'tweet_counts': v['tweet_counts'],
                         'date_joined': str(v['date_joined']),
                         'location': str(v['location']).encode('latin-1', 'ignore').decode('latin-1')}
                    users_map.append(d)
                download_users(users_map, 'stocktwits')
                del users_map

    if df_output is None or df_output.empty:
        logger.warning('Output results are empty. Exiting...')
        sys.exit()
    df_output['datetime'] = pd.to_datetime(df_output['datetime'])
    df_output.index = pd.DatetimeIndex(df_output['datetime']).floor('D')
    all_days = pd.date_range(df_output.index.min(), df_output.index.max(), freq='D')
    df_output.sort_values(by=['cashtag', 'datetime'], ascending=[True, True], inplace=True)
    df_output.loc[all_days]
    df_output.drop(columns=['datetime'])
    df_output.to_stata('full_' + str(args.period[0]) + '_output.dta', write_index=False)
    pd.set_option('display.expand_frame_repr', False)
    print(df_output)
    logger.info('Output file is saved')
    logger.info('Process succesfuly finished.')
