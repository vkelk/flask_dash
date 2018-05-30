from datetime import datetime, timedelta, date
from dateutil import tz
import os
import pandas as pd
from pprint import pprint
import uuid
from flask import current_app, render_template, request, session
from flask_login import current_user, login_required
from sqlalchemy import or_, and_, text, func
from werkzeug.utils import secure_filename, CombinedMultiDict
from application import db
from application.project import project
from ..models import Project, Dataset, TradingDays
from ..forms import CountsFileForm
from ..helpers import slugify
from application.fintweet.models import Tweet, TweetCashtag, TweetHashtag, TweetMention, User, UserCount, mvCashtags


ZONE_NY = tz.gettz('America/New_York')
ZONE_UTC = tz.gettz('UTC')


def convert_date(input_dt, zone_from=ZONE_NY, zone_to=ZONE_UTC):
    utc_datetime = datetime.strptime(input_dt, '%Y-%m-%d %H:%M:%S')
    # Tell the datetime object that it's in NY time zone since datetime objects are 'naive' by default
    utc_datetime = utc_datetime.replace(tzinfo=zone_from)
    return utc_datetime.astimezone(zone_to)


def get_period(date_input, period_type=0):
    if isinstance(date_input, date):
        date_input = str(date_input)
    if period_type == 0:
        period_start = date_input + ' 09:30:00'
        period_end = date_input + ' 16:00:00'
    elif period_type == -1:
        period_start = date_input + ' 00:00:00'
        period_end = date_input + ' 09:30:00'
    elif period_type == 1:
        period_start = date_input + ' 16:00:00'
        period_end = (datetime.strptime(date_input, '%Y-%m-%d') + timedelta(days=1)).strftime("%Y-%m-%d %H:%M:%S")
    return {'start': convert_date(period_start), 'end': convert_date(period_end)}


def get_tweets_in_period(c_tag, date_input, period_type=0, date_joined=None, followers=None, following=None):
    period = get_period(date_input, period_type)
    if period_type in (0, -1):
        filter_period = and_(
            Tweet.date == period['start'].date(),
            Tweet.time >= period['start'].time(),
            Tweet.time < period['end'].time())
    elif period_type == 1:
        start = and_(Tweet.date == period['start'].date(), Tweet.time >= period['start'].time()).self_group()
        end = and_(Tweet.date == period['end'].date(), Tweet.time < period['end'].time()).self_group()
        filter_period = or_(start, end)
    if date_joined:
        tweets = db.session.query(TweetCashtag.tweet_id).join(Tweet).join(User) \
            .filter(User.date_joined >= date_joined) \
            .filter(TweetCashtag.cashtags == c_tag) \
            .filter(filter_period)
    elif followers:
        tweets = db.session.query(TweetCashtag.tweet_id) \
            .join(Tweet).join(UserCount, Tweet.user_id == UserCount.user_id) \
            .filter(UserCount.follower >= followers) \
            .filter(TweetCashtag.cashtags == c_tag) \
            .filter(filter_period)
    elif following:
        tweets = db.session.query(TweetCashtag.tweet_id) \
            .join(Tweet).join(UserCount, Tweet.user_id == UserCount.user_id) \
            .filter(UserCount.following >= following) \
            .filter(TweetCashtag.cashtags == c_tag) \
            .filter(filter_period)
    else:
        tweets = db.session.query(TweetCashtag.tweet_id).join(Tweet) \
            .filter(TweetCashtag.cashtags == c_tag) \
            .filter(filter_period)
    return [t[0] for t in tweets.all()]


def get_users_count(tweet_list):
    q = db.session.query(mvCashtags.user_id).filter(mvCashtags.tweet_id.in_(tweet_list)).group_by(mvCashtags.user_id)
    return len(q.all())


def get_retweet_count(tweet_list):
    true_list = ['1', 'True', 'true']
    q = db.session.query(func.count(mvCashtags.tweet_id)) \
        .join(Tweet, mvCashtags.tweet_id == Tweet.tweet_id) \
        .filter(mvCashtags.tweet_id.in_(tweet_list)) \
        .filter(Tweet.retweet_status.in_(true_list))
    return q.scalar()


def get_hashtag_count(tweet_list):
    q = db.session.query(func.count(TweetHashtag.hashtags)).filter(TweetHashtag.tweet_id.in_(tweet_list))
    return q.scalar()


def get_replys_count(tweet_list):
    q = db.session.query(func.count(Tweet.tweet_id)).filter(Tweet.tweet_id.in_(tweet_list)).filter(Tweet.reply_to > 0)
    return q.scalar()


def get_mentions_count(tweet_list):
    q = db.session.query(func.count(TweetMention.user_id)).filter(TweetMention.tweet_id.in_(tweet_list))
    return q.scalar()


def dataframe_from_file(filename):
    ext = os.path.splitext(filename)[1]
    if ext in ['.xls', '.xlsx']:
        df = pd.read_excel(filename)
        df.columns = [slugify(col) for col in df.columns]
        df["status"] = ""
        return df
    # TODO: Create import from CSV
    return None


def get_all_tweet_ids(cashtag, date_from, date_to, dates='all', date_joined=None, followers=None, following=None):
    date_delta = date_to - date_from
    if dates in ['trading', 'non-trading']:
        trading_days = db.session.query(TradingDays.date) \
            .filter(TradingDays.is_trading == True) \
            .filter(TradingDays.date.between(date_from, date_to))
        days_list = [d[0] for d in trading_days.all()]
    tweets = {'open': [], 'pre': [], 'post': []}
    for i in range(date_delta.days + 1):
        date_input = (date_from + timedelta(days=i))
        if dates in ['trading', 'non-trading']:
            if dates == 'trading' and date_input in days_list:
                open_period = get_tweets_in_period(
                    cashtag, date_input, 0,
                    date_joined=date_joined,
                    followers=followers,
                    following=following)
                tweets['open'].extend(open_period)
                pre_open_period = get_tweets_in_period(
                    cashtag, date_input, -1,
                    date_joined=date_joined,
                    followers=followers,
                    following=following)
                tweets['pre'].extend(pre_open_period)
                post_open_period = get_tweets_in_period(
                    cashtag, date_input, 1,
                    date_joined=date_joined,
                    followers=followers,
                    following=following)
                tweets['post'].extend(post_open_period)
            elif dates == 'non-trading' and date_input not in days_list:
                open_period = get_tweets_in_period(
                    cashtag, date_input, 0,
                    date_joined=date_joined,
                    followers=followers,
                    following=following)
                tweets['open'].extend(open_period)
                pre_open_period = get_tweets_in_period(
                    cashtag, date_input, -1,
                    date_joined=date_joined,
                    followers=followers,
                    following=following)
                tweets['pre'].extend(pre_open_period)
                post_open_period = get_tweets_in_period(
                    cashtag, date_input, 1,
                    date_joined=date_joined,
                    followers=followers,
                    following=following)
                tweets['post'].extend(post_open_period)
        elif dates == 'all':
            open_period = get_tweets_in_period(
                cashtag, date_input, 0,
                date_joined=date_joined,
                followers=followers,
                following=following)
            tweets['open'].extend(open_period)
            pre_open_period = get_tweets_in_period(
                cashtag, date_input, -1,
                date_joined=date_joined,
                followers=followers,
                following=following)
            tweets['pre'].extend(pre_open_period)
            post_open_period = get_tweets_in_period(
                cashtag, date_input, 1,
                date_joined=date_joined,
                followers=followers,
                following=following)
            tweets['post'].extend(post_open_period)
    return tweets


def get_tweet_list(c):
    '''
    Input dict should follow this format
    conditions = {
        'cashtag': string,
        'date_from': date_string,
        'date_to': date_string,
        'time_from': time_string,
        'time_to': time_string,
        'day_status': string: 'all', 'trading', 'non-trading'
        'date_joined': date_string,
        'followers': integer,
        'following': integer,
    }
    '''
    date_delta = c['date_to'] - c['date_from']
    trading_days = db.session.query(TradingDays.date) \
        .filter(TradingDays.is_trading == True) \
        .filter(TradingDays.date.between(c['date_from'], c['date_to']))
    days_list = [d[0] for d in trading_days.all()]
    result = []
    for i in range(date_delta.days + 1):
        date_input = (c['date_from'] + timedelta(days=i))
        c['date_input'] = date_input
        day = {}
        if c['day_status'] in ['trading', 'all'] and date_input in days_list:
            day['date'] = date_input
            day['day_status'] = 'trading'
            day['cashtag'] = c['cashtag']
            day['tweet_ids'] = get_tweet_ids(c)
            day['tweets_count'] = len(day['tweet_ids'])
            result.append(day)
        elif c['day_status'] in ['non-trading', 'all'] and date_input not in days_list:
            day['date'] = date_input
            day['day_status'] = 'non-trading'
            day['cashtag'] = c['cashtag']
            day['tweet_ids'] = get_tweet_ids(c)
            day['tweets_count'] = len(day['tweet_ids'])
            result.append(day)
    return result


def get_tweet_ids(c):
    date_input = c['date_input'].strftime("%Y-%m-%d")
    if c['date_from'] == c['date_to']:
        datetime_start = convert_date(c['date_from'].strftime("%Y-%m-%d") + ' ' + c['time_from'].strftime("%H:%M:%S"))
        datetime_end = convert_date(c['date_to'].strftime("%Y-%m-%d") + ' ' + c['time_to'].strftime("%H:%M:%S"))
    elif c['date_from'].strftime("%Y-%m-%d") == date_input:
        datetime_start = convert_date(c['date_from'].strftime("%Y-%m-%d") + ' ' + c['time_from'].strftime("%H:%M:%S"))
        datetime_end = convert_date(c['date_from'].strftime("%Y-%m-%d") + ' ' + '23:59:59')
    elif c['date_to'].strftime("%Y-%m-%d") == date_input:
        datetime_start = convert_date(c['date_to'].strftime("%Y-%m-%d") + ' ' + '00:00:00')
        datetime_end = convert_date(c['date_to'].strftime("%Y-%m-%d") + ' ' + c['time_to'].strftime("%H:%M:%S"))
    else:
        datetime_start = convert_date(date_input + ' ' + '00:00:00')
        datetime_end = convert_date(date_input + ' ' + '23:59:59')
    filter_period = text(
        "fintweet.tweet.date + fintweet.tweet.time between timestamp '"
        + str(datetime_start)
        + "' and timestamp '"
        + str(datetime_end)
        + "'")
    tweets = db.session.query(mvCashtags.tweet_id) \
        .filter(mvCashtags.cashtags == c['cashtag']) \
        .filter(mvCashtags.datetime.between(datetime_start, datetime_end))
    if 'date_joined' in c and c['date_joined']:
        tweets = tweets.join(User, mvCashtags.user_id == User.user_id).filter(User.date_joined >= c['date_joined'])
    if 'following' in c and c['following']:
        tweets = tweets.join(UserCount, mvCashtags.user_id == UserCount.user_id) \
            .filter(UserCount.following >= c['following'])
    if 'followers' in c and c['followers']:
        tweets = tweets.join(UserCount, mvCashtags.user_id == UserCount.user_id) \
            .filter(UserCount.follower >= c['followers'])
    return [t[0] for t in tweets.all()]


@project.route('/counts_upload', methods=['GET', 'POST'])
@login_required
def counts_upload():
    project = Project.query.filter(Project.account_id == current_user.get_id()) \
        .filter(Project.active == True).first()
    if project:
        session['active_project'] = project.uuid
    datasets = Dataset.query.all()
    form = CountsFileForm(CombinedMultiDict((request.files, request.form)))
    if request.method == 'POST' and form.validate_on_submit():
        if form.create_study.data:
            file_input = secure_filename(
                str(uuid.uuid4()) +
                os.path.splitext(form.file_input.data.filename)[-1])
            form.file_input.data.save(
                os.path.join(current_app.config['UPLOAD_FOLDER'], file_input))
            form.file_name.data = file_input
            df_in = dataframe_from_file(
                os.path.join(current_app.config['UPLOAD_FOLDER'], form.file_name.data))
            if df_in is None or df_in.empty:
                return render_template(
                    'project/counts_upload.html',
                    form=form,
                    project=project,
                    df_in=df_in.to_html(classes='table table-striped'))
            df_output = pd.DataFrame()
            index2 = 0
            for index, row in df_in.iterrows():
                conditions = {
                    'cashtag': row['cashtag'],
                    'date_from': form.date_start.data,
                    'date_to': form.date_end.data,
                    'time_from': form.time_start.data,
                    'time_to': form.time_end.data,
                    'day_status': form.days_status.data,
                    'date_joined': form.date_joining.data,
                    'followers': form.followers.data,
                    'following': form.following.data,
                }
                tweet_list = get_tweet_list(conditions)
                for t in tweet_list:
                    df_output.at[index2, 'gvkey'] = str(row['gvkey'])
                    df_output.at[index2, 'database'] = 'twitter'
                    df_output.at[index2, 'day_status'] = t['day_status']
                    df_output.at[index2, 'date'] = str(t['date'])
                    df_output.at[index2, 'cashtag'] = row['cashtag']
                    df_output.at[index2, 'tweets'] = str(t['tweets_count'])
                    df_output.at[index2, 'retweets'] = str(get_retweet_count(t['tweet_ids']))
                    df_output.at[index2, 'replies'] = str(get_replys_count(t['tweet_ids']))
                    df_output.at[index2, 'users'] = str(get_users_count(t['tweet_ids']))
                    df_output.at[index2, 'mentions'] = str(get_mentions_count(t['tweet_ids']))
                    df_output.at[index2, 'hashtags'] = str(get_hashtag_count(t['tweet_ids']))
                    index2 += 1
            file_output = 'output_' + file_input
            file_output = file_output.replace('.xlsx', '.dta')
            df_output.to_stata(os.path.join(current_app.config['UPLOAD_FOLDER'], file_output), write_index=False)
            form.output_file.data = file_output
            project.file_output = file_output
            return render_template(
                'project/counts_upload.html',
                form=form,
                project=project,
                df_in=df_output.to_html(classes='table table-striped')
                )

    if len(form.errors) > 0:
        pprint(form.errors)
    return render_template(
        'project/counts_upload.html', form=form, project=project)
