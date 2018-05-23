from datetime import datetime, timedelta, date
from dateutil import tz
import os
import pandas as pd
from pprint import pprint
import uuid
from flask import current_app, render_template, request, session
from flask_login import current_user, login_required
from sqlalchemy import or_, and_
from werkzeug.utils import secure_filename, CombinedMultiDict
from application import db, config
from application.project import project
from ..models import Project, Dataset, TradingDays
from ..forms import CountsFileForm
from ..helpers import slugify
from application.fintweet.models import Tweet, TweetCashtag, TweetHashtag, TweetMention


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


def get_tweets_in_period(c_tag, date_input, period_type=0):
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
    tweets = db.session.query(TweetCashtag.tweet_id).join(Tweet) \
        .filter(TweetCashtag.cashtags == c_tag) \
        .filter(filter_period)
    return [t[0] for t in tweets.all()]


def get_users_count(tweet_list):
    q = db.session.query(Tweet.user_id).filter(Tweet.tweet_id.in_(tweet_list)).group_by(Tweet.user_id)
    return q.count()


def get_retweet_count(tweet_list):
    true_list = ['1', 'True', 'true']
    q = db.session.query(Tweet.tweet_id) \
        .filter(Tweet.tweet_id.in_(tweet_list)) \
        .filter(Tweet.retweet_status.in_(true_list))
    return q.count()


def get_hashtag_count(tweet_list):
    q = db.session.query(TweetHashtag.hashtags).filter(TweetHashtag.tweet_id.in_(tweet_list))
    return q.count()


def get_replys_count(tweet_list):
    q = db.session.query(Tweet.tweet_id).filter(Tweet.tweet_id.in_(tweet_list)).filter(Tweet.reply_to > 0)
    return q.count()


def get_mentions_count(tweet_list):
    q = db.session.query(TweetMention.user_id).filter(TweetMention.tweet_id.in_(tweet_list))
    return q.count()


def dataframe_from_file(filename):
    ext = os.path.splitext(filename)[1]
    if ext in ['.xls', '.xlsx']:
        df = pd.read_excel(filename)
        df.columns = [slugify(col) for col in df.columns]
        df["status"] = ""
        # df["trading tweet count"] = ""
        # df["trading user count"] = ""
        # df["trading retweet count"] = ""
        # df["trading hashtag count"] = ""
        # df["trading reply count"] = ""
        # df["trading mention count"] = ""
        # df["pre-trading tweet count"] = ""
        # df["pre-trading user count"] = ""
        # df["pre-trading retweet count"] = ""
        # df["pre-trading hashtag count"] = ""
        # df["pre-trading reply count"] = ""
        # df["pre-trading mention count"] = ""
        # df["post-trading tweet count"] = ""
        # df["post-trading user count"] = ""
        # df["post-trading retweet count"] = ""
        # df["post-trading hashtag count"] = ""
        # df["post-trading reply count"] = ""
        # df["post-trading mention count"] = ""
        return df
    # TODO: Create import from CSV
    return None


def get_all_tweet_ids(cashtag, date_from, date_to, dates='all'):
    date_delta = date_to - date_from
    if dates == 'trading':
        trading_days = db.session.query(TradingDays.date) \
            .filter(TradingDays.is_trading == True) \
            .filter(TradingDays.date.between(date_from, date_to))
        days_list = [d[0] for d in trading_days.all()]
    tweets = {'open': [], 'pre': [], 'post': []}
    for i in range(date_delta.days + 1):
        date_input = (date_from + timedelta(days=i))
        if dates == 'trading' and date_input in days_list:
            open_period = get_tweets_in_period(cashtag, date_input, 0)
            tweets['open'].extend(open_period)
            pre_open_period = get_tweets_in_period(cashtag, date_input, -1)
            tweets['pre'].extend(pre_open_period)
            post_open_period = get_tweets_in_period(cashtag, date_input, 1)
            tweets['post'].extend(post_open_period)
        elif dates == 'all':
            open_period = get_tweets_in_period(cashtag, date_input, 0)
            tweets['open'].extend(open_period)
            pre_open_period = get_tweets_in_period(cashtag, date_input, -1)
            tweets['pre'].extend(pre_open_period)
            post_open_period = get_tweets_in_period(cashtag, date_input, 1)
            tweets['post'].extend(post_open_period)
    return tweets


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
                os.path.join(config.base_config.UPLOAD_FOLDER, file_input))
            form.file_name.data = file_input
            df_in = dataframe_from_file(
                os.path.join(config.base_config.UPLOAD_FOLDER, form.file_name.data))
            if df_in is None or df_in.empty:
                return render_template(
                    'project/counts_upload.html',
                    form=form,
                    project=project,
                    df_in=df_in.to_html(classes='table table-striped'))
            for index, row in df_in.iterrows():
                date_from = form.date_start.data
                date_to = form.date_end.data
                time_from = form.time_start.data
                time_to = form.time_end.data
                cashtag = row['cashtag']
                tweets = get_all_tweet_ids(cashtag, date_from, date_to, form.days_status.data)
                df_in.at[index, 'opent tweets'] = str(len(tweets['open']))
                df_in.at[index, 'opent users'] = str(get_users_count(tweets['open']))
                df_in.at[index, 'opent retweets'] = str(get_retweet_count(tweets['open']))
                df_in.at[index, 'opent hashtags'] = str(get_hashtag_count(tweets['open']))
                df_in.at[index, 'opent replys'] = str(get_replys_count(tweets['open']))
                df_in.at[index, 'opent mentions'] = str(get_mentions_count(tweets['open']))
                df_in.at[index, 'pret tweets'] = str(len(tweets['pre']))
                df_in.at[index, 'pret users'] = str(get_users_count(tweets['pre']))
                df_in.at[index, 'pret retweets'] = str(get_retweet_count(tweets['pre']))
                df_in.at[index, 'pret hashtags'] = str(get_hashtag_count(tweets['pre']))
                df_in.at[index, 'pret replys'] = str(get_replys_count(tweets['pre']))
                df_in.at[index, 'pret mentions'] = str(get_mentions_count(tweets['pre']))
                df_in.at[index, 'postt tc'] = str(len(tweets['post']))
                df_in.at[index, 'postt users'] = str(get_users_count(tweets['post']))
                df_in.at[index, 'postt retweets'] = str(get_retweet_count(tweets['post']))
                df_in.at[index, 'postt hashtags'] = str(get_hashtag_count(tweets['post']))
                df_in.at[index, 'postt replys'] = str(get_replys_count(tweets['post']))
                df_in.at[index, 'postt mentions'] = str(get_mentions_count(tweets['post']))

            file_output = 'output_' + file_input
            file_output = file_output.replace('.xlsx', '.dta')
            df_in.to_stata(os.path.join(config.base_config.UPLOAD_FOLDER, file_output), write_index=False)
            form.output_file.data = file_output
            project.file_output = file_output
            return render_template(
                'project/counts_upload.html',
                form=form,
                project=project,
                df_in=df_in.to_html(classes='table table-striped'))

    if len(form.errors) > 0:
        pprint(form.errors)
    return render_template(
        'project/counts_upload.html', form=form, project=project)
