from datetime import datetime, timedelta
from dateutil import tz
import hashlib
import os
import pandas as pd
from pprint import pprint
import uuid
from flask import render_template, request, session
from flask_login import current_user, login_required
from sqlalchemy import or_, and_
from werkzeug.utils import secure_filename, CombinedMultiDict
from application import db
from application.config import Configuration
from application.project import project
from application.project.models import Project, Dataset
from application.project.forms import CountsFileForm
from application.project.helpers import slugify
from application.fintweet.models import Tweet, TweetCashtag, TweetHashtag, TweetMention


ZONE_NY = tz.gettz('America/New_York')
ZONE_UTC = tz.gettz('UTC')


def convert_date(input_dt, zone_from=ZONE_NY, zone_to=ZONE_UTC):
    utc_datetime = datetime.strptime(input_dt, '%Y-%m-%d %H:%M:%S')
    # Tell the datetime object that it's in NY time zone since datetime objects are 'naive' by default
    utc_datetime = utc_datetime.replace(tzinfo=zone_from)
    return utc_datetime.astimezone(zone_to)


def get_period(date, period_type=0):
    if period_type == 0:
        period_start = date + ' 09:30:00'
        period_end = date + ' 16:00:00'
    elif period_type == -1:
        period_start = date + ' 00:00:00'
        period_end = date + ' 09:30:00'
    elif period_type == 1:
        period_start = date + ' 16:00:00'
        period_end = (datetime.strptime(date, '%Y-%m-%d') + timedelta(days=1)).strftime("%Y-%m-%d %H:%M:%S")
    return {'start': convert_date(period_start), 'end': convert_date(period_end)}


def get_tweets_in_period(c_tag, date, period_type=0):
    period = get_period(date, period_type)
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
    q = db.session.query(Tweet.tweet_id).filter(Tweet.tweet_id.in_(tweet_list)).filter(Tweet.retweet_status is True)
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
        df["trading tweet count"] = ""
        df["trading user count"] = ""
        df["trading retweet count"] = ""
        df["trading hashtag count"] = ""
        df["trading reply count"] = ""
        df["trading mention count"] = ""
        df["pre-trading tweet count"] = ""
        df["pre-trading user count"] = ""
        df["pre-trading retweet count"] = ""
        df["pre-trading hashtag count"] = ""
        df["pre-trading reply count"] = ""
        df["pre-trading mention count"] = ""
        df["post-trading tweet count"] = ""
        df["post-trading user count"] = ""
        df["post-trading retweet count"] = ""
        df["post-trading hashtag count"] = ""
        df["post-trading reply count"] = ""
        df["post-trading mention count"] = ""
        return df
    # TODO: Create import from CSV
    return None


def get_all_counts(cashtag, date_from, date_to, dates='all'):
    date_delta = date_to - date_to
    for i in range(date_delta.days + 1):
        print(date_from + timedelta(days=i))


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
                os.path.join(Configuration.UPLOAD_FOLDER, file_input))
            form.file_name.data = file_input
            df_in = dataframe_from_file(
                os.path.join(Configuration.UPLOAD_FOLDER, form.file_name.data))
            if df_in.empty:
                return render_template(
                    'project/counts_upload.html',
                    form=form,
                    project=project,
                    df_in=df_in.to_html(classes='table table-striped'))
            for index, row in df_in.iterrows():
                date_from = row['date_from'].to_pydatetime()
                date_to = row['date_to'].to_pydatetime()
                cashtag = row['cashtag']
                get_all_counts(cashtag, date_from, date_to)

                return render_template(
                    'project/counts_upload.html',
                    form=form,
                    project=project,
                    df_in=df_in.to_html(classes='table table-striped'))

                estimation_window = {
                    'pre_end': date_estwin_pre_end,
                    'pre_start': date_estwin_pre_start,
                    'post_start': date_estwin_post_start,
                    'post_end': date_estwin_post_end
                }
                result_list = get_data_from_query(row['cashtag'],
                                                  estimation_window, form.dataset.data)
                if len(result_list) > 0:
                    check_string = project.uuid + 'cashtag' + row['cashtag'] + str(
                        event_window['date'])
                    event_uuid = hashlib.md5(
                        check_string.encode('utf-8')).hexdigest()
                    event = Event.query.filter(
                        Event.uuid == event_uuid).first()
                    if not event:
                        event = Event(project.uuid, form.dataset.data)
                        event.type = 'cashtag'
                        event.text = row['cashtag']
                        event.event_date = event_window['date']
                        event.uuid = hashlib.md5(
                            check_string.encode('utf-8')).hexdigest()
                        event.event_start = event_window['start']
                        event.event_end = event_window['end']
                        event.event_pre_start = estimation_window['pre_start']
                        event.event_pre_end = estimation_window['pre_end']
                        event.event_post_start = estimation_window[
                            'post_start']
                        event.event_post_end = estimation_window['post_end']
                        event.days_pre = form.days_pre_event.data
                        event.days_post = form.days_post_event.data
                        event.days_estimation = form.days_estimation.data
                        event.days_grace = form.days_grace.data
                        db.session.add(event)
                        db.session.commit()

                    df_full = pd.DataFrame.from_records(
                        result_list, index='date')
                    df_full.fillna(0)
                    df_pre_est = df_full.loc[:estimation_window['pre_end']
                                             .date()]
                    df_pre_est.fillna(0)
                    df_event = df_full.loc[event_window['start'].date():
                                           event_window['end'].date()]
                    df_event.fillna(0)
                    df_post_est = df_full.loc[event.event_post_start:]
                    df_post_est.fillna(0)
                    df_full.truncate()

                    event_stats = EventStats.query.filter(
                        EventStats.uuid == event_uuid).first()
                    if not event_stats:
                        event_stats = EventStats(event.uuid)

                    event_stats.event_total = int(df_event['count'].sum())
                    event_stats.event_median = df_event[
                        'count'].median() if event_stats.event_total > 0 else 0
                    event_stats.event_mean = df_event[
                        'count'].mean() if event_stats.event_total > 0 else 0
                    # event_stats.event_std = df_event['count'].std() if event_stats.event_total > 1 else 0
                    df_event = None
                    event_sent = count_sentiment(event.text, event.event_start,
                                                 event.event_end)
                    event_stats.event_bullish = event_sent['bullish']
                    event_stats.event_bearish = event_sent['bearish']
                    event_stats.event_positive = event_sent['positive']
                    event_stats.event_negative = event_sent['negative']
                    event_users = count_users_sentimet(
                        event.text, event.event_start, event.event_end)
                    event_stats.users_event = event_users['users']
                    event_stats.users_event_bullish = event_users['bullish']
                    event_stats.users_event_bearish = event_users['bearish']

                    event_stats.pre_total = int(df_pre_est['count'].sum())
                    event_stats.pre_median = df_pre_est[
                        'count'].median() if event_stats.pre_total > 0 else 0
                    event_stats.pre_mean = df_pre_est[
                        'count'].mean() if event_stats.pre_total > 0 else 0
                    # event_stats.pre_std = df_pre_est['count'].std() if event_stats.pre_total > 1 else 0
                    df_pre_est = None
                    event_sent = count_sentiment(
                        event.text, event.event_pre_start, event.event_pre_end)
                    event_stats.pre_bullish = event_sent['bullish']
                    event_stats.pre_bearish = event_sent['bearish']
                    event_stats.pre_positive = event_sent['positive']
                    event_stats.pre_negative = event_sent['negative']
                    event_users = count_users_sentimet(
                        event.text, event.event_pre_start, event.event_pre_end)
                    event_stats.users_pre = event_users['users']
                    event_stats.users_pre_bullish = event_users['bullish']
                    event_stats.users_pre_bearish = event_users['bearish']

                    event_stats.post_total = int(df_post_est['count'].sum())
                    event_stats.post_median = df_post_est[
                        'count'].median() if event_stats.post_total > 0 else 0
                    event_stats.post_mean = df_post_est[
                        'count'].mean() if event_stats.post_total > 0 else 0
                    # event_stats.post_std = df_post_est['count'].std() if event_stats.post_total > 1 else 0
                    df_post_est = None
                    event_sent = count_sentiment(event.text,
                                                 event.event_post_start,
                                                 event.event_post_end)
                    event_stats.post_bullish = event_sent['bullish']
                    event_stats.post_bearish = event_sent['bearish']
                    event_stats.post_positive = event_sent['positive']
                    event_stats.post_negative = event_sent['negative']
                    event_users = count_users_sentimet(event.text,
                                                       event.event_post_start,
                                                       event.event_post_end)
                    event_stats.users_post = event_users['users']
                    event_stats.users_post_bullish = event_users['bullish']
                    event_stats.users_post_bearish = event_users['bearish']

                    # event_stats.pct_change = (
                    #                                      event_stats.post_total - event_stats.pre_total) / event_stats.pre_total if event_stats.pre_total > 0 else 0

                    db.session.add(event_stats)
                    db.session.commit()

                    df_in.loc[index, "total pre event"] = event_stats.pre_total
                    df_in.loc[index,
                              "median pre event"] = event_stats.pre_median
                    df_in.loc[index, "mean pre event"] = event_stats.pre_mean
                    df_in.loc[index,
                              "bullish pre event"] = event_stats.pre_bullish
                    df_in.loc[index,
                              "bearish pre event"] = event_stats.pre_bearish
                    df_in.loc[index,
                              "positive pre event"] = event_stats.pre_positive
                    df_in.loc[index,
                              "negative pre event"] = event_stats.pre_negative
                    # df_in.loc[index, "std pre event"] = event_stats.pre_std
                    df_in.loc[index,
                              "total during event"] = event_stats.event_total
                    df_in.loc[index,
                              "median during event"] = event_stats.event_median
                    df_in.loc[index,
                              "mean during event"] = event_stats.event_mean
                    df_in.loc[
                        index,
                        "bullish during event"] = event_stats.event_bullish
                    df_in.loc[
                        index,
                        "bearish during event"] = event_stats.event_bearish
                    df_in.loc[
                        index,
                        "positive during event"] = event_stats.event_positive
                    df_in.loc[
                        index,
                        "negative during event"] = event_stats.event_negative
                    # df_in.loc[index, "std during event"] = event_stats.event_std
                    df_in.loc[index,
                              "total post event"] = event_stats.post_total
                    df_in.loc[index,
                              "median post event"] = event_stats.post_median
                    df_in.loc[index, "mean post event"] = event_stats.post_mean
                    df_in.loc[index,
                              "bullish post event"] = event_stats.post_bullish
                    df_in.loc[index,
                              "bearish post event"] = event_stats.post_bearish
                    df_in.loc[
                        index,
                        "positive post event"] = event_stats.post_positive
                    df_in.loc[
                        index,
                        "negative post event"] = event_stats.post_negative
                    # df_in.loc[index, "std post event"] = event_stats.post_std
                    # df_in.loc[index, "pct change"] = event_stats.pct_change
                    df_in.loc[index, "users pre event"] = event_stats.users_pre
                    df_in.loc[index,
                              "users during event"] = event_stats.users_event
                    df_in.loc[index,
                              "users post event"] = event_stats.users_post
                    df_in.loc[
                        index,
                        "bullish users pre event"] = event_stats.users_pre_bullish
                    df_in.loc[
                        index,
                        "bearish users pre event"] = event_stats.users_pre_bearish
                    df_in.loc[
                        index,
                        "bullish users during event"] = event_stats.users_event_bullish
                    df_in.loc[
                        index,
                        "bearish users during event"] = event_stats.users_event_bearish
                    df_in.loc[
                        index,
                        "bullish users post event"] = event_stats.users_post_bullish
                    df_in.loc[
                        index,
                        "bearish users post event"] = event_stats.users_post_bearish

                    # insert_event_tweets(event)

            file_output = 'output_' + file_input
            project.file_output = file_output
            df_in.to_excel(
                os.path.join(Configuration.UPLOAD_FOLDER, file_output),
                index=False)
            # df_in.to_sql('table', db.engine)
            # df_in.to_stata(os.path.join(Configuration.UPLOAD_FOLDER, 'output.dta'), index=False)
            # form.output_file.data = 'upload/output' + file_input
            form.output_file.data = file_output
            db.session.add(project)
            db.session.commit()
            return render_template(
                'project/counts_upload.html',
                form=form,
                project=project,
                df_in=df_in.to_html(classes='table table-striped'))

    if len(form.errors) > 0:
        pprint(form.errors)
    return render_template(
        'project/counts_upload.html', form=form, project=project)