import concurrent.futures as cf
from datetime import datetime, timedelta, date
from dateutil import tz
import os
import pandas as pd
from pprint import pprint
import sys
import uuid
from flask import current_app, render_template, request, session, Markup, flash
from flask_login import current_user, login_required
from sqlalchemy import or_, and_, text, func
from werkzeug.utils import secure_filename, CombinedMultiDict
from application import db
from application.project import project
from ..models import Project, Dataset, TradingDays
from ..forms import CountsFileForm
from ..helpers import slugify
from .count_helper import load_counts, get_tweet_ids
from application.fintweet.models import Tweet, TweetCashtag, TweetHashtag, TweetMention, User, UserCount, mvCashtags


ZONE_NY = tz.gettz('America/New_York')
ZONE_UTC = tz.gettz('UTC')


def convert_date(input_dt, zone_from=ZONE_NY, zone_to=ZONE_UTC):
    utc_datetime = datetime.strptime(input_dt, '%Y-%m-%d %H:%M:%S')
    # Tell the datetime object that it's in NY time zone since datetime objects are 'naive' by default
    utc_datetime = utc_datetime.replace(tzinfo=zone_from)
    return utc_datetime.astimezone(zone_to)


def dataframe_from_file(filename):
    ext = os.path.splitext(filename)[1]
    if ext in ['.xls', '.xlsx']:
        df = pd.read_excel(filename)
        df.columns = [slugify(col.strip()) for col in df.columns]
        if 'cashtag' in df.columns and 'gvkey' in df.columns:
            return df
    # TODO: Create import from CSV
    return None


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
        cn = c.copy()
        date_input = (c['date_from'] + timedelta(days=i))
        cn['date_input'] = date_input
        cn['date'] = date_input
        if c['day_status'] in ['trading', 'all'] and date_input in days_list:
            cn['day_status'] = 'trading'
        elif c['day_status'] in ['non-trading', 'all'] and date_input not in days_list:
            cn['day_status'] = 'non-trading'
        result.append(cn)
    res_list = []
    with cf.ThreadPoolExecutor(max_workers=32) as executor:
        future_to_tweet = {executor.submit(get_tweet_ids, i): i for i in result}
        for future in cf.as_completed(future_to_tweet):
            try:
                tw = future.result()
                tw['tweets_count'] = len(tw['tweet_ids'])
                res_list.append(tw)
            except Exception as e:
                fname = sys._getframe().f_code.co_name
                print(fname, type(e), str(e))
    return res_list


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
                message = Markup(
                    "<strong>Error!</strong> Incorrect input file format. <br> \
                    The input file should only contain two columns: 'gvkey' and 'cashtag'")
                flash(message, 'danger')
                return render_template(
                    'project/counts_upload.html',
                    form=form,
                    project=project)
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
                with cf.ThreadPoolExecutor(max_workers=32) as executor:
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
                        except Exception as e:
                            fname = sys._getframe().f_code.co_name
                            print(fname, type(e), str(e))
            df_output.sort_values(by=['cashtag', 'date'])
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
