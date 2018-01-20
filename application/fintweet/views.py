import os
from datetime import timedelta
import pandas as pd
import json
from functools import reduce
from pprint import pprint
from flask import request, render_template, url_for, jsonify, Response, Markup, flash, abort, send_file, make_response
from flask_login import login_required
from sqlalchemy.exc import IntegrityError
from werkzeug.utils import secure_filename, CombinedMultiDict
from application.config import Configuration
# from application import login_manager
# from datatables import ColumnDT, DataTables
from application.fintweet.helpers import Collections, DataTables, slugify
from application.fintweet import fintweet, table_builder
from application.fintweet.models import *
from application.fintweet.forms import *

from .helpers import object_list


def tweets_list(template, query, **context):
    search = request.args.get('q')
    if search:
        query = query.filter((Tweet.text.contains(search)) |
                             (User.user_name.contains(search)) |
                             (User.twitter_handle.contains(search)))
    return object_list(template, query, **context)


@fintweet.route('/')
@login_required
def index():
    tweets = Tweet.query \
        .add_columns(User.user_name, Tweet.tweet_id, Tweet.text) \
        .join(User, isouter=True) \
        .order_by(Tweet.tweet_id.asc())
    # print(tweets)
    return tweets_list('fintweet/index.html', tweets)


@fintweet.route("/dt_110x")
def dt_110x():
    """List users with DataTables <= 1.10.x."""
    return render_template('fintweet/dt_110x.html', project='dt_110x')


# @fintweet.route('/api/user/all')
# def api_users_all():
#     users = User.query.all()
#     return jsonify(json_list=[i.serialize for i in users])


# @fintweet.route('/api/user/<int:user_id>')
# def api_users_by_id(user_id):
#     user = User.query.filter_by(user_id=user_id).first_or_404()
#     return jsonify(json_list = [user.serialize])


# @fintweet.route('/data')
# def data():
#     """Return server side data."""
#     # defining columns
#     columns = [
#         ColumnDT(TweetCashtag.cashtags),
#         ColumnDT(TweetCashtag.count)
#         # ColumnDT(Tweet.tweet_id),
#         # ColumnDT(Tweet.text)
#     ]
#
#     # defining the initial query depending on your purpose
#     q = db.session.query(TweetCashtag.cashtags, func.count(TweetCashtag.cashtags).label('count')).group_by(
#         TweetCashtag.cashtags)
#     query = TweetCashtag.query \
#         .add_columns(User.user_id, User.user_name) \
#         .join(User, isouter=True) \
#         .order_by(Tweet.tweet_id.asc())
#     print(str(q))
#
#     # GET parameters
#     params = request.args.to_dict()
#
#     # instantiating a DataTable for the query and table needed
#     rowTable = DataTables(params, query, columns)
#
#     # returns what is needed by DataTable
#     return jsonify(rowTable.output_result())


@fintweet.route('/<tweet_id>/')
@login_required
def detail(tweet_id):
    # tweet = Tweet.query.filter(Tweet.tweet_id == tweet_id).first_or_404()
    tweet = Tweet.query \
        .add_columns(Tweet.user_id, Tweet.text, Tweet.date, Tweet.time,
                     User.user_name, User.twitter_handle) \
        .join(User) \
        .filter(Tweet.tweet_id == tweet_id).first_or_404()
    # user = User.query.filter(User.user_id == tweet.user_id).first_or_404()
    hashtags = TweetHashtag.query.filter(TweetHashtag.tweet_id == tweet_id)
    cashtags = TweetCashtag.query.filter(TweetCashtag.tweet_id == tweet_id)
    return render_template('fintweet/detail.html', tweet=tweet, hashtags=hashtags, cashtags=cashtags)


@fintweet.route('/cashtag/<cashtag>')
@login_required
def cashtag_detail(cashtag):
    tweets = Tweet.query \
        .add_columns(User.user_name, Tweet.tweet_id, Tweet.text) \
        .join(User, isouter=True) \
        .join(TweetCashtag) \
        .filter(TweetCashtag.cashtags == cashtag) \
        .order_by(Tweet.tweet_id.asc())
    # print(tweets)
    return object_list('fintweet/tag_detail.html', tweets, cashtag=cashtag)


@fintweet.route('/hashtag/<hashtag>')
@login_required
def hashtag_detail(hashtag):
    tweets = Tweet.query \
        .add_columns(User.user_name, Tweet.tweet_id, Tweet.text) \
        .join(User, isouter=True) \
        .join(TweetHashtag) \
        .filter(TweetHashtag.hashtags == hashtag) \
        .order_by(Tweet.tweet_id.asc())
    # print(tweets)
    return object_list('fintweet/tag_detail.html', tweets, hashtag=hashtag)


@fintweet.route('/dt')
def dt():
    dataTables = DataTables(source="data")
    dataTables.setColumns([{"param": "Parameters"}, {"val": "Value"}])
    return render_template('fintweet/datatables.html', table=dataTables.render())


@fintweet.route("/data")
def data():
    list = [
        {'param': 'foo', 'val': 2},
        {'param': 'bar', 'val': 10}
    ]
    collection = Collections(list)
    return Response(collection.respond(), mimetype="application/json")


@fintweet.route("/clientside_table")
def clientside_table():
    return render_template("fintweet/clientside_table.html")


@fintweet.route("/clientside_table_data", methods=['GET'])
def clientside_table_content():
    data = table_builder.collect_data_clientside()
    return jsonify(data)


@fintweet.route("/serverside_table")
def serverside_table():
    return render_template("fintweet/serverside_table.html")


@fintweet.route("/serverside_table_data", methods=['GET'])
def serverside_table_content():
    data = table_builder.collect_data_serverside(request)
    return jsonify(data)


@fintweet.route("/eventstudyfile", methods=["GET", "POST"])
def eventstudyfile():
    def dataframe_from_file(filename):
        ext = os.path.splitext(filename)[1]
        if ext in ['.xls', '.xlsx']:
            df = pd.read_excel(filename)
            df.columns = [slugify(col) for col in df.columns]
            return df

    def get_data_from_query(c_tag, estimation):
        q = db.session \
            .query(Tweet.date, db.func.count(Tweet.tweet_id).label("count")) \
            .join(TweetCashtag) \
            .filter(TweetCashtag.cashtags == c_tag) \
            .filter(Tweet.date >= estimation['pre_start']) \
            .filter(Tweet.date <= estimation['post_end']) \
            .group_by(Tweet.date).order_by(Tweet.date)
        # print(q)
        return [r._asdict() for r in q.all()]

    form = EventStudyFileForm(CombinedMultiDict((request.files, request.form)))
    if request.method == "POST":
        if form.btn_file_upload.data:
            if form.validate_on_submit():
                file_input = secure_filename(form.file_input.data.filename)
                form.file_input.data.save(os.path.join(Configuration.UPLOAD_FOLDER, file_input))
                form.file_name.data = file_input
            return render_template('fintweet/eventstudyfile.html', form=form)

        if form.btn_calculate.data:
            df_in = dataframe_from_file(os.path.join(Configuration.UPLOAD_FOLDER, form.file_name.data))
            df_in["median pre event"] = ""
            df_in["mean pre event"] = ""
            df_in["total during event"] = ""
            df_in["median during event"] = ""
            df_in["mean during event"] = ""
            df_in["median post event"] = ""
            df_in["mean post event"] = ""
            for index, row in df_in.iterrows():
                date_on_event = row['event_date'].to_pydatetime()
                days_pre_event = abs(form.days_pre_event.data)
                event_window = {'date': date_on_event,
                                'start': date_on_event - timedelta(days=days_pre_event),
                                'end': date_on_event + timedelta(days=form.days_post_event.data)}
                date_estwin_pre_end = event_window['start'] - timedelta(days=(form.days_grace_period.data + 1))
                date_estwin_pre_start = date_estwin_pre_end - timedelta(days=form.days_estimation.data)
                if form.select_deal_resolution.data == 'false':
                    date_estwin_post_start = event_window['end'] + timedelta(days=1)
                    date_estwin_post_end = date_estwin_post_start + timedelta(days=form.days_estimation.data)
                else:
                    date_estwin_post_start = event_window['end'] + timedelta(days=1)
                    date_estwin_post_end = date_estwin_post_start + timedelta(days=row['deal_resolution'])

                estimation_window = {'pre_end': date_estwin_pre_end,
                                     'pre_start': date_estwin_pre_start,
                                     'post_start': date_estwin_post_start,
                                     'post_end': date_estwin_post_end}
                result_list = get_data_from_query(row['cashtag'], estimation_window)
                if len(result_list) > 0:
                    df_full = pd.DataFrame.from_records(result_list, index='date')
                    df_pre_est = df_full.loc[: estimation_window['pre_end'].date()]
                    df_event = df_full.loc[event_window['start'].date():event_window['end'].date()]
                    df_post_est = df_full.loc[event_window['end'].date():]

                    df_in.loc[index, "median pre event"] = df_pre_est['count'].median()
                    df_in.loc[index, "mean pre event"] = df_pre_est['count'].mean()
                    df_in.loc[index, "total during event"] = df_event['count'].sum()
                    df_in.loc[index, "median during event"] = df_event['count'].median()
                    df_in.loc[index, "mean during event"] = df_event['count'].mean()
                    df_in.loc[index, "median post event"] = df_post_est['count'].median()
                    df_in.loc[index, "mean post event"] = df_post_est['count'].mean()
            df_in.to_excel(os.path.join(Configuration.UPLOAD_FOLDER, 'output.xlsx'), index=False)
            form.file_csv.data = 'upload/output.xlsx'

            return render_template('fintweet/eventstudyfile.html', form=form,
                                   df_in=df_in.to_html(classes='table table-striped'))

    if len(form.errors) > 0:
        pprint(form.errors)
    del form.btn_calculate
    return render_template('fintweet/eventstudyfile.html', form=form)


@fintweet.route('/getCSV')  # this is a job for GET, not POST
def send_csv():
    return send_file('uploads/output.xlsx',
                     mimetype='text/csv',
                     attachment_filename='output.xlsx',
                     as_attachment=True)


@fintweet.route("/eventstudy", methods=["GET", "POST"])
def eventstudy():
    def get_from_radio(code_type, codes):
        code_list = codes.split(',')
        if code_type == 'permno':
            query = DealNosFT.query.filter(DealNosFT.permno.in_(code_list)).all()
            return [item.cashtag for item in query]
        elif code_type == 'cashtag':
            return code_list
        elif code_type == 'ticker':
            for i, item in enumerate(code_list):
                code_list[i] = '$' + item.strip()
            return code_list

    def get_data_from_query(c_tag, start, end):
        q = db.session \
            .query(Tweet.date, db.func.count(Tweet.tweet_id).label("count")) \
            .join(TweetCashtag) \
            .filter(TweetCashtag.cashtags == c_tag) \
            .filter(Tweet.date >= start) \
            .filter(Tweet.date <= end) \
            .group_by(Tweet.date).order_by(Tweet.date)
        print(q)
        return [r._asdict() for r in q.all()]

    form = EventStydyForm(request.form)
    if request.method == 'POST':
        if form.btn_get_cashtags.data and form.code_type_radio.data and form.company_codes.data:
            try:
                cashtags = get_from_radio(form.code_type_radio.data, form.company_codes.data)
                form.codes_list.data = json.dumps(cashtags)
                pprint(cashtags)
                pprint(form.codes_list.data)
                form.cashtags_options.choices = [("", "ALL")]
                form.cashtags_options.choices += [(cashtag, cashtag) for cashtag in cashtags]
                if form.event_window.data:
                    second_event_date = form.event_date.data + timedelta(days=form.event_window.data)
                    start_date = min({form.event_date.data, second_event_date.data})
                    end_date = max({form.event_date.data, second_event_date.data})
                permnos = TweetCashtag.query.with_entities(TweetCashtag.permno).order_by(TweetCashtag.permno).group_by(
                    TweetCashtag.permno).all()
                del form.btn_download_csv
                return render_template('fintweet/eventstudy.html', form=form, cashtags=cashtags)
            except IntegrityError:
                message = Markup(
                    "<strong>Error!</strong> Something went wrong.")
                flash(message, 'danger')
        elif (form.btn_get_event_data.data or form.btn_download_csv.data) and form.event_date.data:
            try:
                code_list = json.loads(form.codes_list.data)
                form.cashtags_options.choices = [("", "ALL")]
                form.cashtags_options.choices += [(cashtag, cashtag) for cashtag in code_list]
                days_pre_event = abs(form.pre_event.data)
                start_date = form.event_date.data - timedelta(days=days_pre_event)
                end_date = form.event_date.data + timedelta(days=form.post_event.data)
                dates_range = pd.date_range(start_date, end_date)
                pd_index = [i for i in range(-days_pre_event, form.post_event.data + 1, 1)]
                if form.cashtags_options.data == '':
                    if len(code_list) == 1:
                        result_list = get_data_from_query(code_list[0], start_date, end_date)
                        data = pd.DataFrame(i for i in result_list)
                        data.index = pd.DatetimeIndex(data['date'])
                        data = data.reindex(dates_range, fill_value=0)
                        data['days'] = pd_index
                    elif len(code_list) > 1:
                        q = db.session \
                            .query(Tweet.date, db.func.count(Tweet.tweet_id).label("count")) \
                            .join(TweetCashtag) \
                            .filter(TweetCashtag.cashtags.in_(code_list)) \
                            .filter(Tweet.date >= start_date) \
                            .filter(Tweet.date <= end_date) \
                            .group_by(Tweet.date).order_by(Tweet.date)
                        print(q)
                        data = pd.DataFrame([r._asdict() for r in q.all()])
                        data.index = pd.DatetimeIndex(data['date'])
                        data = data.reindex(dates_range, fill_value=0)
                        data['days'] = pd_index
                        pprint(data)
                else:
                    result_list = get_data_from_query(form.cashtags_options.data, start_date, end_date)
                    data = pd.DataFrame(i for i in result_list)
                    data.index = pd.DatetimeIndex(data['date'])
                    data = data.reindex(dates_range, fill_value=0)
                    data['days'] = pd_index
                if len(data) < 1:
                    message = Markup(
                        "<strong>Warning</strong> The selected filters didn't produce any data.")
                    flash(message, 'warning')
                    return render_template('fintweet/eventstudy.html', form=form)
                data = data[['days', 'count']]
                pre_data = data.loc[data['days'] < 0]
                event_data = data.loc[data['days'] == 0]
                post_data = data.loc[data['days'] > 0]
                # tables = [pre_data.to_html(classes='table table-striped'),
                #           event_data.to_html(classes='table table-striped'),
                #           post_data.to_html(classes='table table-striped')]
                tables = [data.to_html(classes='table table-striped')]
                if form.cashtags_options.data == '':
                    ew_title = 'Event window for cashtag(s) {}'.format(str(code_list))
                else:
                    ew_title = 'Event window for cashtag(s) {}'.format(form.cashtags_options.data)
                titles = ['na', ew_title]
                stats = pd.DataFrame({'PRE EVENT': [pre_data['count'].mean(), pre_data['count'].median()],
                                      'ON EVENT': [event_data['count'].mean(), event_data['count'].median()],
                                      'POST EVENT': [post_data['count'].mean(), post_data['count'].median()]},
                                     index=['mean', 'median'],
                                     columns=['PRE EVENT', 'ON EVENT', 'POST EVENT'])
                if form.btn_download_csv.data and len(data) > 1:
                    resp = make_response(data.to_csv())
                    resp.headers["Content-Disposition"] = "attachment; filename=export.csv"
                    resp.headers["Content-Type"] = "text/csv"
                    return resp
                    # buffer = StringIO()
                    # data.to_csv(buffer, encoding='utf-8')
                    # buffer.seek(0)
                    # return send_file(buffer,
                    #                  attachment_filename="test.csv",
                    #                  mimetype='text/csv')
                return render_template('fintweet/eventstudy.html', form=form,
                                       tables=tables, titles=titles, stats=stats.to_html(classes='table table-striped'))

            except IntegrityError:
                message = Markup(
                    "<strong>Error!</strong> Something went wrong.")
                flash(message, 'danger')
    del form.btn_get_event_data
    del form.btn_download_csv
    if len(form.errors) > 0:
        pprint(form.errors)
    return render_template('fintweet/eventstudy.html', form=form)
