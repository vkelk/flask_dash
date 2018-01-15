from datetime import timedelta
import pandas as pd
import json
from pprint import pprint
from flask import request, render_template, url_for, jsonify, Response, Markup, flash, abort
from flask_login import login_required
from sqlalchemy.exc import IntegrityError
# from application import login_manager
# from datatables import ColumnDT, DataTables
from application.fintweet.helpers import Collections, DataTables
from application.fintweet import fintweet, table_builder
from application.fintweet.models import *
from application.fintweet.forms import Form1

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

    form = Form1(request.form)
    if request.method == 'POST':
        if form.get_cashtags.data and form.code_type_radio.data and form.company_codes.data:
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
                return render_template('fintweet/eventstudy.html', form=form, cashtags=cashtags)
            except IntegrityError:
                message = Markup(
                    "<strong>Error!</strong> Something went wrong.")
                flash(message, 'danger')
        elif form.get_event_data.data and form.event_date.data:
            try:
                code_list = json.loads(form.codes_list.data)
                start_date = form.event_date.data - timedelta(days=form.pre_event.data)
                end_date = form.event_date.data + timedelta(days=form.post_event.data)
                query = db.session \
                    .query(Tweet.date, db.func.count(Tweet.tweet_id).label("count")) \
                    .join(TweetCashtag) \
                    .filter(TweetCashtag.cashtags.in_(code_list)) \
                    .filter(Tweet.date >= start_date) \
                    .filter(Tweet.date <= end_date) \
                    .group_by(Tweet.date).order_by(Tweet.date)
                data = pd.DataFrame([r._asdict() for r in query.all()])
                pre_data = data.loc[data['date'] < form.event_date.data]
                event_data = data.loc[data['date'] == form.event_date.data]
                post_data = data.loc[data['date'] > form.event_date.data]
                # pprint(data)
                # pprint(pre_data)
                # pprint(event_data)
                # pprint(post_data)
                tables = [pre_data.to_html(classes='table table-striped'),
                          event_data.to_html(classes='male'),
                          post_data.to_html(classes='male')]
                titles = ['na', 'Pre Event', 'On Event', 'Post Event']
                num = pd.DataFrame([{"pre_event": [pre_data['count'].mean(), pre_data['count'].median()]},
                                    {"on_event": [event_data['count'].mean(), event_data['count'].median()]},
                                    {"post_event": [post_data['count'].mean(), post_data['count'].median()]}])
                return render_template('fintweet/eventstudy.html', form=form,
                                       tables=tables, titles=titles, num=num)

            except IntegrityError:
                message = Markup(
                    "<strong>Error!</strong> Something went wrong.")
                flash(message, 'danger')
    if len(form.errors) > 0:
        pprint(form.errors)
    return render_template('fintweet/eventstudy.html', form=form)
