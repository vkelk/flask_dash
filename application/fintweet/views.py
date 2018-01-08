from flask import request, render_template, url_for, jsonify
from flask_login import login_required
# from application import login_manager
from datatables import ColumnDT, DataTables
from application.fintweet import fintweet
from application.fintweet.models import *

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


# @fintweet.route("/dt_110x")
# def dt_110x():
#     """List users with DataTables <= 1.10.x."""
#     return render_template('fintweet/dt_110x.html', project='dt_110x')


# @fintweet.route('/api/user/all')
# def api_users_all():
#     users = User.query.all()
#     return jsonify(json_list=[i.serialize for i in users])


# @fintweet.route('/api/user/<int:user_id>')
# def api_users_by_id(user_id):
#     user = User.query.filter_by(user_id=user_id).first_or_404()
#     return jsonify(json_list = [user.serialize])


@fintweet.route('/data')
def data():
    """Return server side data."""
    # defining columns
    columns = [
        ColumnDT(User.user_id),
        ColumnDT(User.user_name),
        ColumnDT(Tweet.tweet_id),
        ColumnDT(Tweet.text)
    ]

    # defining the initial query depending on your purpose
    query = Tweet.query \
        .add_columns(User.user_id, User.user_name) \
        .join(User, isouter=True) \
        .order_by(Tweet.tweet_id.asc())
    print(str(query))

    # GET parameters
    params = request.args.to_dict()

    # instantiating a DataTable for the query and table needed
    rowTable = DataTables(params, query, columns)

    # returns what is needed by DataTable
    return jsonify(rowTable.output_result())


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
