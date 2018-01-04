from flask import Blueprint, flash, redirect, render_template, request, url_for
from app import db

from helpers import object_list
from models import *

tweets = Blueprint('tweets', __name__, template_folder='templates')


def tweets_list(template, query, **context):
    search = request.args.get('q')
    if search:
        query = query.filter((Tweet.text.contains(search)) |
                             (User.user_name.contains(search)) |
                             (User.twitter_handle.contains(search)))
    return object_list(template, query, **context)


@tweets.route('/')
def index():
    tweets = Tweet.query\
        .add_columns(User.user_name, Tweet.tweet_id, Tweet.text)\
        .join(User, isouter=True)\
        .order_by(Tweet.tweet_id.asc())
    # print(tweets)
    return tweets_list('tweets/index.html', tweets)


@tweets.route('/cashtag/<cashtag>')
def cashtag_detail(cashtag):
    tweets = Tweet.query \
        .add_columns(User.user_name, Tweet.tweet_id, Tweet.text) \
        .join(User, isouter=True) \
        .join(TweetCashtag)\
        .filter(TweetCashtag.cashtags == cashtag)\
        .order_by(Tweet.tweet_id.asc())
    # print(tweets)
    return object_list('tweets/tag_detail.html', tweets, cashtag=cashtag)


@tweets.route('/hashtag/<hashtag>')
def hashtag_detail(hashtag):
    tweets = Tweet.query \
        .add_columns(User.user_name, Tweet.tweet_id, Tweet.text) \
        .join(User, isouter=True) \
        .join(TweetHashtag)\
        .filter(TweetHashtag.hashtags == hashtag)\
        .order_by(Tweet.tweet_id.asc())
    # print(tweets)
    return object_list('tweets/tag_detail.html', tweets, hashtag=hashtag)


@tweets.route('/<tweet_id>/')
def detail(tweet_id):
    tweet = Tweet.query.filter(Tweet.tweet_id == tweet_id).first_or_404()
    user = User.query.filter(User.user_id == tweet.user_id).first_or_404()
    hashtags = TweetHashtag.query.filter(TweetHashtag.tweet_id == tweet_id)
    cashtags = TweetCashtag.query.filter(TweetCashtag.tweet_id == tweet_id)
    return render_template('tweets/detail.html', tweet=tweet, user=user, hashtags=hashtags, cashtags=cashtags)
