from flask import Blueprint, render_template
from app import db

from helpers import object_list
from models import *

tweets = Blueprint('tweets', __name__, template_folder='templates')


@tweets.route('/')
def index():
    # tweets = db.session.query(Tweet, User).join(User, isouter=True).order_by(Tweet.tweet_id.asc())
    tweets = Tweet.query\
        .add_columns(User.user_name, Tweet.tweet_id, Tweet.text)\
        .join(User, isouter=True)\
        .order_by(Tweet.tweet_id.asc())
    return object_list('tweets/index.html', tweets)


@tweets.route('/cashtag/<cashtag>')
def cashtag_detail(cashtag):
    cashtag = TweetCashtag.query.distinct(TweetCashtag.tweet_id).filter(TweetCashtag.cashtags == cashtag)
    print(cashtag)
    tweets = cashtag.tweet_id.order_by(Tweet.tweet_id.asc())
    print(tweets)
    return object_list('tweets/cashtag_detail.html', tweets, cashtag=cashtag)


@tweets.route('/<tweet_id>/')
def detail(tweet_id):
    tweet = Tweet.query.filter(Tweet.tweet_id == tweet_id).first_or_404()
    user = User.query.filter(User.user_id == tweet.user_id).first_or_404()
    hashtags = TweetHashtag.query.filter(TweetHashtag.tweet_id == tweet_id)
    cashtags = TweetCashtag.query.filter(TweetCashtag.tweet_id == tweet_id)
    return render_template('tweets/detail.html', tweet=tweet, user=user, hashtags=hashtags, cashtags=cashtags)
