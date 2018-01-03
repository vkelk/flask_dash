from flask import Blueprint, render_template
from app import db

from helpers import object_list
from models import *

tweets = Blueprint('tweets', __name__, template_folder='templates')


@tweets.route('/')
def index():
    tweets = db.session.query(Tweet, User).join(User, isouter=True).order_by(Tweet.tweet_id.asc())
    # tweets = Tweet.query.join(User, isouter=True).order_by(Tweet.tweet_id.asc())
    print(tweets)
    return object_list('tweets/index.html', tweets)


@tweets.route('/<tweet_id>/')
def detail(tweet_id):
    tweet = Tweet.query.filter(Tweet.tweet_id == tweet_id).first_or_404()
    user = User.query.filter(User.user_id == tweet.user_id).first_or_404()
    return render_template('tweets/detail.html', tweet=tweet, user=user)
