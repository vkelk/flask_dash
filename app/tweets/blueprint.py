from flask import Blueprint

from helpers import object_list
from models import Tweet

tweets = Blueprint('tweets', __name__, template_folder='templates')


@tweets.route('/')
def index():
    tweets = Tweet.query.order_by(Tweet.tweet_id.asc())
    return object_list('tweets/index.html', tweets)


@tweets.route('/tweet_id/')
def detail(tweet_id):
    pass
