from flask_restless import APIManager, ProcessingException
from flask_login import current_user, login_required
from .models import db, User, Tweet, TweetCashtag, TweetHashtag, TweetMention


@login_required
def check_auth(*args, **kwargs):
    pass
    # if not current_user.is_authenticated():
    #     raise ProcessingException(detail='Not authenticated', status=401)


preprocessors = {'GET_SINGLE': [check_auth],
                 'GET_MANY': [check_auth]}

api_fintweet = APIManager(flask_sqlalchemy_db=db, allow_functions=True)

api_fintweet.create_api(
    User,
    methods=['GET'],
    preprocessors=preprocessors,
    url_prefix='/fintweet/api',
    allow_functions=True)
api_fintweet.create_api(
    Tweet, methods=['GET'],
    preprocessors=preprocessors,
    url_prefix='/fintweet/api',
    allow_functions=True)
api_fintweet.create_api(
    TweetCashtag,
    methods=['GET'],
    collection_name='cashtag',
    preprocessors=preprocessors,
    url_prefix='/fintweet/api',
    allow_functions=True)
api_fintweet.create_api(
    TweetHashtag,
    methods=['GET'],
    collection_name='hashtag',
    preprocessors=preprocessors,
    url_prefix='/fintweet/api',
    allow_functions=True)
api_fintweet.create_api(
    TweetMention,
    methods=['GET'],
    collection_name='mention',
    preprocessors=preprocessors,
    url_prefix='/fintweet/api',
    allow_functions=True)
