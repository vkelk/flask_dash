from flask import g
from flask_restless import APIManager, ProcessingException
from flask_login import current_user, login_required
from application.fintweet.models import *


@login_required
def check_auth(*args, **kwargs):
    pass
    # if not current_user.is_authenticated():
    #     raise ProcessingException(detail='Not authenticated', status=401)


preprocessors = {'GET_SINGLE': [check_auth],
                 'GET_MANY': [check_auth]}
api_fintweet = APIManager(flask_sqlalchemy_db=db, allow_functions=True)

api_fintweet.create_api(User, methods=['GET'],
                        preprocessors=preprocessors, url_prefix='/fintweet/api')
api_fintweet.create_api(Tweet, methods=['GET'], primary_key='tweet_id',
                        preprocessors=preprocessors, url_prefix='/fintweet/api')
api_fintweet.create_api(UserCount, methods=['GET'], primary_key='user_id',
                        preprocessors=preprocessors, url_prefix='/fintweet/api')
api_fintweet.create_api(TweetCashtag, methods=['GET'],
                        preprocessors=preprocessors, url_prefix='/fintweet/api', allow_functions=True)
