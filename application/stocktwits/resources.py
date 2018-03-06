from flask_restless import APIManager
from flask_login import login_required
from .models import db, User, Ideas, IdeaCashtags, IdeaHashtags


@login_required
def check_auth(*args, **kwargs):
    pass
    # if not current_user.is_authenticated():
    #     raise ProcessingException(detail='Not authenticated', status=401)


preprocessors = {'GET_SINGLE': [check_auth],
                 'GET_MANY': [check_auth]}

api_stocktwits = APIManager(flask_sqlalchemy_db=db, allow_functions=True)

api_stocktwits.create_api(
    User,
    methods=['GET'],
    preprocessors=preprocessors,
    url_prefix='/stocktwits/api',
    allow_functions=True)
api_stocktwits.create_api(
    Ideas, methods=['GET'],
    preprocessors=preprocessors,
    url_prefix='/stocktwits/api',
    allow_functions=True)
api_stocktwits.create_api(
    IdeaCashtags,
    methods=['GET'],
    collection_name='cashtag',
    preprocessors=preprocessors,
    url_prefix='/stocktwits/api',
    allow_functions=True)
api_stocktwits.create_api(
    IdeaHashtags,
    methods=['GET'],
    collection_name='hashtag',
    preprocessors=preprocessors,
    url_prefix='/stocktwits/api',
    allow_functions=True)
