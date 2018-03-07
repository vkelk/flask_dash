from flask_restless import APIManager, ProcessingException
from flask_login import current_user, login_required
from application import db
from application.project.models import Project, Event
from application.fintweet.models import User as ftUser, Tweet, TweetCashtag, TweetHashtag, TweetMention
from application.stocktwits.models import Ideas, User as stUser, IdeaCashtags, IdeaHashtags


@login_required
def check_auth(*args, **kwargs):
    pass
    # if not current_user.is_authenticated():
    #     raise ProcessingException(detail='Not authenticated', status=401)


preprocessors = {'GET_SINGLE': [check_auth],
                 'GET_MANY': [check_auth]}

api_manager = APIManager(flask_sqlalchemy_db=db, allow_functions=True)

api_manager.create_api(Project, methods=['GET'], collection_name='project',
                       preprocessors=preprocessors, url_prefix='/project/api', allow_functions=True)
api_manager.create_api(Event, methods=['GET'], collection_name='event',
                       preprocessors=preprocessors, url_prefix='/project/api', allow_functions=True)
api_manager.create_api(ftUser, methods=['GET'],
                       preprocessors=preprocessors, url_prefix='/fintweet/api', allow_functions=True)
api_manager.create_api(Tweet, methods=['GET'],
                       preprocessors=preprocessors, url_prefix='/fintweet/api', allow_functions=True)
api_manager.create_api(TweetCashtag, methods=['GET'], collection_name='cashtag',
                       preprocessors=preprocessors, url_prefix='/fintweet/api', allow_functions=True)
api_manager.create_api(TweetHashtag, methods=['GET'], collection_name='hashtag',
                       preprocessors=preprocessors, url_prefix='/fintweet/api', allow_functions=True)
api_manager.create_api(TweetMention, methods=['GET'], collection_name='mention',
                       preprocessors=preprocessors, url_prefix='/fintweet/api', allow_functions=True)
api_manager.create_api(
    stUser,
    methods=['GET'],
    preprocessors=preprocessors,
    url_prefix='/stocktwits/api',
    allow_functions=True)
api_manager.create_api(
    Ideas,
    methods=['GET'],
    preprocessors=preprocessors,
    url_prefix='/stocktwits/api',
    allow_functions=True)
api_manager.create_api(
    IdeaCashtags,
    methods=['GET'],
    collection_name='cashtag',
    preprocessors=preprocessors,
    url_prefix='/stocktwits/api',
    allow_functions=True)
api_manager.create_api(
    IdeaHashtags,
    methods=['GET'],
    collection_name='hashtag',
    preprocessors=preprocessors,
    url_prefix='/stocktwits/api',
    allow_functions=True)
