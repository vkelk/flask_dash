from flask import g
from flask_restless import APIManager, ProcessingException
from flask_login import current_user, login_required
from application.fintweet.models import *
from application.fintweet.helpers import ServerSideTable


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
api_fintweet.create_api(TweetCashtag, methods=['GET'], collection_name='cashtag',
                        preprocessors=preprocessors, url_prefix='/fintweet/api', allow_functions=True)

SERVERSIDE_TABLE_COLUMNS = [
    {
        "data_name": "A",
        "column_name": "Column A",
        "default": "",
        "order": 1,
        "searchable": True
    },
    {
        "data_name": "B",
        "column_name": "Column B",
        "default": "",
        "order": 2,
        "searchable": True
    },
    {
        "data_name": "C",
        "column_name": "Column C",
        "default": 0,
        "order": 3,
        "searchable": False
    },
    {
        "data_name": "D",
        "column_name": "Column D",
        "default": 0,
        "order": 4,
        "searchable": False
    }
]

DATA_SAMPLE = [
    {'A': 'Hello!', 'B': 'How is it going?', 'C': 3, 'D': 4},
    {'A': 'These are sample texts', 'B': 0, 'C': 5, 'D': 6},
    {'A': 'Mmmm', 'B': 'I do not know what to say', 'C': 7, 'D': 16},
    {'A': 'Is it enough?', 'B': 'Okay', 'C': 8, 'D': 9},
    {'A': 'Just one more', 'B': '...', 'C': 10, 'D': 11},
    {'A': 'Thanks!', 'B': 'Goodbye.', 'C': 12, 'D': 13}
]


class TableBuilder(object):

    def collect_data_clientside(self):
        return {'data': DATA_SAMPLE}

    def collect_data_serverside(self, request):
        columns = SERVERSIDE_TABLE_COLUMNS
        return ServerSideTable(request, DATA_SAMPLE, columns).output_result()
