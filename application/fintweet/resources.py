import json
from flask import jsonify
from flask_restful import Api, Resource
from sqlalchemy.ext.declarative import DeclarativeMeta
from datetime import datetime
from application import apimanager
from application.fintweet.models import *


class AlchemyEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj.__class__, DeclarativeMeta):
            # an SQLAlchemy class
            fields = {}
            for field in [x for x in dir(obj) if not x.startswith('_') and x != 'metadata']:
                data = obj.__getattribute__(field)
                try:
                    json.dumps(data)  # this will fail on non-encodable values, like other classes
                    fields[field] = data
                except TypeError:
                    fields[field] = None
            # a json-encodable dict
            return fields

        return json.JSONEncoder.default(self, obj)


class UserAPI(Resource):
    def get(self, user_id=None):
        if user_id:
            query = Tweet.query \
                .add_columns(User.user_id, User.user_name) \
                .join(User, isouter=True) \
                .order_by(Tweet.tweet_id.asc())
            # user = User.query.filter_by(user_id=user_id)
            # user.date_joined = user.date_joined.isoformat()
            return {'user': jsonify(json.dumps(query, cls=AlchemyEncoder))}


api = Api()
api.add_resource(UserAPI, '/api/v1/user/<int:user_id>')

apimanager.create_api(User, methods=['GET'])
apimanager.create_api(Tweet, methods=['GET'], primary_key='tweet_id')
apimanager.create_api(UserCount, methods=['GET'], primary_key='user_id')
