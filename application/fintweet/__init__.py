from flask import Blueprint, g, session, request, current_app
from flask_login import current_user
# from flask_restful import Api, Resource
from application.fintweet.models import Tweet
from application.fintweet.resources import TableBuilder

fintweet = Blueprint('fintweet', __name__)
table_builder = TableBuilder()

# This signal handler will load before every request.
def _before_request():
    g.user = current_user
    if "current_page" in session:
        session['last_page'] = session['current_page']
    session['current_page'] = request.path


fintweet.before_request(_before_request)

import application.fintweet.views

# class HelloWorld(Resource):
#     def get(self):
#         return {'hello': 'world'}
#
#
# api = Api(prefix='/api/v1')  # Note, no app
# api.add_resource(HelloWorld, '/hello')
# api.init_app(fintweet)
