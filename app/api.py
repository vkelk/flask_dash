from app import api
from models import Tweet

api.create_api(Tweet, methods=['GET'])
