from flask import Blueprint

fintweet = Blueprint('fintweet', __name__)


@fintweet.route('/me')
def me():
    return "This is my page.", 200
