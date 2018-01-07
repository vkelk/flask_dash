from datetime import timedelta
from flask import Blueprint, g, session, request
from flask_login import current_user

fintweet = Blueprint('fintweet', __name__, template_folder='templates')

from . import views


# This signal handler will load before every request.
@fintweet.before_request
def _before_request():
    g.user = current_user


@fintweet.before_request
def _last_page_visited():
    if "current_page" in session:
        session['last_page'] = session['current_page']
    session['current_page'] = request.path


@fintweet.before_request
def _make_session_permanent():
    session.permanent = True
    fintweet.permanent_session_lifetime = timedelta(hours=24)
