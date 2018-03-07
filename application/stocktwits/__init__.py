from flask import Blueprint, g, session, request, current_app
from flask_login import current_user
from .models import Ideas

stocktwit = Blueprint('stocktwit', __name__)


# This signal handler will load before every request.
def _before_request():
    g.user = current_user
    if "current_page" in session:
        session['last_page'] = session['current_page']
    session['current_page'] = request.path


stocktwit.before_request(_before_request)

import application.stocktwits.views
