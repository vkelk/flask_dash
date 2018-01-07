from flask import Blueprint, g
from flask_login import current_user

main = Blueprint('main', __name__)

# def _before_request():
#     g.user = current_user


# main.before_app_request(_before_request)

from . import views, errors  # noqa
