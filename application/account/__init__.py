from flask import Blueprint

account = Blueprint('account', __name__)

import application.account.views
