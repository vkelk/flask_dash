from flask import request, render_template, url_for, jsonify, Response, Markup, flash, abort, send_file, make_response
from flask_login import login_required
from sqlalchemy import func
from sqlalchemy.exc import IntegrityError
from werkzeug.utils import secure_filename, CombinedMultiDict
from application.config import Configuration
from application.stocktwits import stocktwit
from .models import *
# from .forms import *

# from .helpers import object_list


@stocktwit.route('/')
@login_required
def index():
    return render_template('fintweet/index.html')
