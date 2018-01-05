from flask import Flask, g, request, session
from flask_sqlalchemy import SQLAlchemy
from flask_login import LoginManager, current_user
from flask_bcrypt import Bcrypt
from flask_restless import APIManager
from logging.handlers import RotatingFileHandler
from datetime import timedelta

from config import Configuration  # imports configuration data from config.py

app = Flask(__name__)
app.config.from_object(Configuration)  # use values from Configuration object
db = SQLAlchemy(app)
db.Model.metadata.reflect(db.engine)
db.reflect()  # reflection to get table meta

api = APIManager(app, flask_sqlalchemy_db=db)
bcrypt = Bcrypt(app)

login_manager = LoginManager(app)
login_manager.login_view = 'login'

file_handler = RotatingFileHandler('dashboard.log')
app.logger.addHandler(file_handler)
app.logger.setLevel(Configuration.LOG_LEVEL)

from application.users import models as user_models
from application.users.views import users

app.register_blueprint(users)


@login_manager.user_loader
def _user_loader(user_id):
    """
    Determine which user is logged in
    This way Flask-Login knows how to convert a user ID into a User object,
    and that user will be available to us as g.user
    :param user_id:
    :return:
    """
    return user_models.query.get(int(user_id))


# This signal handler will load before every request.
@app.before_request
def _before_request():
    g.user = current_user


@app.before_request
def _last_page_visited():
    if "current_page" in session:
        session['last_page'] = session['current_page']
    session['current_page'] = request.path


@app.before_request
def _make_session_permanent():
    session.permanent = True
    app.permanent_session_lifetime = timedelta(hours=24)
