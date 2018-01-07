import os
from datetime import timedelta
from flask import Flask, g, request, session
from flask_sqlalchemy import SQLAlchemy
from flask_login import LoginManager, current_user
from flask_bcrypt import Bcrypt
from flask_assets import Environment
from flask_wtf import CSRFProtect
from flask_mail import Mail
from flask_restless import APIManager
from logging.handlers import RotatingFileHandler

from config import Configuration
from .assets import app_css, app_js, vendor_css, vendor_js

basedir = os.path.abspath(os.path.dirname(__file__))

db = SQLAlchemy()
bcrypt = Bcrypt()
csrf = CSRFProtect()
mail = Mail()

# Set up Flask-Login
login_manager = LoginManager()
login_manager.session_protection = 'strong'
login_manager.login_view = 'account.login'


def create_app(config=None):
    app = Flask(__name__)
    if config:
        app.config.from_object(config)
    # not using sqlalchemy event system, hence disabling it
    app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False

    # Setup logging
    file_handler = RotatingFileHandler('dashboard.log')
    app.logger.addHandler(file_handler)
    app.logger.setLevel(Configuration.LOG_LEVEL)

    # Initialize any extensions and bind blueprints to the application instance here
    db.init_app(app)
    mail.init_app(app)
    bcrypt.init_app(app)
    login_manager.init_app(app)

    # Extensions like Flask-SQLAlchemy not know what the "current" app
    with app.app_context():
        # reflection to get table meta
        db.Model.metadata.reflect(db.engine)
        db.reflect()

    # Register Jinja template functions
    # from .utils import register_template_utils
    # register_template_utils(app)

    # Set up asset pipeline
    assets_env = Environment(app)
    dirs = ['assets/styles', 'assets/scripts']
    for path in dirs:
        assets_env.append_path(os.path.join(basedir, path))
    assets_env.url_expire = True

    assets_env.register('app_css', app_css)
    assets_env.register('app_js', app_js)
    assets_env.register('vendor_css', vendor_css)
    assets_env.register('vendor_js', vendor_js)

    # Create app blueprints
    from .main import main as main_blueprint
    app.register_blueprint(main_blueprint)

    from .account import account
    app.register_blueprint(account, url_prefix='/account')

    from .fintweet import fintweet
    app.register_blueprint(fintweet, url_prefix='/fintweet')

    # from .admin import admin as admin_blueprint
    # app.register_blueprint(admin_blueprint, url_prefix='/admin')
    return app

#
# from config import Configuration  # imports configuration data from config.py
#
# app = Flask(__name__)
# app.config.from_object(Configuration)  # use values from Configuration object
# db = SQLAlchemy(app)
# db.Model.metadata.reflect(db.engine)
# db.reflect()  # reflection to get table meta
#
# api = APIManager(app, flask_sqlalchemy_db=db)
# bcrypt = Bcrypt(app)
#
# login_manager = LoginManager(app)
# login_manager.login_view = 'login'
#
# file_handler = RotatingFileHandler('dashboard.log')
# app.logger.addHandler(file_handler)
# app.logger.setLevel(Configuration.LOG_LEVEL)
#
# from application.users import models as user_models
# from application.users.views import users
#
# app.register_blueprint(users)
#
#
#
#
# @login_manager.user_loader
# def _user_loader(user_id):
#     """
#     Determine which user is logged in
#     This way Flask-Login knows how to convert a user ID into a User object,
#     and that user will be available to us as g.user
#     :param user_id:
#     :return:
#     """
#     return user_models.DashUser.query.get(int(user_id))
#
#
# # This signal handler will load before every request.
# @app.before_request
# def _before_request():
#     g.user = current_user
#
#
# @app.before_request
# def _last_page_visited():
#     if "current_page" in session:
#         session['last_page'] = session['current_page']
#     session['current_page'] = request.path
#
#
# @app.before_request
# def _make_session_permanent():
#     session.permanent = True
#     app.permanent_session_lifetime = timedelta(hours=24)
