import os
from flask import Flask
from flask_sqlalchemy import SQLAlchemy
from flask_login import LoginManager
from flask_wtf import CSRFProtect
from flask_bcrypt import Bcrypt
# from flask_restful import Api
# from flask_restless import APIManager
# from flask_marshmallow import Marshmallow
from logging.handlers import RotatingFileHandler

from .config import Configuration

db = SQLAlchemy()
csrf = CSRFProtect()
bcrypt = Bcrypt()
# api = Api()
# apimanager = APIManager(flask_sqlalchemy_db=db)
# ma = Marshmallow()

# Set up Flask-Login
login_manager = LoginManager()
# login_manager.session_protection = 'strong'
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
    login_manager.init_app(app)
    bcrypt.init_app(app)

    # Extensions like Flask-SQLAlchemy not know what the "current" app
    with app.app_context():
        # reflection to get table meta
        db.Model.metadata.reflect(db.engine)
        db.reflect()
    # ma.init_app(app)

    from application.main import main as main_blueprint
    from application.account import account as account_blueprint
    from application.project import project as project_blueprint
    from application.fintweet import fintweet as fintweet_blueprint
    from application.stocktwits import stocktwit as stocktwit_blueprint
    # from application.fintweet.resources import api_fintweet
    from application.main.resources import api_manager

    # api.init_app(app)
    # api_fintweet.init_app(app)
    api_manager.init_app(app)

    app.register_blueprint(main_blueprint)
    app.register_blueprint(account_blueprint, url_prefix='/account')
    app.register_blueprint(project_blueprint, url_prefix='/project')
    app.register_blueprint(fintweet_blueprint, url_prefix='/fintweet')
    app.register_blueprint(stocktwit_blueprint, url_prefix='/stocktwits')

    print(app.url_map)
    if not os.path.exists(Configuration.UPLOAD_FOLDER):
        try:
            os.makedirs(os.path.dirname(Configuration.UPLOAD_FOLDER), exist_ok=True)
        except Exception as e:  # Guard against race condition
            print(e)

    return app
