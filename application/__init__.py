from flask import Flask
from flask_sqlalchemy import SQLAlchemy
from flask_login import LoginManager
from flask_wtf import CSRFProtect
from flask_bcrypt import Bcrypt
from logging.handlers import RotatingFileHandler

from config import Configuration

db = SQLAlchemy()
csrf = CSRFProtect()
bcrypt = Bcrypt()

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

    # Create app blueprints
    from application.main import main as main_blueprint
    app.register_blueprint(main_blueprint)

    from application.account import account as account_blueprint
    app.register_blueprint(account_blueprint, url_prefix='/account')

    return app
