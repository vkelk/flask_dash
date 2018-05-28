import time
import os
import requests
import arrow
from flask import Flask, g, request, render_template
# from flask_restful import Api
# from flask_restless import APIManager
# from flask_marshmallow import Marshmallow
from logging.handlers import RotatingFileHandler

from application import config
from .extenstions import db, csrf, bcrypt, toolbar, login_manager
from .main import main as main_blueprint
from .account import account as account_blueprint
from .project import project as project_blueprint
from .fintweet import fintweet as fintweet_blueprint
from .stocktwits import stocktwit as stocktwit_blueprint
from .utils import url_for_other_page

# api = Api()
# apimanager = APIManager(flask_sqlalchemy_db=db)
# ma = Marshmallow()

# Set up Flask-Login
# login_manager = LoginManager()
# login_manager.session_protection = 'strong'
login_manager.login_view = 'account.login'


def create_app(config=config.base_config):
    """Returns an initialized Flask application."""
    app = Flask(__name__)
    app.config.from_object(config)

    register_extensions(app)
    register_blueprints(app)
    register_errorhandlers(app)
    register_jinja_env(app)
    register_commands(app)
    register_logging(app)

    @app.before_request
    def before_request():
        """Prepare some things before the application handles a request."""
        g.request_start_time = time.time()
        g.request_time = lambda: '%.5fs' % (time.time() - g.request_start_time)
        g.pjax = 'X-PJAX' in request.headers

    # Create upload folder
    if not os.path.exists(app.config['UPLOAD_FOLDER']):
        try:
            os.makedirs(os.path.dirname(app.config['UPLOAD_FOLDER']), exist_ok=True)
        except Exception as e:  # Guard against race condition
            print(type(e), str(e))

    # Extensions like Flask-SQLAlchemy not know what the "current" app
    with app.app_context():
        # reflection to get table meta
        db.Model.metadata.reflect(db.engine)
        db.reflect()
    # ma.init_app(app)

    from application.main.resources import api_manager
    api_manager.init_app(app)

    print(app.url_map)
    return app


def register_extensions(app):
    """Register extensions with the Flask application."""
    db.init_app(app)
    login_manager.init_app(app)
    login_manager.login_view = 'account.login'
    bcrypt.init_app(app)
    toolbar.init_app(app)
    csrf.init_app(app)


def register_blueprints(app):
    """Register blueprints with the Flask application."""
    app.register_blueprint(main_blueprint)
    app.register_blueprint(account_blueprint, url_prefix='/account')
    app.register_blueprint(project_blueprint, url_prefix='/project')
    app.register_blueprint(fintweet_blueprint, url_prefix='/fintweet')
    app.register_blueprint(stocktwit_blueprint, url_prefix='/stocktwits')


def register_errorhandlers(app):
    """Register error handlers with the Flask application."""

    def render_error(e):
        return render_template('errors/%s.html' % e.code), e.code

    for e in [
        requests.codes.INTERNAL_SERVER_ERROR,
        requests.codes.NOT_FOUND,
        requests.codes.UNAUTHORIZED,
    ]:
        app.errorhandler(e)(render_error)


def register_jinja_env(app):
    """Configure the Jinja env to enable some functions in templates."""
    app.jinja_env.globals.update({
        'timeago': lambda x: arrow.get(x).humanize(),
        'url_for_other_page': url_for_other_page,
    })


def register_commands(app):
    """Register custom commands for the Flask CLI."""
    # for command in [create_db, drop_db, populate_db, recreate_db]:
    #    app.cli.command()(command)
    pass


def register_logging(app):
    """Setup logging system."""
    file_handler = RotatingFileHandler('dashboard.log')
    app.logger.addHandler(file_handler)
    app.logger.setLevel(app.config['LOG_LEVEL'])
