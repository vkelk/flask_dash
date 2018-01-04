from flask import Flask, g, request, session
from flask_sqlalchemy import SQLAlchemy
from flask_login import LoginManager, current_user
from flask_bcrypt import Bcrypt
from datetime import timedelta

from config import Configuration  # imports configuration data from config.py

app = Flask(__name__)
app.config.from_object(Configuration)  # use values from Configuration object
db = SQLAlchemy(app)
db.Model.metadata.reflect(db.engine)
bcrypt = Bcrypt(app)

login_manager = LoginManager(app)
login_manager.login_view = 'login'


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
