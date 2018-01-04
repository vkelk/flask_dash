from flask import Flask, g
from flask_sqlalchemy import SQLAlchemy
from flask_login import LoginManager, current_user
from flask_bcrypt import Bcrypt

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

