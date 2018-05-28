from flask_sqlalchemy import SQLAlchemy
from flask_login import LoginManager
from flask_wtf import CSRFProtect
from flask_bcrypt import Bcrypt
from flask_debugtoolbar import DebugToolbarExtension


db = SQLAlchemy()
csrf = CSRFProtect()
bcrypt = Bcrypt()
toolbar = DebugToolbarExtension()
login_manager = LoginManager()
