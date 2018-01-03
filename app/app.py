from flask import Flask
from flask_sqlalchemy import SQLAlchemy

from config import Configuration  # imports configuration data from config.py

app = Flask(__name__)
app.config.from_object(Configuration)  # use values from Configuration object
db = SQLAlchemy(app)
