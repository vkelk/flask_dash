#!C:\Python36\python.exe
import sys

sys.path.append('d:/webapps/flask_dash/')

from application import create_app
from application.config import Configuration

application = create_app(config=Configuration)
