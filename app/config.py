import logging
import os
from db_config import db_config


class Configuration(object):
    APPLICATION_DIR = os.path.dirname(os.path.realpath(__file__))
    DEBUG = True
    LOG_LEVEL = logging.WARNING
    SECRET_KEY = 'protectyoursecrets'  # Create a unique key for your app
    # The SQLALCHEMY_DATABASE_URI comprises the following parts:
    # dialect+driver://username:password@host:port/database
    dsn_main = "mysql+mysqlconnector://{username}:{password}@{host}:3306/dashboard".format(**db_config)
    dsn_fintweet = "mysql+mysqlconnector://{username}:{password}@{host}:3306/fintweet2".format(**db_config)
    # SQLALCHEMY_DATABASE_URI = 'sqlite:///%s/blog.db' % APPLICATION_DIR
    SQLALCHEMY_DATABASE_URI = dsn_main
    SQLALCHEMY_BINDS = {
        'dashboard': dsn_main,
        'fintweet': dsn_fintweet
    }
    SQLALCHEMY_TRACK_MODIFICATIONS = False
