import os
from db_config import db_config


class Configuration(object):
    APPLICATION_DIR = os.path.dirname(os.path.realpath(__file__))
    DEBUG = True
    # The SQLALCHEMY_DATABASE_URI comprises the following parts:
    # dialect+driver://username:password@host:port/database
    dsn = "mysql+mysqlconnector://{username}:{password}@{host}:3306/{database}".format(**db_config)
    # SQLALCHEMY_DATABASE_URI = 'sqlite:///%s/blog.db' % APPLICATION_DIR
    SQLALCHEMY_DATABASE_URI = dsn
    SQLALCHEMY_TRACK_MODIFICATIONS = False

