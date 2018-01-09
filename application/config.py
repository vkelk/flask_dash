import logging
import os
from db_config import db_config, MAILGUN_KEY as MK, MAILGUN_DOMAIN as MD


class Configuration(object):
    # Statement for enabling the development environment
    DEBUG = True

    # Define the application directory
    APPLICATION_DIR = os.path.dirname(os.path.realpath(__file__))
    BASE_DIR = os.path.abspath(os.path.dirname(__file__))

    # Define the database - we are working with
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
    SQLALCHEMY_POOL_RECYCLE = 300
    SQLALCHEMY_TRACK_MODIFICATIONS = False

    # Application threads. A common general assumption is
    # using 2 per available processor cores - to handle
    # incoming requests using one and performing background
    # operations using the other.
    THREADS_PER_PAGE = 2

    # Enable protection agains *Cross-site Request Forgery (CSRF)*
    CSRF_ENABLED = True

    # Use a secure, unique and absolutely secret key for
    # signing the data.
    CSRF_SESSION_KEY = "protectyoursecrets"

    # Secret key for signing cookies
    SECRET_KEY = 'protectyoursecrets'  # Create a unique key for your app

    LOG_LEVEL = logging.WARNING

    MAILGUN_KEY = MK
    MAILGUN_DOMAIN = MD
