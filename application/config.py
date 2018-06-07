import logging
import os
from dotenv import load_dotenv
# from application.db_config import db_config, pg_config, MAILGUN_KEY as MK, MAILGUN_DOMAIN as MD

APP_DIR = os.path.abspath(os.path.join(os.path.abspath(__file__), os.pardir))
PROJECT_DIR = os.path.abspath(os.path.join(APP_DIR, os.pardir))
load_dotenv(os.path.join(PROJECT_DIR, '.env'))


class base_config(object):
    """Default configuration options."""

    # Define the application directory
    UPLOAD_FOLDER = os.path.join(os.path.realpath(PROJECT_DIR), 'uploads/')
    ALLOWED_EXTENSIONS = {'txt', 'csv', 'xls', 'xlsx'}

    SITE_NAME = os.environ.get('SITE_NAME', 'Dashboard')

    SECRET_KEY = os.environ.get('SECRET_KEY', 'secrets')
    # SERVER_NAME = os.environ.get('SERVER_NAME', 'localhost')

    CSRF_SESSION_KEY = os.getenv("SECRET_KEY") or "secrets"

    MAIL_SERVER = os.environ.get('MAIL_SERVER', '127.0.0.1')
    MAIL_PORT = os.environ.get('MAIL_PORT', 25)
    MAILGUN_KEY = os.environ.get('MAILGUN_KEY', None)
    MAILGUN_DOMAIN = os.environ.get('MAILGUN_DOMAIN', None)

    REDIS_HOST = os.environ.get('REDIS_HOST', '127.0.0.1')
    REDIS_PORT = os.environ.get('REDIS_PORT', 6379)
    RQ_REDIS_URL = 'redis://{}:{}'.format(REDIS_HOST, REDIS_PORT)

    CACHE_HOST = os.environ.get('MEMCACHED_HOST', '127.0.0.1')
    CACHE_PORT = os.environ.get('MEMCACHED_PORT', 11211)

    POSTGRES_HOST = os.environ.get('POSTGRES_HOST', '127.0.0.1')
    POSTGRES_PORT = os.environ.get('POSTGRES_PORT', 5432)
    POSTGRES_USER = os.environ.get('DB_ENV_USER', 'postgres')
    POSTGRES_PASS = os.environ.get('DB_ENV_PASS', 'postgres')
    POSTGRES_DB = os.environ.get('POSTGRES_DB', 'postgres')

    SQLALCHEMY_DATABASE_URI = 'postgresql://%s:%s@%s:%s/%s' % (
        POSTGRES_USER,
        POSTGRES_PASS,
        POSTGRES_HOST,
        POSTGRES_PORT,
        POSTGRES_DB
    )
    SQLALCHEMY_POOL_RECYCLE = 300
    SQLALCHEMY_TRACK_MODIFICATIONS = False

    LOG_LEVEL = logging.WARNING

    # Application threads. A common general assumption is using 2 per available processor cores
    # to handle incoming requests using one and performing background operations using the other.
    THREADS_PER_PAGE = 2


class dev_config(base_config):
    """Development configuration options."""
    ASSETS_DEBUG = True
    DEBUG = True
    DEBUG_TB_INTERCEPT_REDIRECTS = False
    FLASK_ENV = 'development'
    ENV = 'development'
    # SERVER_NAME = 'localhost:5000'
    TESTING = True
    WTF_CSRF_ENABLED = False


class test_config(base_config):
    """Testing configuration options."""
    TESTING = True
    WTF_CSRF_ENABLED = False
