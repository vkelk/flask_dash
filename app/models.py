import datetime, re

from app import db
from app import bcrypt
from app import login_manager

db.reflect()  # reflection to get table meta


@login_manager.user_loader
def _user_loader(user_id):
    """
    Determine which user is logged in
    This way Flask-Login knows how to convert a user ID into a User object,
    and that user will be available to us as g.user
    :param user_id:
    :return:
    """
    return DashUser.query.get(int(user_id))


def slugify(s):
    """
    Turns string to lowercase and replace whitespace to '-'
    :param s: string
    :return: string
    """
    return re.sub('[^\w]+', '-', s).lower()


class User(db.Model):
    __bind_key__ = 'fintweet'
    __table__ = db.Model.metadata.tables['user']

    # tweets = db.relationship('Tweet', backref='user_id', lazy='dynamic')

    def __repr__(self):
        return self.twitter_handle


class UserCount(db.Model):
    __bind_key__ = 'fintweet'
    __table__ = db.Model.metadata.tables['user_count']

    def __repr__(self):
        return self.user_id


class Tweet(db.Model):
    __bind_key__ = 'fintweet'
    __table__ = db.Model.metadata.tables['tweet']

    def __repr__(self):
        return self.tweet_id


class Retweet(db.Model):
    __bind_key__ = 'fintweet'
    __table__ = db.Model.metadata.tables['retweet']

    def __repr__(self):
        return self.retweet_id


class Reply(db.Model):
    __bind_key__ = 'fintweet'
    __table__ = db.Model.metadata.tables['reply']

    def __repr__(self):
        return self.reply_id


class TweetCashtag(db.Model):
    __bind_key__ = 'fintweet'
    __table__ = db.Model.metadata.tables['tweet_cashtags']

    def __repr__(self):
        return self.id


class TweetCount(db.Model):
    __bind_key__ = 'fintweet'
    __table__ = db.Model.metadata.tables['tweet_count']

    def __repr__(self):
        return self.tweet_id


class TweetHashtag(db.Model):
    __bind_key__ = 'fintweet'
    __table__ = db.Model.metadata.tables['tweet_hashtags']

    def __repr__(self):
        return self.id


class TweetMention(db.Model):
    __bind_key__ = 'fintweet'
    __table__ = db.Model.metadata.tables['tweet_mentions']

    def __repr__(self):
        return self.id


class TweetUrl(db.Model):
    __bind_key__ = 'fintweet'
    __table__ = db.Model.metadata.tables['tweet_url']

    def __repr__(self):
        return self.id


class DashUser(db.Model):
    __bind_key__ = 'dashboard'
    __table__ = db.Model.metadata.tables['users']

    STATUS_ACTIVE = True
    STATUS_DISABLED = False

    # Flask-Login interface
    def get_id(self):
        return self.id

    def is_authenticated(self):
        return True

    def is_active(self):
        return self.active

    def is_admin(self):
        return self.admin

    def is_anonymous(self):
        return False

    @staticmethod
    def make_password(plaintext):
        """
        Accepts a plaintext password and returns the hashed version
        :param plaintext:
        :return:
        """
        return bcrypt.generate_password_hash(plaintext)

    def check_password(self, raw_password):
        """
        Accepts a plaintext password and determines whether it matches the hashed version stored in the database
        :param raw_password:
        :return:
        """
        return bcrypt.check_password_hash(self.password_hash, raw_password)

    @staticmethod
    def create(email, password, **kwargs):
        """
        Creates a new user, automatically hashing the password before saving
        :param email:
        :param password:
        :param kwargs:
        :return:
        """
        return DashUser(
            email=email,
            password_hash=DashUser.make_password(password),
            **kwargs
        )

    @staticmethod
    def authenticate(email, password):
        """
        Retrieve a user given a username and password
        :param email:
        :param password:
        :return:
        """
        user = DashUser.query.filter(DashUser.email == email).first()
        if user and user.check_password(password):
            return user
        return False
