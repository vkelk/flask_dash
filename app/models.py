import datetime, re

from app import db


def slugify(s):
    """
    Turns string to lowercase and replace whitespace to '-'
    :param s: string
    :return: string
    """
    return re.sub('[^\w]+', '-', s).lower()


class Entry(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    title = db.Column(db.String(100))
    slug = db.Column(db.String(100), unique=True)
    body = db.Column(db.Text)
    created_timestamp = db.Column(db.DateTime, default=datetime.datetime.now)
    modified_timestamp = db.Column(db.DateTime, default=datetime.datetime.now, onupdate=datetime.datetime.now)

    def __init__(self, *args, **kwargs):
        super(Entry, self).__init__(*args, **kwargs)  # Call parent constructor
        self.generate_slug()

    def generate_slug(self):
        self.slug = ''
        if self.title:
            self.slug = slugify(self.title)

    def __repr__(self):
        return '<Entry: %s>' % self.title


class User(db.Model):
    __tablename__ = 'user'
    user_id = db.Column(db.BigInteger, primary_key=True)
    twitter_handle = db.Column(db.String(120))
    user_name = db.Column(db.String(120))
    location = db.Column(db.String(120))
    date_joined = db.Column(db.Date)
    timezone = db.Column(db.String(10))
    website = db.Column(db.String(75))
    user_intro = db.Column(db.String(100))
    verified = db.Column(db.String(10))
    # tweets = relationship('Tweet')
    # counts = relationship('UserCount')
