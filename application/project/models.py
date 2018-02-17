import json, uuid
from datetime import datetime
from application import db
from application.account.models import Account


class Project(db.Model):
    __tablename__ = 'projects'
    __table_args__ = {"schema": "dashboard"}

    uuid = db.Column(db.String(36), primary_key=True)
    account_id = db.Column(db.Integer, db.ForeignKey(Account.id))
    name = db.Column(db.String(60))
    description = db.Column(db.Text)
    date_start = db.Column(db.Date)
    date_end = db.Column(db.Date)
    active = db.Column(db.Boolean)
    file_output = db.Column(db.String)

    details = db.relationship('ProjectDetais', backref='project_details', lazy='dynamic')

    def __init__(self, name, description, date_start, date_end):
        self.uuid = str(uuid.uuid1())
        self.name = name
        self.description = description
        self.date_start = date_start
        self.date_end = date_end


class ProjectDetais(db.Model):
    __tablename__ = 'project_details'
    __table_args__ = {"schema": "dashboard"}

    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    project_id = db.Column(db.String(36), db.ForeignKey(Project.uuid))
    search_type = db.Column(db.String(15))
    search_text = db.Column(db.JSON)
    active = db.Column(db.Boolean)


class Dataset(db.Model):
    __tablename__ = 'datasets'
    __table_args__ = {"schema": "dashboard"}

    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    dataset_db = db.Column(db.String(15))
    dataset_name = db.Column(db.String(30))
    created = db.DateTime()


class Event(db.Model):
    __tablename__ = 'events'
    __table_args__ = {"schema": "dashboard"}

    uuid = db.Column(db.String(36), primary_key=True)
    project_id = db.Column(db.String(36), db.ForeignKey(Project.uuid))
    active = db.Column(db.Boolean)
    dataset = db.Column(db.String(30))
    type = db.Column(db.String(15))
    text = db.Column(db.String(30))
    event_date = db.Column(db.Date)
    event_start = db.Column(db.Date)
    event_end = db.Column(db.Date)
    event_pre_start = db.Column(db.Date)
    event_pre_end = db.Column(db.Date)
    event_post_start = db.Column(db.Date)
    event_post_end = db.Column(db.Date)
    days_pre = db.Column(db.Integer)
    days_post = db.Column(db.Integer)
    days_estimation = db.Column(db.Integer)
    days_grace = db.Column(db.Integer)
    created = db.Column(db.DateTime)

    stats = db.relationship('EventStats', backref='event_stats', lazy='dynamic')

    def __init__(self, project_id, dataset):
        # self.uuid = str(uuid.uuid1())
        self.project_id = project_id
        self.dataset = dataset
        self.created = datetime.now()


class EventStats(db.Model):
    __tablename__ = 'event_stats'
    __table_args__ = {"schema": "dashboard"}

    uuid = db.Column(db.String(36), db.ForeignKey(Event.uuid), primary_key=True)
    event_total = db.Column(db.Integer)
    event_median = db.Column(db.Float)
    event_mean = db.Column(db.Float)
    pre_total = db.Column(db.Integer)
    pre_median = db.Column(db.Float)
    pre_mean = db.Column(db.Float)
    post_total = db.Column(db.Integer)
    post_median = db.Column(db.Float)
    post_mean = db.Column(db.Float)
    created = db.Column(db.DateTime)
    pre_std = db.Column(db.Float)
    event_std = db.Column(db.Float)
    post_std = db.Column(db.Float)
    pct_change = db.Column(db.Float)
    pre_bullish = db.Column(db.Integer)
    event_bullish = db.Column(db.Integer)
    post_bullish = db.Column(db.Integer)
    pre_bearish = db.Column(db.Integer)
    event_bearish = db.Column(db.Integer)
    post_bearish = db.Column(db.Integer)
    pre_sentiment = db.Column(db.Float)
    event_sentiment = db.Column(db.Float)
    post_sentiment = db.Column(db.Float)
    pre_positive = db.Column(db.Integer)
    pre_negative = db.Column(db.Integer)
    event_positive = db.Column(db.Integer)
    event_negative = db.Column(db.Integer)
    post_positive = db.Column(db.Integer)
    post_negative = db.Column(db.Integer)
    users_pre = db.Column(db.Integer)
    users_event = db.Column(db.Integer)
    users_post = db.Column(db.Integer)
    users_pre_bullish = db.Column(db.Integer)
    users_pre_bearish = db.Column(db.Integer)
    users_event_bullish = db.Column(db.Integer)
    users_event_bearish = db.Column(db.Integer)
    users_post_bullish = db.Column(db.Integer)
    users_post_bearish = db.Column(db.Integer)

    def __init__(self, uuid):
        self.uuid = uuid
        self.created = datetime.now()


class EventTweets(db.Model):
    __tablename__ = 'event_tweets'
    __table_args__ = {"schema": "dashboard"}

    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    event_uuid = db.Column(db.String(36), db.ForeignKey(Event.uuid))
    event_period = db.Column(db.String(15))
    tweet_id = db.Column(db.BigInteger)

    def __init__(self, uuid, event_period, tweet_id):
        self.event_uuid = uuid
        self.event_period = event_period
        self.tweet_id = tweet_id
