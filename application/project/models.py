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


class Event(db.Model):
    __tablename__ = 'events'
    __table_args__ = {"schema": "dashboard"}

    uuid = db.Column(db.String(36), primary_key=True)
    project_id = db.Column(db.String(36), db.ForeignKey(Project.uuid))
    active = db.Column(db.Boolean)
    dataset = db.Column(db.String(30))
    type = db.Column(db.String(15))
    text = db.Column(db.String(30))
    days_pre = db.Column(db.Integer)
    days_post = db.Column(db.Integer)
    days_estimation = db.Column(db.Integer)
    days_grace = db.Column(db.Integer)
    created = db.Column(db.DateTime)

    def __init__(self, project_id, dataset, code_type, text, days_pre, days_post, days_estimation, days_grace):
        self.uuid = str(uuid.uuid1())
        self.project_id = project_id
        self.dataset = dataset
        self.type = code_type
        self.text = text
        self.days_pre = days_pre
        self.days_post = days_post
        self.days_estimation = days_estimation
        self.days_grace = days_grace
        self.created = datetime.now()


class Dataset(db.Model):
    __tablename__ = 'datasets'
    __table_args__ = {"schema": "dashboard"}

    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    dataset_db = db.Column(db.String(15))
    dataset_name = db.Column(db.String(30))
    created = db.DateTime()


class EventStats(db.Model):
    __tablename__ = 'event_stats'
    __table_args__ = {"schema": "dashboard"}

    uuid = db.Column(db.String(36), db.ForeignKey(Event.uuid), primary_key=True)
    event_total = db.Column(db.Integer)
    event_median = db.Column(db.Integer)
    event_mean = db.Column(db.Integer)
    pre_total = db.Column(db.Integer)
    pre_median = db.Column(db.Integer)
    pre_mean = db.Column(db.Integer)
    post_total = db.Column(db.Integer)
    post_median = db.Column(db.Integer)
    post_mean = db.Column(db.Integer)
    created = db.Column(db.DateTime)

    def __init__(self, uuid, event_total, event_median, event_mean, pre_total, pre_median, pre_mean, post_total,
                 post_median, post_mean):
        self.uuid = uuid
        self.event_total = event_total
        self.event_median = event_median
        self.event_mean = event_mean
        self.pre_total = pre_total
        self.pre_median = pre_median
        self.pre_mean = pre_mean
        self.post_total = post_total
        self.post_median = post_median
        self.post_mean = post_mean
        self.created = datetime.now()


class EventTweets(db.Model):
    __tablename__ = 'event_tweets'
    __table_args__ = {"schema": "dashboard"}

    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    event_uuid = db.Column(db.String(36), db.ForeignKey(Event.uuid))
    event_period = db.Column(db.String(15))
    tweet_id = db.Column(db.BigInteger)

    def __init__(self, uuid, event_period, tweet_id):
        self.uuid = uuid
        self.event_period = event_period
        self.tweet_id = tweet_id
