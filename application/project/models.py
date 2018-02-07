import json, uuid
from datetime import datetime
from application import db, bcrypt, login_manager
from flask_login import AnonymousUserMixin, UserMixin
from sqlalchemy.ext.hybrid import hybrid_property, hybrid_method
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
    search_data = db.Column(db.JSON)
    event_dates = db.Column(db.JSON)
    event_stats = db.Column(db.JSON)
    created = db.Column(db.DateTime)

    def __init__(self, project_id, dataset, search, dates, stats):
        self.uuid = str(uuid.uuid1())
        self.project_id = project_id
        self.dataset = dataset
        self.search_data = json.dumps(search, ensure_ascii=False)
        self.event_dates = json.dumps(dates, ensure_ascii=False)
        self.event_stats = json.dumps(stats, ensure_ascii=False)
        self.created = datetime.now()
