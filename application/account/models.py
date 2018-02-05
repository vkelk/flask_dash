from datetime import datetime
from application import db, bcrypt, login_manager
from flask_login import AnonymousUserMixin, UserMixin
from sqlalchemy.ext.hybrid import hybrid_property, hybrid_method


# @login_manager.user_loader
# def _user_loader(user_id):
#     """
#     Determine which user is logged in
#     This way Flask-Login knows how to convert a user ID into a User object,
#     and that user will be available to us as g.user
#     :param user_id:
#     :return:
#     """
#     return Account.query.get(int(user_id))


class Account(UserMixin, db.Model):
    # __bind_key__ = 'dashboard'
    # __tablename__ = 'accounts'
    # __table_args__ = {"schema":"dashboard"}
    # __tablename__ = db.Model.metadata.tables['dashboard.accounts']
    __tablename__ = 'accounts'
    __table_args__ = {"schema": "dashboard"}
    # __mapper_args__ = {'primary_key': [__tablename__.c.id]}
    id = db.Column(db.Integer, primary_key=True)
    email = db.Column(db.String, unique=True, nullable=False)
    _password = db.Column(db.Binary(60), nullable=False)
    authenticated = db.Column(db.Boolean, default=False)
    email_confirmation_sent_on = db.Column(db.DateTime, nullable=True)
    email_confirmed = db.Column(db.Boolean, nullable=True, default=False)
    email_confirmed_on = db.Column(db.DateTime, nullable=True)
    registered_on = db.Column(db.DateTime, nullable=True)
    last_logged_in = db.Column(db.DateTime, nullable=True)
    current_logged_in = db.Column(db.DateTime, nullable=True)
    role = db.Column(db.String, default='user')

    def __init__(self, email, password, email_confirmation_sent_on=None, role='user'):
        self.email = email
        self._password = bcrypt.generate_password_hash(password)
        self.authenticated = False
        self.email_confirmation_sent_on = email_confirmation_sent_on
        self.email_confirmed = False
        self.email_confirmed_on = None
        self.registered_on = datetime.now()
        self.last_logged_in = None
        self.current_logged_in = datetime.now()
        self.role = role

    @hybrid_property
    def password(self):
        return self._password

    @password.setter
    def set_password(self, password):
        self._password = bcrypt.generate_password_hash(password).decode('utf-8')

    @hybrid_method
    def is_correct_password(self, password):
        return bcrypt.check_password_hash(self._password, password.encode('utf-8'))

    @property
    def is_authenticated(self):
        """Return True if the user is authenticated."""
        return self.authenticated

    @property
    def is_active(self):
        """Always True, as all users are active."""
        return True

    @property
    def is_email_confirmed(self):
        """Return True if the user confirmed their email address."""
        if self.email_confirmed:
            return True
        # return self.email_confirmed

    @property
    def is_anonymous(self):
        """Always False, as anonymous users aren't supported."""
        return False

    def get_id(self):
        """Return the email address to satisfy Flask-Login's requirements."""
        """Requires use of Python 3"""
        return str(self.id)

    def __repr__(self):
        return '<User {}>'.format(self.email)


class Project(db.Model):
    __tablename__ = 'projects'
    __table_args__ = {"schema": "dashboard"}

    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    account_id = db.Column(db.Integer, db.ForeignKey(Account.id))
    name = db.Column(db.String(60))
    description = db.Column(db.Text)
    date_start = db.Column(db.Date)
    date_end = db.Column(db.Date)
    active = db.Column(db.Boolean)

    details = db.relationship('ProjectDetais', backref='project_details', lazy='dynamic')

    def __init__(self, name, description, date_start, date_end):
        self.name = name
        self.description = description
        self.date_start = date_start
        self.date_end = date_end


class ProjectDetais(db.Model):
    __tablename__ = 'project_detailss'
    __table_args__ = {"schema": "dashboard"}

    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    project_id = db.Column(db.Integer, db.ForeignKey(Project.id))
    search_type = db.Column(db.String(15))
    search_text = db.Column(db.JSON)
    active = db.Column(db.Boolean)
