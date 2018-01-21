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
    __tablename__ = 'accounts'
    __table_args__ = {"schema": "dashboard"}

    # __table__ = db.Model.metadata.tables['accounts']

    id = db.Column(db.Integer, primary_key=True)

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
        self._password = bcrypt.generate_password_hash(password)

    @hybrid_method
    def is_correct_password(self, password):
        return bcrypt.check_password_hash(self._password, password)

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

#
# class AnonymousUser(AnonymousUserMixin):
#     def can(self, _):
#         return False
#
#     def is_admin(self):
#         return False
#
#
# login_manager.anonymous_user = AnonymousUser
#
#
# @login_manager.user_loader
# def load_user(user_id):
#     return Account.query.get(int(user_id))
