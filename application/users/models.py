import datetime, re
from application import db, bcrypt
from sqlalchemy.ext.hybrid import hybrid_property


class DashUser(db.Model):
    __bind_key__ = 'dashboard'
    __table__ = db.Model.metadata.tables['users']

    STATUS_ACTIVE = True
    STATUS_DISABLED = False

    @hybrid_property
    def password(self):
        """The bcrypt'ed password of the given user."""
        return self.password_hash

    @password.setter
    def password(self, raw_password):
        """Bcrypt the password on assignment."""
        self.password_hash = bcrypt.generate_password_hash(raw_password)

    def __repr__(self):
        return '<User: {!r}>'.format(self.email)

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
