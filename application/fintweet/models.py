from flask import url_for, current_app
from sqlalchemy import func
from flask_login import AnonymousUserMixin, UserMixin
from application import db


class DealNosFT(db.Model):
    # __bind_key__ = 'fintweet'
    # __table__ = db.Model.metadata.tables['dealnos']

    __tablename__ = 'dealnos'
    __table_args__ = {"schema": "fintweet"}

    dealno = db.Column(db.BigInteger, primary_key=True)


class User(UserMixin, db.Model):
    # __bind_key__ = 'fintweet'
    # __table__ = db.Model.metadata.tables['user']

    __tablename__ = 'user'
    __table_args__ = {"schema": "fintweet"}

    user_id = db.Column(db.BigInteger, primary_key=True)

    # tweets = db.relationship('Tweet', backref='user_id', lazy='dynamic')

    def __repr__(self):
        return self.twitter_handle

    @property
    def serialize(self):
        """Return object data in easily serializeable format"""
        return {
            'id': self.user_id,
            'handle': self.twitter_handle,
            'name': self.user_name,
            # 'url': url_for('fintweet.api_users_by_id', user_id=self.user_id)
        }

    @property
    def as_dict(self):
        return {c.name: getattr(self, c.name) for c in self.__table__.columns}


class UserCount(db.Model):
    # __bind_key__ = 'fintweet'
    # __table__ = db.Model.metadata.tables['user_count']

    __tablename__ = 'user_count'
    __table_args__ = {"schema": "fintweet"}

    user_id = db.Column(db.BigInteger, primary_key=True)
    # user = db.relationship('User')
    user = db.relationship('User', backref='user_count', lazy='dynamic',
                           primaryjoin="UserCount.user_id==User.user_id", foreign_keys='User.user_id')

    def __repr__(self):
        return self.user_id


class Tweet(UserMixin, db.Model):
    # __bind_key__ = 'fintweet'
    # __table__ = db.Model.metadata.tables['tweet']

    __tablename__ = 'tweet'
    __table_args__ = {"schema": "fintweet"}

    tweet_id = db.Column(db.BigInteger, primary_key=True)
    user = db.relationship('User')

    def __repr__(self):
        return self.tweet_id

    # @property
    # def user_name(self):
    #     return User.query.with_entities(User.user_name).filter(User.user_id == self.user_id).first_or_404()


# class Retweet(db.Model):
#     __bind_key__ = 'fintweet'
#     __table__ = db.Model.metadata.tables['retweet']
#
#     def __repr__(self):
#         return self.retweet_id


# class Reply(db.Model):
#     __bind_key__ = 'fintweet'
#     __table__ = db.Model.metadata.tables['reply']
#
#     def __repr__(self):
#         return self.reply_id


class TweetCashtag(db.Model):
    # __bind_key__ = 'fintweet'
    # __table__ = db.Model.metadata.tables['tweet_cashtags']

    __tablename__ = 'tweet_cashtags'
    __table_args__ = {"schema": "fintweet"}

    id = db.Column(db.BigInteger, db.Sequence('tweet_cashtags_id_seq'), primary_key=True)
    tweet = db.relationship('Tweet')

    # user = db.relationship('User', backref='user_count', lazy='dynamic',
    #                        primaryjoin="TweetCashtag.user_id==User.user_id", foreign_keys='User.user_id')

    def __repr__(self):
        return self.id


class TweetCount(db.Model):
    # __bind_key__ = 'fintweet'
    # __table__ = db.Model.metadata.tables['tweet_count']

    __tablename__ = 'tweet_count'
    __table_args__ = {"schema": "fintweet"}

    id = db.Column(db.BigInteger, db.Sequence('tweet_count_id_seq'), primary_key=True)

    def __repr__(self):
        return self.tweet_id


class TweetHashtag(db.Model):
    # __bind_key__ = 'fintweet'
    # __table__ = db.Model.metadata.tables['tweet_hashtags']

    __tablename__ = 'tweet_hashtags'
    __table_args__ = {"schema": "fintweet"}

    id = db.Column(db.BigInteger, db.Sequence('tweet_hashtags_id_seq'), primary_key=True)

    def __repr__(self):
        return self.id


class TweetMention(db.Model):
    # __bind_key__ = 'fintweet'
    # __table__ = db.Model.metadata.tables['tweet_mentions']

    __tablename__ = 'tweet_mentions'
    __table_args__ = {"schema": "fintweet"}

    id = db.Column(db.BigInteger, db.Sequence('tweet_mentions_id_seq'), primary_key=True)

    def __repr__(self):
        return self.id


class TweetUrl(db.Model):
    # __bind_key__ = 'fintweet'
    # __table__ = db.Model.metadata.tables['tweet_url']

    __tablename__ = 'tweet_url'
    __table_args__ = {"schema": "fintweet"}

    id = db.Column(db.BigInteger, db.Sequence('tweet_url_id_seq'), primary_key=True)

    def __repr__(self):
        return self.id

# apimanager.create_api(Tweet, methods=['GET'])
# api.add_resource(Tweet, "/tweet")
# api.init_app(current_app)
# with current_app.app_context():
# reflection to get table meta
# pass
