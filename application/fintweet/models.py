from application import db


class DealNosFT(db.Model):
    __tablename__ = 'dealnos'
    __table_args__ = {"schema": "fintweet"}

    dealno = db.Column(db.BigInteger, primary_key=True)


class User(db.Model):
    __tablename__ = 'user'
    __table_args__ = {"schema": "fintweet"}

    user_id = db.Column(db.BigInteger, primary_key=True)
    twitter_handle = db.Column(db.String(120))
    user_name = db.Column(db.String(120))
    location = db.Column(db.String(255))
    date_joined = db.Column(db.Date)
    timezone = db.Column(db.String(10))
    website = db.Column(db.String(255))
    user_intro = db.Column(db.String(255))
    verified = db.Column(db.String(10))

    tweets = db.relationship('Tweet', lazy='dynamic')
    counts = db.relationship(
        "application.fintweet.models.UserCount", backref='user', uselist=False)
    mentions = db.relationship(
        'TweetMention',
        primaryjoin="TweetMention.user_id==application.fintweet.models.User.user_id",
        backref='user',
        lazy='dynamic')

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
    __tablename__ = 'user_count'
    __table_args__ = {"schema": "fintweet"}

    user_id = db.Column(db.BigInteger, db.ForeignKey(User.user_id), primary_key=True)
    # user_id = Column(BIGINT, ForeignKey('user.user_id'))
    follower = db.Column(db.Integer)
    following = db.Column(db.Integer)
    tweets = db.Column(db.Integer)
    likes = db.Column(db.Integer)
    lists = db.Column(db.Integer)

    def __repr__(self):
        return self.user_id


class Tweet(db.Model):
    __tablename__ = 'tweet'
    __table_args__ = {"schema": "fintweet"}

    tweet_id = db.Column(db.BigInteger, primary_key=True)
    date = db.Column(db.Date)
    time = db.Column(db.Time)
    timezone = db.Column(db.String(10))
    retweet_status = db.Column(db.String(10))
    text = db.Column(db.Text)
    location = db.Column(db.String(255))
    user_id = db.Column(db.BigInteger, db.ForeignKey(User.user_id))
    emoticon = db.Column(db.Text)
    reply_to = db.Column(db.BigInteger)
    permalink = db.Column(db.String(255))

    user = db.relationship('application.fintweet.models.User', uselist=False)

    # cashtags = db.relationship('TweetCashtag', lazy='dynamic')
    # counts = db.relationship('TweetCount', lazy='dynamic')
    # ment_s = relationship('TweetMentions')
    # cash_s = relationship('TweetCashtags')
    # hash_s = relationship('TweetHashtags')
    # url_s = relationship('TweetUrl')
    # replies = relationship('Reply')
    # retweets = relationship('Retweet')
    # emoticon = Column(TEXT)

    def __repr__(self):
        return self.tweet_id

    @property
    def as_dict(self):
        return {c.name: getattr(self, c.name) for c in self.__table__.columns}

    @property
    def user_name(self):
        return User.query.with_entities(User.user_name).filter(User.user_id == self.user_id) \
            .first_or_404()


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
    __tablename__ = 'tweet_cashtags'
    __table_args__ = {"schema": "fintweet"}

    id = db.Column(db.BigInteger, primary_key=True)
    # id = Column(INTEGER, primary_key=True, autoincrement=True)
    tweet_id = db.Column(db.BigInteger, db.ForeignKey(Tweet.tweet_id))
    permno = db.Column(db.Integer)
    cashtags = db.Column(db.String(120))

    # user_id = Column(BIGINT)

    tweets = db.relationship('Tweet', backref="cashtags")

    # user = db.relationship('User', backref='user', lazy='dynamic',
    #                        primaryjoin="TweetCashtag.tweet_id==Tweet.tweetr_id", foreign_keys='User.user_id')

    def __repr__(self):
        return self.id


class TweetCount(db.Model):
    __tablename__ = 'tweet_count'
    __table_args__ = {"schema": "fintweet"}

    tweet_id = db.Column(db.BigInteger, db.ForeignKey(Tweet.tweet_id), primary_key=True)
    reply = db.Column(db.Integer)
    retweet = db.Column(db.Integer)
    favorite = db.Column(db.Integer)

    tweets = db.relationship('Tweet', backref="counts")

    def __repr__(self):
        return self.tweet_id


class TweetHashtag(db.Model):
    __tablename__ = 'tweet_hashtags'
    __table_args__ = {"schema": "fintweet"}

    id = db.Column(db.BigInteger, primary_key=True)
    tweet_id = db.Column(db.BigInteger, db.ForeignKey(Tweet.tweet_id))
    hashtags = db.Column(db.String(120))

    tweets = db.relationship('Tweet', backref="hashtags")

    def __repr__(self):
        return self.id


class TweetMention(db.Model):
    __tablename__ = 'tweet_mentions'
    __table_args__ = {"schema": "fintweet"}

    id = db.Column(db.BigInteger, primary_key=True)
    tweet_id = db.Column(db.BigInteger, db.ForeignKey(Tweet.tweet_id))
    mentions = db.Column(db.String(120))
    user_id = db.Column(db.BigInteger, db.ForeignKey(User.user_id))

    def __repr__(self):
        return self.id


class TweetUrl(db.Model):
    __tablename__ = 'tweet_url'
    __table_args__ = {"schema": "fintweet"}

    id = db.Column(db.BigInteger, primary_key=True)
    tweet_id = db.Column(db.BigInteger, db.ForeignKey(Tweet.tweet_id))
    url = db.Column(db.String(255))
    link = db.Column(db.String(255))

    def __repr__(self):
        return self.id


class TweetSentiment(db.Model):
    __tablename__ = 'tweet_sentiment'
    __table_args__ = {"schema": "fintweet"}

    tweet_id = db.Column(db.BigInteger, db.ForeignKey(Tweet.tweet_id), primary_key=True)
    sentiment = db.Column(db.String(255))

    tweets = db.relationship('Tweet', backref="sentiment")


class TopCashtags(db.Model):
    __tablename__ = 'top_cashtags'
    __table_args__ = {"schema": "fintweet"}

    cashtags = db.Column(db.String(60), primary_key=True)
    tweets_count = db.Column(db.Integer)


class TopHashtags(db.Model):
    __tablename__ = 'top_hashtags'
    __table_args__ = {"schema": "fintweet"}

    hashtags = db.Column(db.String(60), primary_key=True)
    tweets_count = db.Column(db.Integer)


class TopUsers(db.Model):
    __tablename__ = 'top_users'
    __table_args__ = {"schema": "fintweet"}

    user_id = db.Column(db.BigInteger, primary_key=True)
    twitter_handle = db.Column(db.String(120))
    tweets_count = db.Column(db.Integer)


class mvCashtags(db.Model):
    __tablename__ = 'mv_cashtags'
    __table_args__ = {"schema": "fintweet"}

    id = db.Column(db.BigInteger, primary_key=True)
    tweet_id = db.Column(db.BigInteger)
    user_id = db.Column(db.BigInteger)
    cashtags = db.Column(db.String(120))
    datetime = db.Column(db.DateTime)
