from application import db


class Stocktwits(db.Model):
    __abstract__ = True


class User(Stocktwits):
    __tablename__ = 'user'
    __table_args__ = {"schema": "stocktwits"}

    user_id = db.Column(db.BigInteger, primary_key=True)
    user_handle = db.Column(db.String(75))
    user_name = db.Column(db.String(75))
    date_joined = db.Column(db.Date)
    website = db.Column(db.String(255))
    source = db.Column(db.String(255))
    user_topmentioned = db.Column(db.String(255))
    location = db.Column(db.String(255))
    verified = db.Column(db.String(10))

    ideas = db.relationship('Ideas', lazy='dynamic')
    counts = db.relationship(
        'stocktwits.UserCount',
        backref='user_count',
        primaryjoin="stocktwits.UserCount.user_id==User.user_id",
        lazy='dynamic')
    strategy = db.relationship(
        'stocktwits.UserStrategy',
        primaryjoin="stocktwits.UserStrategy.user_id==User.user_id",
        lazy='dynamic')

    def __repr__(self):
        return self.user_handle


class UserCount(Stocktwits):
    __tablename__ = 'user_count'
    __table_args__ = {"schema": "stocktwits"}

    user_id = db.Column(db.BigInteger, db.ForeignKey(User.user_id), primary_key=True)
    followers = db.Column(db.Integer)
    following = db.Column(db.Integer)
    watchlist_count = db.Column(db.Integer)
    watchlist_stocks = db.Column(db.String(500))
    ideas = db.Column(db.Integer)

    user = db.relationship('stocktwits.User', backref='counts')

    def __repr__(self):
        return self.user_id


class UserStrategy(db.Model):
    __tablename__ = 'user_strategy'
    __table_args__ = {"schema": "stocktwits"}

    id = db.Column(db.BigInteger, autoincrement=True)
    user_id = db.Column(db.BigInteger, db.ForeignKey(User.user_id), primary_key=True)
    assets_frequently_traded = db.Column(db.String(255))
    approach = db.Column(db.String(255))
    holding_period = db.Column(db.String(255))
    experience = db.Column(db.String(255))

    user = db.relationship('stocktwits.User', backref='strategy')

    def __repr__(self):
        return self.user_id


class Ideas(db.Model):
    __tablename__ = 'ideas'
    __table_args__ = {"schema": "stocktwits"}

    ideas_id = db.Column(db.BigInteger, primary_key=True)
    user_id = db.Column(db.BigInteger, db.ForeignKey(User.user_id))
    permno = db.Column(db.Integer)
    date = db.Column(db.Date)
    time = db.Column(db.Time)
    replied = db.Column(db.String(3))
    text = db.Column(db.Text)
    sentiment = db.Column(db.String(60))
    cashtags_other = db.Column(db.String(255))

    user = db.relationship('stocktwits.User')
    cashtags = db.relationship('stocktwits.IdeaCashtags')
    hashtags = db.relationship('stocktwits.IdeaHashtags')
    counts = db.relationship('stocktwits.IdeaCounts')
    urls = db.relationship('stocktwits.IdeaUrls')
    replys = db.relationship('stocktwits.Replys')

    def __repr__(self):
        return self.ideas_id


class IdeaCounts(db.Model):
    __tablename__ = 'ideas_count'
    __table_args__ = {"schema": "stocktwits"}

    ideas_id = db.Column(
        db.BigInteger, db.ForeignKey(Ideas.ideas_id), primary_key=True)
    replies = db.Column(db.Integer)
    likes = db.Column(db.Integer)

    idea = db.relationship('stocktwits.Ideas', backref='counts')

    def __repr__(self):
        return self.ideas_id


class IdeaCashtags(db.Model):
    __tablename__ = 'idea_cashtags'
    __table_args__ = {"schema": "stocktwits"}

    id = db.Column(db.BigInteger, autoincrement=True)
    ideas_id = db.Column(
        db.BigInteger, db.ForeignKey(Ideas.ideas_id), primary_key=True)
    cashtag = db.Column(db.String(60))

    idea = db.relationship('stocktwits.Ideas', backref='cashtags')

    def __repr__(self):
        return self.cashtag


class IdeaHashtags(db.Model):
    __tablename__ = 'idea_hashtags'
    __table_args__ = {"schema": "stocktwits"}

    id = db.Column(db.BigInteger, autoincrement=True)
    ideas_id = db.Column(
        db.BigInteger, db.ForeignKey(Ideas.ideas_id), primary_key=True)
    hashtag = db.Column(db.String(60))

    idea = db.relationship('stocktwits.Ideas', backref='hashtags')

    def __repr__(self):
        return self.hashtag


class IdeaUrls(db.Model):
    __tablename__ = 'ideas_url'
    __table_args__ = {"schema": "stocktwits"}

    id = db.Column(db.BigInteger, autoincrement=True)
    ideas_id = db.Column(
        db.BigInteger, db.ForeignKey(Ideas.ideas_id), primary_key=True)
    url = db.Column(db.String(255))
    link = db.Column(db.String(255))

    idea = db.relationship('stocktwits.Ideas', backref='urls')

    def __repr__(self):
        return self.url


class Replys(db.Model):
    __tablename__ = 'reply'
    __table_args__ = {"schema": "stocktwits"}

    reply_id = db.Column(db.BigInteger, primary_key=True, autoincrement=True)
    ideas_id = db.Column(db.BigInteger, db.ForeignKey(Ideas.ideas_id))
    date = db.Column(db.Date)
    time = db.Column(db.Time)
    reply_userid = db.Column(db.BigInteger)
    text = db.Column(db.Text)

    idea = db.relationship('stocktwits.Ideas', backref='replys')

    def __repr__(self):
        return self.reply_id
