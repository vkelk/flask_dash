from application import db, api


class User(db.Model):
    __bind_key__ = 'fintweet'
    __table__ = db.Model.metadata.tables['user']

    # tweets = db.relationship('Tweet', backref='user_id', lazy='dynamic')

    def __repr__(self):
        return self.twitter_handle


class UserCount(db.Model):
    __bind_key__ = 'fintweet'
    __table__ = db.Model.metadata.tables['user_count']

    def __repr__(self):
        return self.user_id


class Tweet(db.Model):
    __bind_key__ = 'fintweet'
    __table__ = db.Model.metadata.tables['tweet']

    def __repr__(self):
        return self.tweet_id


class Retweet(db.Model):
    __bind_key__ = 'fintweet'
    __table__ = db.Model.metadata.tables['retweet']

    def __repr__(self):
        return self.retweet_id


class Reply(db.Model):
    __bind_key__ = 'fintweet'
    __table__ = db.Model.metadata.tables['reply']

    def __repr__(self):
        return self.reply_id


class TweetCashtag(db.Model):
    __bind_key__ = 'fintweet'
    __table__ = db.Model.metadata.tables['tweet_cashtags']

    def __repr__(self):
        return self.id


class TweetCount(db.Model):
    __bind_key__ = 'fintweet'
    __table__ = db.Model.metadata.tables['tweet_count']

    def __repr__(self):
        return self.tweet_id


class TweetHashtag(db.Model):
    __bind_key__ = 'fintweet'
    __table__ = db.Model.metadata.tables['tweet_hashtags']

    def __repr__(self):
        return self.id


class TweetMention(db.Model):
    __bind_key__ = 'fintweet'
    __table__ = db.Model.metadata.tables['tweet_mentions']

    def __repr__(self):
        return self.id


class TweetUrl(db.Model):
    __bind_key__ = 'fintweet'
    __table__ = db.Model.metadata.tables['tweet_url']

    def __repr__(self):
        return self.id


api.create_api(Tweet, methods=['GET'])
