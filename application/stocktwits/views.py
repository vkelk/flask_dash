from flask import request, render_template, url_for, jsonify, Response, Markup, flash, abort, send_file, make_response
from flask_login import login_required
from sqlalchemy import func
from sqlalchemy.exc import IntegrityError
from werkzeug.utils import secure_filename, CombinedMultiDict
from application.config import base_config
from application.stocktwits import stocktwit
from application.stocktwits.models import db, IdeaCashtags, IdeaHashtags, Ideas, User
# from .forms import *

# from .helpers import object_list


@stocktwit.route('/')
@login_required
def index():
    return render_template('stocktwits/index.html')


@stocktwit.route('/users')
@login_required
def users():
    return render_template('stocktwits/users.html')


@stocktwit.route('/tweets')
@login_required
def tweets():
    return render_template('stocktwits/tweets.html')


@stocktwit.route('/ajax_topctags')
def ajax_topctags():
    q = db.session.query(IdeaCashtags.cashtag, func.count(IdeaCashtags.cashtag).label('count')) \
        .group_by(IdeaCashtags.cashtag).order_by('count desc').limit(25).all()
    return jsonify(q)


@stocktwit.route('/ajax_tophtags')
def ajax_tophtags():
    q = db.session.query(IdeaHashtags.hashtag, func.count(IdeaHashtags.hashtag).label('count')) \
        .group_by(IdeaHashtags.hashtag).order_by('count desc').limit(25).all()
    return jsonify(q)


@stocktwit.route('/ajax_topusers/<limit>')
def ajax_topusers(limit=25):
    q = db.session.query(Ideas.user_id, User.user_handle, User.date_joined,
                         func.count(Ideas.ideas_id).label('count')) \
        .select_from(User).join(Ideas).group_by(Ideas.user_id).group_by(User.user_handle).group_by(User.date_joined) \
        .order_by('count desc').limit(limit).all()
    return jsonify(q)


@stocktwit.route('/ajax_tweetcount_timeline')
def ajax_tweetcount_timeline():
    q = db.session.query(func.to_char(func.date_trunc('month', Ideas.datetime), 'YYYY-MM-DD').label('month'),
                         func.count(Ideas.ideas_id).label('count')) \
        .group_by('month')
    return jsonify(q.all())


@stocktwit.route('/ajax_ctags_by_user/<user_id>')
def ajax_ctags_by_user(user_id):
    q = db.session.query(IdeaCashtags.cashtag, func.count(IdeaCashtags.ideas_id).label('count')) \
        .select_from(IdeaCashtags).join(Ideas).filter(Ideas.user_id == user_id).group_by(IdeaCashtags.cashtag) \
        .order_by('count desc').limit(10).all()
    return jsonify(q)


@stocktwit.route('/ajax_htags_by_user/<user_id>')
def ajax_htags_by_user(user_id):
    q = db.session.query(IdeaHashtags.hashtag, func.count(IdeaHashtags.ideas_id).label('count')) \
        .select_from(IdeaHashtags).join(Ideas).filter(Ideas.user_id == user_id).group_by(IdeaHashtags.hashtag) \
        .order_by('count desc').limit(10).all()
    return jsonify(q)
