from app import app, db  # import Flask app
import admin  # Admin module is loaded after the app
import api
import models
import views

from tweets.blueprint import tweets
from fintweet.views import fintweet
app.register_blueprint(tweets, url_prefix='/tweets')
app.register_blueprint(fintweet, url_prefix='/fintweet')

if __name__ == '__main__':
    app.run()