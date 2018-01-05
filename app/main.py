from app import app, db  # import Flask app
import admin  # Admin module is loaded after the app
import api
import models
import views

from tweets.blueprint import tweets
app.register_blueprint(tweets, url_prefix='/tweets')


if __name__ == '__main__':
    app.run()