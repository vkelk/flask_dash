from flask import flash, redirect, render_template, request, url_for

from application import app


@app.route('/')
def homepage():
    # app.logger.info("Homepage accessed.")
    return render_template('homepage.html')
