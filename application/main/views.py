from flask import render_template

from application.main import main


@main.route('/')
def home():
    return render_template('home.html')
