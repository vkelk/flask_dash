from flask import render_template, url_for, current_app
from flask_login import current_user
from application.main import main


@main.route('/')
def home():
    if current_user.is_authenticated:
        print(current_user)
    return render_template('home.html')


def has_no_empty_params(rule):
    defaults = rule.defaults if rule.defaults is not None else ()
    arguments = rule.arguments if rule.arguments is not None else ()
    return len(defaults) >= len(arguments)


@main.route("/site-map")
def site_map():
    links = []
    for rule in current_app.url_map.iter_rules():
        # Filter out rules we can't navigate to in a browser
        # and rules that require parameters
        if "GET" in rule.methods and has_no_empty_params(rule):
            url = url_for(rule.endpoint, **(rule.defaults or {}))
            links.append((url, rule.endpoint))
            links.reverse()
    return render_template('site-map.html', links=links)
    # links is now a list of url, endpoint tuples
