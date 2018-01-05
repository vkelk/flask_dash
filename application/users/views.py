from flask import Blueprint, request, flash, redirect, url_for, render_template
from flask_login import login_user, logout_user

from application.users.forms import LoginForm

users = Blueprint('users', __name__, template_folder='templates')


@users.route('/login/', methods=["GET", "POST"])
def login():
    if request.method == "POST":
        form = LoginForm(request.form)
        if form.validate():
            login_user(form.user, remember=form.remember_me.data)
            flash("Successfully logged in as %s" % form.user.email, "success")
            return redirect(request.args.get("next") or url_for("homepage"))
        else:
            #     app.logger.warning("'{user}' failed to login successfully.".format(user=form.user.email))
            return redirect(url_for("login"))
    else:
        form = LoginForm()
        return render_template("login.html", form=form)


@users.route('/logout/')
def logout():
    logout_user()
    flash("You have been logged out.", "success")
    return redirect(request.args.get('next') or url_for("homepage"))
