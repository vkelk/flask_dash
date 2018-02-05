from datetime import datetime, timedelta
from pprint import pprint
from flask import render_template, request, Markup, flash, redirect, url_for, abort, session
from flask_login import login_user, current_user, login_required, logout_user
from sqlalchemy.exc import IntegrityError
from itsdangerous import URLSafeTimedSerializer

from application import db, login_manager
from application.account import account
from application.account.models import Account, Project
from .forms import *
from .helpers import *
from ..config import Configuration


@login_manager.user_loader
def load_user(user_id):
    return Account.query.filter(Account.id == int(user_id)).first()


@account.route('/register', methods=['GET', 'POST'])
def register():
    form = RegisterForm(request.form)
    if request.method == 'POST':
        if form.validate_on_submit():
            try:
                new_user = Account(form.email.data, form.password.data)
                new_user.authenticated = True
                send_confirmation_email(new_user.email)
                new_user.email_confirmation_sent_on = datetime.now()
                db.session.add(new_user)
                db.session.commit()
                message = Markup(
                    "<strong>Success!</strong> Thanks for registering. Please check your email to confirm your email address.")
                flash(message, 'success')
                return redirect(url_for('main.home'))
            except IntegrityError:
                db.session.rollback()
                message = Markup(
                    "<strong>Error!</strong> Unable to process registration.")
                flash(message, 'danger')
    return render_template('account/register.html', form=form)


@account.route('/login', methods=['GET', 'POST'])
def login():
    form = LoginForm(request.form)
    if request.method == 'POST':
        if form.validate_on_submit():
            user = Account.query.filter_by(email=form.email.data).first()
            if user is not None and user.is_correct_password(form.password.data):
                if user.is_email_confirmed is not True:
                    user.authenticated = True
                    db.session.add(user)
                    db.session.commit()
                    login_user(user)
                    return redirect(url_for('account.resend_email_confirmation'), )
                if user.is_email_confirmed is True:
                    user.authenticated = True
                    user.last_logged_in = user.current_logged_in
                    user.current_logged_in = datetime.now()
                    db.session.add(user)
                    db.session.commit()
                    login_user(user)
                    message = Markup(
                        "<strong>Welcome back!</strong> You are now successfully logged in.")
                    flash(message, 'success')
                    return redirect(url_for('main.home'))
            else:
                message = Markup(
                    "<strong>Error!</strong> Incorrect login credentials.")
                flash(message, 'danger')
    return render_template('account/login.html', form=form)


@account.route('/user_profile', methods=['GET', 'POST'])
@login_required
def user_profile():
    return render_template('account/user_profile.html')


@account.route('/confirm/<token>')
def confirm_email(token):
    try:
        confirm_serializer = URLSafeTimedSerializer(Configuration.SECRET_KEY)
        email = confirm_serializer.loads(token, salt='email-confirmation-salt', max_age=86400)
        print(type(email), email)
    except:
        message = Markup(
            "The confirmation link is invalid or has expired.")
        flash(message, 'danger')
        return redirect(url_for('account.login'))

    user = Account.query.filter_by(email=email).first()

    if user.email_confirmed:
        message = Markup(
            "Account already confirmed. Please login.")
        flash(message, 'info')
    else:
        user.email_confirmed = True
        user.email_confirmed_on = datetime.now()
        db.session.add(user)
        db.session.commit()
        message = Markup(
            "Thank you for confirming your email address!")
        flash(message, 'success')

    return redirect(url_for('main.home'))


@account.route('/reset', methods=["GET", "POST"])
def reset():
    form = EmailForm()
    if form.validate_on_submit():
        try:
            user = Account.query.filter_by(email=form.email.data).first_or_404()
        except:
            message = Markup(
                "Invalid email address!")
            flash(message, 'danger')
            return render_template('account/password_reset_email.html', form=form)
        if user.email_confirmed:
            send_password_reset_email(user.email)
            message = Markup(
                "Please check your email for a password reset link.")
            flash(message, 'success')
        else:
            message = Markup(
                "Your email address must be confirmed before attempting a password reset.")
            flash(message, 'danger')
        return redirect(url_for('account.login'))

    return render_template('account/password_reset_email.html', form=form)


@account.route('/reset/<token>', methods=["GET", "POST"])
def reset_with_token(token):
    try:
        password_reset_serializer = URLSafeTimedSerializer(Configuration.SECRET_KEY)
        email = password_reset_serializer.loads(token, salt='password-reset-salt', max_age=3600)
    except:
        message = Markup(
            "The password reset link is invalid or has expired.")
        flash(message, 'danger')
        return redirect(url_for('account.login'))

    form = PasswordForm()

    if form.validate_on_submit():
        try:
            user = Account.query.filter_by(email=email).first_or_404()
        except:
            message = Markup(
                "Invalid email address!")
            flash(message, 'danger')
            return redirect(url_for('account.login'))

        user.password = form.password.data
        db.session.add(user)
        db.session.commit()
        message = Markup(
            "Your password has been updated!")
        flash(message, 'success')
        return redirect(url_for('account.login'))

    return render_template('account/reset_password_with_token.html', form=form, token=token)


@account.route('/admin_view_users')
@login_required
def admin_view_users():
    if current_user.role != 'admin':
        abort(403)
    else:
        users = Account.query.order_by(Account.id).all()
        return render_template('account/admin_view_users.html', users=users)


@account.route('/admin_dashboard')
@login_required
def admin_dashboard():
    if current_user.role != 'admin':
        abort(403)
    else:
        users = Account.query.order_by(Account.id).all()
        kpi_mau = Account.query.filter(Account.last_logged_in > (datetime.today() - timedelta(days=30))).count()
        kpi_total_confirmed = Account.query.filter_by(email_confirmed=True).count()
        kpi_mau_percentage = (100 / kpi_total_confirmed) * kpi_mau
        return render_template('account/admin_dashboard.html', users=users, kpi_mau=kpi_mau,
                               kpi_total_confirmed=kpi_total_confirmed, kpi_mau_percentage=kpi_mau_percentage)


@account.route('/logout')
@login_required
def logout():
    user = current_user
    user.authenticated = False
    db.session.add(user)
    db.session.commit()
    logout_user()
    message = Markup("<strong>Goodbye!</strong> You are now logged out.")
    flash(message, 'info')
    return redirect(url_for('account.login'))


@account.route('/password_change', methods=["GET", "POST"])
@login_required
def user_password_change():
    form = PasswordForm()
    if request.method == 'POST':
        if form.validate_on_submit():
            user = current_user
            user.password = form.password.data
            db.session.add(user)
            db.session.commit()
            message = Markup(
                "Password has been updated!")
            flash(message, 'success')
            return redirect(url_for('account.user_profile'))

    return render_template('account/password_change.html', form=form)


@account.route('/resend_confirmation')
@login_required
def resend_email_confirmation():
    try:
        send_confirmation_email(current_user.email)
        message = Markup(
            "Email sent to confirm your email address. Please check your inbox!")
        flash(message, 'success')
        user = current_user
        user.authenticated = False
        db.session.add(user)
        db.session.commit()
        logout_user()
    except IntegrityError:
        message = Markup(
            "Error!  Unable to send email to confirm your email address.")
        flash(message, 'danger')
        user = current_user
        user.authenticated = False
        db.session.add(user)
        db.session.commit()
        logout_user()
    return redirect(url_for('account.login'))


@account.route('/email_change', methods=["GET", "POST"])
@login_required
def user_email_change():
    form = EmailForm()
    if request.method == 'POST':
        if form.validate_on_submit():
            try:
                user_check = Account.query.filter_by(email=form.email.data).first()
                if user_check is None:
                    user = current_user
                    user.email = form.email.data
                    user.email_confirmed = False
                    user.email_confirmed_on = None
                    user.email_confirmation_sent_on = datetime.now()
                    db.session.add(user)
                    db.session.commit()
                    send_confirmation_email(user.email)
                    message = Markup(
                        "Email changed!  Please confirm your new email address (link sent to new email)")
                    flash(message, 'success')
                    return redirect(url_for('account.user_profile'))
                else:
                    message = Markup(
                        "Sorry, that email already exists!")
                    flash(message, 'danger')
            except IntegrityError:
                message = Markup(
                    "Sorry, that email already exists!")
                flash(message, 'danger')
    return render_template('account/email_change.html', form=form)


@account.route('/project_add', methods=['GET', 'POST'])
@login_required
def project_add():
    form = NewProjectForm(request.form)
    if request.method == 'POST' and form.validate_on_submit():
        try:
            new_project = Project(form.name.data, form.description.data, form.date_start.data, form.date_end.data)
            new_project.account_id = current_user.get_id()
            db.session.add(new_project)
            db.session.commit()
            message = Markup(
                "<strong>Project created!</strong> Please continue...")
            flash(message, 'success')
            next_url = 'account.project/' + str(new_project.id)
            return redirect(url_for('account.project', id=new_project.id))
        except Exception as e:
            db.session.rollback()
            pprint(e)
            message = Markup("<strong>Error!</strong> Unable to create new project.")
            flash(message, 'danger')
    if len(form.errors) > 0:
        pprint(form.errors)
    return render_template('account/project_add.html', form=form)


@account.route('/project_edit/<int:id>', methods=['GET', 'POST'])
@login_required
def project_edit(id):
    form = NewProjectForm(request.form)
    if request.method == 'POST' and form.validate_on_submit():
        try:
            new_project = Project(form.name.data, form.description.data, form.date_start.data, form.date_end.data)
            new_project.account_id = current_user.get_id()
            db.session.add(new_project)
            db.session.commit()
            message = Markup(
                "<strong>Project created!</strong> Please continue...")
            flash(message, 'success')
            next_url = 'account.project/' + str(new_project.id)
            return redirect(url_for('account.project', id=new_project.id))
        except Exception as e:
            db.session.rollback()
            pprint(e)
            message = Markup("<strong>Error!</strong> Unable to create new project.")
            flash(message, 'danger')
    if len(form.errors) > 0:
        pprint(form.errors)
    return render_template('account/project_details.html', form=form)


@account.route('/project_activate/<int:id>', methods=['GET'])
@login_required
def project_activate(id):
    try:
        projects = Project.query.filter(Project.account_id == current_user.get_id()).all()
        for project in projects:
            if project.id == id:
                project.active = True
                db.session.add(project)
                db.session.commit()
                session['active_project'] = id
                session['active_project_name'] = project.name
            else:
                project.active = False
                db.session.add(project)
                db.session.commit()
        message = Markup(
            "<strong>Project activated!</strong>")
        flash(message, 'success')
        return redirect(url_for('account.project', id=id))
    except Exception as e:
        db.session.rollback()
        pprint(e)
        message = Markup("<strong>Error!</strong> Unable to activate project.")
        flash(message, 'danger')
    return redirect(url_for('account.projects'))


@account.route('/projects')
@login_required
def projects():
    projects = Project.query.filter(Project.account_id == current_user.get_id()).order_by(Project.id).all()
    return render_template('account/projects.html', projects=projects)


@account.route('/project/<int:id>')
@login_required
def project(id):
    project = Project.query.filter(Project.id == id).first()
    return render_template('account/project_details.html', project=project)
