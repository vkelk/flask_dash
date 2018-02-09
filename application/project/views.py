import json
from datetime import datetime, timedelta
from pprint import pprint
from flask import render_template, request, Markup, flash, redirect, url_for, abort, session, jsonify
from flask_login import login_user, current_user, login_required, logout_user
from application import db, login_manager
from application.project import project
from application.project.models import *
from application.fintweet.models import *
from application.project.forms import *


@project.route('/project_add', methods=['GET', 'POST'])
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
            return redirect(url_for('project.project', uuid=new_project.uuid))
        except Exception as e:
            db.session.rollback()
            pprint(e)
            message = Markup("<strong>Error!</strong> Unable to create new project.")
            flash(message, 'danger')
    if len(form.errors) > 0:
        pprint(form.errors)
    return render_template('project/project_add.html', form=form)


@project.route('/project_edit/<uuid>', methods=['GET', 'POST'])
@login_required
def project_edit(uuid):
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
            return redirect(url_for('account.project', uuid=new_project.uuid))
        except Exception as e:
            db.session.rollback()
            pprint(e)
            message = Markup("<strong>Error!</strong> Unable to create new project.")
            flash(message, 'danger')
    else:
        project = Project.query.filter(Project.uuid == uuid).first()
    if len(form.errors) > 0:
        pprint(form.errors)
    return render_template('project/project_details.html', project=project, form=form)


@project.route('/project_activate/<uuid>', methods=['GET'])
@login_required
def project_activate(uuid):
    try:
        projects = Project.query.filter(Project.account_id == current_user.get_id()).all()
        for project in projects:
            if project.uuid == uuid:
                project.active = True
                db.session.add(project)
                db.session.commit()
                session['active_project'] = uuid
                session['active_project_name'] = project.name
            else:
                project.active = False
                db.session.add(project)
                db.session.commit()
        message = Markup(
            "<strong>Project activated!</strong>")
        flash(message, 'success')
        return redirect(url_for('project.project', uuid=uuid))
    except Exception as e:
        db.session.rollback()
        pprint(e)
        message = Markup("<strong>Error!</strong> Unable to activate project.")
        flash(message, 'danger')
    return redirect(url_for('project.list'))


@project.route('/list')
@login_required
def list():
    projects = Project.query.filter(Project.account_id == current_user.get_id()).order_by(Project.uuid).all()
    return render_template('project/projects.html', projects=projects)


@project.route('/event_list')
@login_required
def event_list():
    project_uuid = session['active_project']
    events = Event.query.filter(Event.project_id == project_uuid).all()
    return jsonify(events)


@project.route('/event_new', methods=['GET', 'POST'])
@login_required
def event_new():
    project_uuid = session['active_project']
    project = Project.query.filter(Project.uuid == project_uuid).first()
    datasets = Dataset.query.all()
    form = EventStudyForm(request.form)
    if request.method == 'POST' and form.validate_on_submit():
        events = []
        if form.add_event.data:
            new_event_item = form.events.append_entry()
            return render_template('project/event_new.html', form=form, project=project)

    if len(form.errors) > 0:
        pprint(form.errors)
    return render_template('project/event_new.html', form=form, project=project)


@project.route('/<uuid>')
@login_required
def project(uuid):
    project = Project.query.filter(Project.uuid == uuid).first()
    events = Event.query.filter(Event.project_id == project.uuid).all()
    return render_template('project/project_details.html', project=project, events=events)
