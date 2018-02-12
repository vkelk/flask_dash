import hashlib, json, os
from datetime import datetime, timedelta
import pandas as pd
from pprint import pprint
from flask import render_template, request, Markup, flash, redirect, url_for, abort, session, jsonify, send_file
from flask_login import login_user, current_user, login_required, logout_user
from werkzeug.utils import secure_filename, CombinedMultiDict
from application import db, login_manager
from application.project import project
from application.project.models import *
from application.fintweet.models import *
from application.project.forms import *
from application.project.helpers import *


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
    # projects = Project.query.filter(Project.account_id == current_user.get_id()).order_by(Project.uuid).all()
    projects = db.session.query(Project.uuid, Project.name, Project.description, Project.date_start, Project.date_end,
                                Project.active, func.count(Event.project_id).label('events_count')).select_from(Project) \
        .outerjoin(Event, Event.project_id == Project.uuid).filter(Project.account_id == current_user.get_id()) \
        .group_by(Project.uuid).group_by(Event.project_id).order_by(Project.uuid).all()
    return render_template('project/projects.html', projects=projects)


@project.route('/event_list')
@login_required
def event_list():
    project = Project.query.filter(Project.account_id == current_user.get_id()).filter(Project.active == True).first()
    if project:
        session['active_project'] = project.uuid
    events = Event.query.filter(Event.project_id == project.uuid).all()
    return render_template('project/events.html', events=events, project=project)


@project.route('/event_tweets/<uuid>')
@login_required
def event_tweets(uuid):
    event = Event.query.filter(Event.uuid == uuid).first()
    return render_template('project/event_tweets.html', event=event)


@project.route('/event_new', methods=['GET', 'POST'])
@login_required
def event_new():
    project = Project.query.filter(Project.account_id == current_user.get_id()).filter(Project.active == True).first()
    if project:
        session['active_project'] = project.uuid
    datasets = Dataset.query.all()
    form = EventStudyForm(request.form)
    if request.method == 'POST' and form.validate_on_submit():
        if form.add_event.data:
            form.events.append_entry()
            return render_template('project/event_new.html', form=form, project=project)
        if form.create_study.data:
            pprint(form.events.data)
            return render_template('project/event_new.html', form=form, project=project)

    if len(form.errors) > 0:
        pprint(form.errors)
    return render_template('project/event_new.html', form=form, project=project)


@project.route('/events_upload', methods=['GET', 'POST'])
@login_required
def events_upload():
    project = Project.query.filter(Project.account_id == current_user.get_id()).filter(Project.active == True).first()
    if project:
        session['active_project'] = project.uuid
    datasets = Dataset.query.all()
    form = EventStudyFileForm(CombinedMultiDict((request.files, request.form)))
    if request.method == 'POST' and form.validate_on_submit():
        if form.create_study.data:
            file_input = secure_filename(str(uuid.uuid4()) + os.path.splitext(form.file_input.data.filename)[-1])
            form.file_input.data.save(os.path.join(Configuration.UPLOAD_FOLDER, file_input))
            form.file_name.data = file_input
            df_in = dataframe_from_file(os.path.join(Configuration.UPLOAD_FOLDER, form.file_name.data))
            if df_in.empty:
                return None
            for index, row in df_in.iterrows():
                date_on_event = row['event_date'].to_pydatetime()
                days_pre_event = abs(form.days_pre_event.data)
                event_window = {'date': date_on_event,
                                'start': date_on_event - timedelta(days=days_pre_event),
                                'end': date_on_event + timedelta(days=form.days_post_event.data)}
                date_estwin_pre_end = event_window['start'] - timedelta(days=(form.days_grace.data + 1))
                date_estwin_pre_start = date_estwin_pre_end - timedelta(days=form.days_estimation.data)
                date_estwin_pre_start = min(date_estwin_pre_start.date(), project.date_start)
                if form.select_deal_resolution.data == 'false':
                    date_estwin_post_start = event_window['end'] + timedelta(days=1)
                    date_estwin_post_end = date_estwin_post_start + timedelta(days=form.days_estimation.data)
                else:
                    date_estwin_post_start = event_window['end'] + timedelta(days=1)
                    date_estwin_post_end = date_estwin_post_start + timedelta(days=row['deal_resolution'])
                    date_estwin_post_end = max(date_estwin_post_end.date(), project.date_end)

                estimation_window = {'pre_end': date_estwin_pre_end,
                                     'pre_start': date_estwin_pre_start,
                                     'post_start': date_estwin_post_start,
                                     'post_end': date_estwin_post_end}
                result_list = get_data_from_query(row['cashtag'], estimation_window)
                if len(result_list) > 0:
                    check_string = project.uuid + 'cashtag' + row['cashtag'] + str(event_window['date'])
                    event_uuid = hashlib.md5(check_string.encode('utf-8')).hexdigest()
                    event = Event.query.filter(Event.uuid == event_uuid).first()
                    if not event:
                        event = Event(project.uuid, form.dataset.data)
                        event.type = 'cashtag'
                        event.text = row['cashtag']
                        event.event_date = event_window['date']
                        event.uuid = hashlib.md5(check_string.encode('utf-8')).hexdigest()
                        event.event_start = event_window['start']
                        event.event_end = event_window['end']
                        event.event_pre_start = estimation_window['pre_start']
                        event.event_pre_end = estimation_window['pre_end']
                        event.event_post_start = estimation_window['post_start']
                        event.event_post_end = estimation_window['post_end']
                        event.days_pre = form.days_pre_event.data
                        event.days_post = form.days_post_event.data
                        event.days_estimation = form.days_estimation.data
                        event.days_grace = form.days_grace.data
                        db.session.add(event)
                        db.session.commit()

                    df_full = pd.DataFrame.from_records(result_list, index='date')
                    df_full.fillna(0)
                    df_pre_est = df_full.loc[: estimation_window['pre_end'].date()]
                    df_pre_est.fillna(0)
                    df_event = df_full.loc[event_window['start'].date():event_window['end'].date()]
                    df_event.fillna(0)
                    df_post_est = df_full.loc[event_window['end'].date():]
                    df_post_est.fillna(0)

                    event_stats = EventStats.query.filter(EventStats.uuid == event_uuid).first()
                    if not event_stats:
                        event_stats = EventStats(event.uuid)

                    event_stats.event_total = int(df_event['count'].sum())
                    event_stats.event_median = df_event['count'].median() if event_stats.event_total > 0 else 0
                    event_stats.event_mean = df_event['count'].mean() if event_stats.event_total > 0 else 0
                    event_stats.event_std = df_event['count'].std() if event_stats.event_total > 1 else 0
                    event_stats.pre_total = int(df_pre_est['count'].sum())
                    event_stats.pre_median = df_pre_est['count'].median() if event_stats.pre_total > 0 else 0
                    event_stats.pre_mean = df_pre_est['count'].mean() if event_stats.pre_total > 0 else 0
                    event_stats.pre_std = df_pre_est['count'].std() if event_stats.pre_total > 1 else 0
                    event_stats.post_total = int(df_post_est['count'].sum())
                    event_stats.post_median = df_post_est['count'].median() if event_stats.post_total > 0 else 0
                    event_stats.post_mean = df_post_est['count'].mean() if event_stats.post_total > 0 else 0
                    event_stats.post_std = df_post_est['count'].std() if event_stats.post_total > 1 else 0
                    event_stats.pct_change = (
                                                         event_stats.post_total - event_stats.pre_total) / event_stats.pre_total if event_stats.pre_total > 0 else 0

                    db.session.add(event_stats)
                    db.session.commit()

                    df_in.loc[index, "total pre event"] = event_stats.pre_total
                    df_in.loc[index, "median pre event"] = event_stats.pre_median
                    df_in.loc[index, "mean pre event"] = event_stats.pre_mean
                    df_in.loc[index, "std pre event"] = event_stats.pre_std
                    df_in.loc[index, "total during event"] = event_stats.event_total
                    df_in.loc[index, "median during event"] = event_stats.event_median
                    df_in.loc[index, "mean during event"] = event_stats.event_mean
                    df_in.loc[index, "std during event"] = event_stats.event_std
                    df_in.loc[index, "total post event"] = event_stats.post_total
                    df_in.loc[index, "median post event"] = event_stats.post_median
                    df_in.loc[index, "mean post event"] = event_stats.post_mean
                    df_in.loc[index, "std post event"] = event_stats.post_std
                    df_in.loc[index, "pct change"] = event_stats.pct_change

                    insert_event_tweets(event)

            file_output = 'output_' + file_input
            project.file_output = file_output
            df_in.to_excel(os.path.join(Configuration.UPLOAD_FOLDER, file_output), index=False)
            # df_in.to_stata(os.path.join(Configuration.UPLOAD_FOLDER, 'output.dta'), index=False)
            # form.output_file.data = 'upload/output' + file_input
            form.output_file.data = file_output
            db.session.add(project)
            db.session.commit()
            return render_template('project/events_upload.html', form=form, project=project,
                                   df_in=df_in.to_html(classes='table table-striped'))

    if len(form.errors) > 0:
        pprint(form.errors)
    return render_template('project/events_upload.html', form=form, project=project)


@project.route('/ajax_event_tweets/<uuid>/<period>')
def ajax_event_tweets(uuid, period):
    q = db.session.query(func.to_char(EventTweets.tweet_id, 'FM999999999999999999').label('tweet_id'), Tweet.text) \
        .select_from(EventTweets).join(Tweet, Tweet.tweet_id == EventTweets.tweet_id) \
        .filter(EventTweets.event_uuid == uuid).filter(EventTweets.event_period == period) \
        .order_by(Tweet.tweet_id).all()
    pprint(q)
    return jsonify(q)


@project.route('/getfile/<filename>')  # this is a job for GET, not POST
def getfile(filename):
    if not filename:
        return None
    return send_file('uploads/' + filename,
                     mimetype='text/csv',
                     attachment_filename=filename,
                     as_attachment=True)


@project.route('/<uuid>')
@login_required
def project(uuid):
    project = Project.query.filter(Project.uuid == uuid).first()
    events = Event.query.filter(Event.project_id == project.uuid).all()
    return render_template('project/project_details.html', project=project, events=events)
