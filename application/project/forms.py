from datetime import datetime
from flask_wtf import FlaskForm
from flask_wtf.file import FileField, FileRequired, FileAllowed
from wtforms import StringField, DateField, SubmitField, SelectField, HiddenField, IntegerField, RadioField, FieldList, \
    FormField
from wtforms.validators import DataRequired, Length, EqualTo, Email
from wtforms.widgets import TextArea


class NewProjectForm(FlaskForm):
    name = StringField("Project name", validators=[DataRequired()])
    description = StringField("Project description", widget=TextArea())
    date_start = DateField('Date start:', default=datetime(2013, 1, 1), validators=[DataRequired()])
    date_end = DateField('Date end', default=datetime(2016, 12, 31), validators=[DataRequired()])
    btn_enable = SubmitField("Enable project")
    btn_disable = SubmitField("Disable project")


class ProjectDetails(FlaskForm):
    type = SelectField('Label', validators=[DataRequired()],
                       choices=[('permno', 'PermNo'), ('tickers', 'Ticker'), ('hashtags', 'Hashtag'),
                                ('cashtags', 'Cashtag'), ('mentions', 'Mentions'),
                                ('user_names', 'User Names')])
    text = StringField('Search codes', validators=[DataRequired()])
    btn_enable = SubmitField("Enable Criteria")
    btn_disable = SubmitField("Disable Criteria")


class EventForm(FlaskForm):
    pre_event = IntegerField('Pre event days', default=-1)
    post_event = IntegerField('Post event days', default=2)
    event_date = DateField('Event date')
    code_type_radio = SelectField('Select code type', validators=[DataRequired()],
                                  choices=[('permno', 'PermNo'), ('ticker', 'Ticker'), ('hashtag', 'Hashtag'),
                                          ('cashtag', 'Cashtag'), ('mentions', 'Mentions'),
                                          ('user_names', 'User Names')])
    code_text = StringField('Tag text')


class EventStudyForm(FlaskForm):
    project_id = HiddenField('Project Id', validators=[DataRequired()])
    dataset = SelectField("Dataset", validators=[DataRequired()],
                          choices=[('fintweet', 'Fintweet')])

    events = FieldList(FormField(EventForm), min_entries=1)
