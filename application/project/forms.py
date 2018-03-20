from datetime import datetime
from flask_wtf import FlaskForm
from flask_wtf.file import FileField, FileRequired, FileAllowed
from wtforms import StringField, DateField, SubmitField, SelectField, HiddenField, IntegerField, RadioField, FieldList, \
    FormField
from wtforms.validators import DataRequired, NumberRange
from wtforms.widgets import TextArea
from application.config import Configuration


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
    pre_event = IntegerField('Pre event days', default=-1, validators=[DataRequired()])
    post_event = IntegerField('Post event days', default=2, validators=[DataRequired()])
    event_date = DateField('Event date', validators=[DataRequired()])
    days_estimation = IntegerField('Estimation window', default=120, validators=[DataRequired()])
    days_grace = IntegerField('Grace period', default=0)
    code_type_radio = SelectField('Select code type', validators=[DataRequired()],
                                  choices=[('permno', 'PermNo'), ('ticker', 'Ticker'), ('hashtag', 'Hashtag'),
                                          ('cashtag', 'Cashtag'), ('mentions', 'Mentions'),
                                          ('user_names', 'User Names')])
    code_text = StringField('Tag text', validators=[DataRequired()])


class EventStudyForm(FlaskForm):
    # project_id = HiddenField('Project Id', validators=[DataRequired()])
    dataset = SelectField(
        "Select dataset",
        validators=[DataRequired()],
        choices=[('fintweet', 'Fintweet'), ('stocktwits', 'Stocktwits')])

    events = FieldList(FormField(EventForm), min_entries=1)
    add_event = SubmitField('Add new event')
    create_study = SubmitField('Process study')


class EventStudyFileForm(FlaskForm):
    dataset = SelectField(
        "Select dataset",
        validators=[DataRequired()],
        choices=[('fintweet', 'Fintweet'), ('stocktwits', 'Stocktwits')])
    days_pre_event = IntegerField('Pre event days', default=-1, validators=[DataRequired()])
    days_post_event = IntegerField('Post event days', default=2,
                                   validators=[DataRequired(), NumberRange(min=0, message='Must be non-negative')])
    days_estimation = IntegerField('Estimation days', default=120,
                                   validators=[DataRequired(), NumberRange(min=0, message='Must be non-negative')])
    days_grace = IntegerField('Grace period days', default=0,
                              validators=[NumberRange(min=0, message='Must be non-negative')])

    select_deal_resolution = RadioField('Include deal resolution', default='false',
                                        choices=[('false', 'No'), ('true', 'Yes')],
                                        validators=[DataRequired()])

    file_input = FileField(
        validators=[FileRequired(), FileAllowed(Configuration.ALLOWED_EXTENSIONS, 'Text data files only!')])
    file_name = HiddenField()
    output_file = HiddenField()

    create_study = SubmitField('Process study')
