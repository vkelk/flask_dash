from datetime import datetime
from flask_wtf import FlaskForm
from flask_wtf.file import FileField, FileRequired, FileAllowed
from wtforms import StringField, DateField, SubmitField, SelectField, HiddenField, IntegerField, RadioField
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
    project_id = HiddenField('Project Id', validators=[DataRequired()])
    dataset = SelectField("Dataset", validators=[DataRequired()],
                          choices=[('fintweet', 'Fintweet')])
    pre_event = IntegerField('Pre event days', default=-1)
    post_event = IntegerField('Post event days', default=2)
    event_date = DateField('Event date')

    code_type_radio = RadioField('Label', validators=[DataRequired()],
                                 choices=[('permno', 'PermNo'), ('ticker', 'Ticker'), ('hashtag', 'Hashtag'),
                                          ('cashtag', 'Cashtag'), ('mentions', 'Mentions'),
                                          ('user_names', 'User Names')])
    company_codes = StringField('Company codes')
    codes_file = FileField('Upload file')
    cashtags_options = SelectField('Select cashtag(s)', choices=[("", "---")])
    codes_list = HiddenField()

    event_window = IntegerField('Event window')

    # buttons
    btn_get_cashtags = SubmitField("Get cashtags")
    btn_get_event_data = SubmitField("Get event data")
    btn_download_csv = SubmitField("Download to CSV")
