from datetime import timedelta, datetime
from flask_wtf import FlaskForm
from flask_wtf.file import FileField, FileRequired, FileAllowed
from wtforms import StringField, SelectField, DateField, RadioField, HiddenField, IntegerField, SubmitField
from wtforms.validators import DataRequired, NumberRange
from application.config import Configuration


class EventStydyForm(FlaskForm):
    # dates
    date_range_start = DateField('Date range:', default=datetime(2013, 1, 1), validators=[DataRequired()])
    date_range_end = DateField('to', default=datetime(2016, 12, 31), validators=[DataRequired()])
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


class EventStudyFileForm(FlaskForm):
    date_range_start = DateField('From:', default=datetime(2013, 1, 1), validators=[DataRequired()])
    date_range_end = DateField('to', default=datetime(2016, 12, 31), validators=[DataRequired()])
    days_pre_event = IntegerField('Pre event days', default=-1, validators=[DataRequired()])
    days_post_event = IntegerField('Post event days', default=2,
                                   validators=[DataRequired(), NumberRange(min=0, message='Must be non-negative')])
    days_estimation = IntegerField('Estimation days', default=120,
                                   validators=[DataRequired(), NumberRange(min=0, message='Must be non-negative')])
    days_grace_period = IntegerField('Grace period days', default=0,
                                     validators=[NumberRange(min=0, message='Must be non-negative')])

    select_deal_resolution = RadioField('Include deal resolution', default='false',
                                        choices=[('false', 'No'), ('true', 'Yes')],
                                        validators=[DataRequired()])

    file_input = FileField(
        validators=[FileRequired(), FileAllowed(Configuration.ALLOWED_EXTENSIONS, 'Text data files only!')])
    file_name = HiddenField()
    file_csv = HiddenField()

    btn_file_upload = SubmitField("Upload file")
    btn_calculate = SubmitField("Calculate data")
    btn_download_csv = SubmitField("Download as Excel")
