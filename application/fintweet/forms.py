from datetime import timedelta, datetime
from flask_wtf import FlaskForm
from wtforms import StringField, SelectField, DateField, RadioField, FileField, HiddenField, IntegerField, SubmitField
from wtforms.validators import DataRequired


class EventStydyForm(FlaskForm):
    # dates
    date_range_start = DateField('Date range:', default=datetime(2013, 1, 1), validators=[DataRequired()])
    date_range_end = DateField('to', default=datetime(2016, 12, 31), validators=[DataRequired()])
    pre_event = IntegerField('Pre event days', default=1)
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
