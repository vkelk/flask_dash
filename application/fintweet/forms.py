from datetime import timedelta
from flask_wtf import FlaskForm
from wtforms import StringField, SelectField, DateField, RadioField, FileField, HiddenField, IntegerField, SubmitField
from wtforms.validators import DataRequired


class Form1(FlaskForm):
    start_date = DateField('Date range:')
    end_date = DateField('to')

    code_type_radio = RadioField('Label', validators=[DataRequired()],
                                 choices=[('permno', 'PermNo'), ('ticker', 'Ticker'), ('hashtag', 'Hashtag'),
                                          ('cashtag', 'Cashtag'), ('mentions', 'Mentions'),
                                          ('user_names', 'User Names')])
    company_codes = StringField('Company codes')
    codes_file = FileField('Upload file')
    cashtags_options = SelectField('Cashtags', choices=[("", "---")])
    codes_list = HiddenField()

    event_date = DateField('Event date')
    pre_event = IntegerField('Pre event days', default=1)
    post_event = IntegerField('Post event days', default=2)
    event_window = IntegerField('Event window')
    get_cashtags = SubmitField("Get cashtags")
    get_event_data = SubmitField("Get event date data")
