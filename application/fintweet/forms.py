from datetime import timedelta
from flask_wtf import FlaskForm
from wtforms import StringField, SelectField, DateField, RadioField, FileField, FormField, IntegerField


class Form1(FlaskForm):
    start_date = DateField('Date range:')
    end_date = DateField('to')

    code_type_radio = RadioField('Label',
                                 choices=[('permno', 'PermNo'), ('ticker', 'Ticker'), ('hashtag', 'Hashtag'),
                                          ('cashtag', 'Cashtag'), ('mentions', 'Mentions'),
                                          ('user_names', 'User Names')])
    company_codes = StringField('Company codes')
    codes_file = FileField('Upload file')

    event_date = DateField('Event date')
    event_window = IntegerField('Event window')
