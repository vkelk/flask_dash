from flask_wtf import FlaskForm
from wtforms import StringField, SelectField, DateField, RadioField


class Form1(FlaskForm):
    start_date = DateField('Date range:')
    end_date = DateField('to')

    code_type_radio = RadioField('Label',
                                 choices=[('permno', 'PermNo'), ('ticker', 'Ticker'), ('hashtag', 'Hashtag'),
                                          ('cashtag', 'Cashtag'), ('mentions', 'Mentions'),
                                          ('user_names', 'User Names')])

    permno = SelectField()
    cashtag = SelectField()
