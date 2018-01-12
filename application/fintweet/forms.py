from flask_wtf import FlaskForm
from wtforms import StringField, SelectField, DateField, RadioField, FileField, FormField


class Form2(FlaskForm):
    company_codes = StringField()
    codes_file = FileField()


class Form1(FlaskForm):
    start_date = DateField('Date range:')
    end_date = DateField('to')

    code_type_radio = RadioField('Label',
                                 choices=[('permno', 'PermNo'), ('ticker', 'Ticker'), ('hashtag', 'Hashtag'),
                                          ('cashtag', 'Cashtag'), ('mentions', 'Mentions'),
                                          ('user_names', 'User Names')])
    codes_container_radio = FormField(Form2)

    permno = SelectField()
    cashtag = SelectField()
