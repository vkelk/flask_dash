from flask_wtf import FlaskForm
from wtforms import StringField, SelectField


class PermnoCashtagForm(FlaskForm):
    permno = SelectField()
    cashtag = SelectField()
