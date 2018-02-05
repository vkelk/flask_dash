from datetime import datetime
from flask_wtf import FlaskForm
from wtforms import StringField, PasswordField, DateField, SubmitField, SelectField
from wtforms.validators import DataRequired, Length, EqualTo, Email
from wtforms.widgets import TextArea


class RegisterForm(FlaskForm):
    email = StringField('Email', validators=[DataRequired(),
                                             Email(),
                                             Length(min=6, max=254)])
    password = PasswordField('Password', validators=[DataRequired(),
                                                     Length(min=6, max=254)])
    confirm = PasswordField('Repeat Password',
                            validators=[DataRequired(),
                                        EqualTo('password')])


class LoginForm(FlaskForm):
    email = StringField('Email', validators=[DataRequired(),
                                             Email(),
                                             Length(min=6, max=40)])
    password = PasswordField('Password', validators=[DataRequired()])


class EmailForm(FlaskForm):
    email = StringField('Email', validators=[DataRequired(), Email(), Length(min=6, max=254)])


class PasswordForm(FlaskForm):
    password = PasswordField('Password', validators=[DataRequired()])


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
