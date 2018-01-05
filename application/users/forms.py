from flask import Blueprint
import wtforms
from wtforms import validators

users = Blueprint('users', __name__, template_folder='templates')


class LoginForm(wtforms.Form):
    email = wtforms.StringField("Email", validators=[validators.DataRequired(), validators.Email()])
    password = wtforms.PasswordField("Password", validators=[validators.DataRequired()])
    remember_me = wtforms.BooleanField("Remember me?", default=True)

    def validate(self):
        if not super(LoginForm, self).validate():
            return False
        self.user = users.authenticate(self.email.data, self.password.data)
        if not self.user:
            self.email.errors.append("Invalid email or password")
            return False
        return True
