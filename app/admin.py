from flask import g, redirect, request, url_for
from flask_admin import Admin, AdminIndexView, expose
from flask_admin.contrib.sqla import ModelView
from wtforms.fields import PasswordField

from app import app, db
from models import DashUser


class AdminAuthentication(object):
    def is_accessible(self):
        return g.user.is_authenticated and g.user.is_admin()


class BaseModelView(AdminAuthentication, ModelView):
    pass


class DashUserModelView(BaseModelView):
    _status_choices = [(choice, label) for choice, label in [
        (DashUser.STATUS_ACTIVE, 'Active'),
        (DashUser.STATUS_DISABLED, 'Disabled')
    ]]
    column_choices = {
        'active': _status_choices
    }
    column_filters = ('email', 'name', 'active')
    column_list = ['email', 'name', 'active', 'created_timestamp']
    column_searchable_list = ['email', 'name']

    form_columns = ['email', 'password', 'name', 'active']
    form_extra_fields = {
        'password': PasswordField('New password')
    }

    def on_model_change(self, form, model, is_created):
        if form.password.data:
            model.password_hash = DashUser.make_password(form.password.data)
        return super(DashUserModelView, self).on_model_change(form, model, is_created)


class IndexView(AdminIndexView):
    @expose('/')
    def index(self):
        if not (g.user.is_authenticated and g.user.is_admin()):
            return redirect(url_for('login', next=request.path))
        return self.render('admin/index.html')


admin = Admin(app, 'Dashboard Admin', index_view=IndexView())
admin.add_view(DashUserModelView(DashUser, db.session))
