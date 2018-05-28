from flask import Blueprint

project = Blueprint('project', __name__, template_folder='templates')

# import application.project.views
from .views import count_views
from .views import project_views
