from flask import Blueprint

project = Blueprint('project', __name__)

# import application.project.views
from .views import count_views
from .views import project_views
