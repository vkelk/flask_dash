from flask import Blueprint

project = Blueprint('project', __name__)

import application.project.views
