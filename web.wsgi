#!C:\Python36\python.exe
import sys
 
sys.path.append('d:/webapps/flask_dash/')
 
from application import create_app
from application.config import base_config

application = create_app(config=base_config)
# app.run()
