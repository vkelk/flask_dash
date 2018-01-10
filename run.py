from application import create_app
from application.config import Configuration

app = create_app(config=Configuration)
app.run()
