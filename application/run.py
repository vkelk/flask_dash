from application import create_app
from config import Configuration

app = create_app(config=Configuration)
app.run()
