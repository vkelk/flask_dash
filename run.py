from application import create_app
from application.config import Configuration

if __name__ == '__main__':
    app = create_app(config=Configuration)
    app.run()
