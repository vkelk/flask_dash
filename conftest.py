import pytest
from application import create_app
from application.config import Configuration


@pytest.fixture
def app():
    app = create_app(config=Configuration)
    return app
    