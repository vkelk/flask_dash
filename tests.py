from application import create_app
from application.config import Configuration
import unittest


class AppTestCase(unittest.TestCase):
    def setUp(self):
        app = create_app(Configuration)
        app.config['TESTING'] = True
        self.app = app

    def tearDown(self):
        pass

    def test_app_configuration(self):
        self.assertTrue(self.app.config['TESTING'])


if __name__ == "__main__":
    unittest.main()
