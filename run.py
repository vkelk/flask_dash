from application import create_app, config


if __name__ == '__main__':
    app = create_app(config=config.dev_config)
    app.run()
