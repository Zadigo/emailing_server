import logging
import pathlib

import dotenv

PROJECT_PATH = pathlib.Path(__file__).parent

dotenv.load_dotenv(PROJECT_PATH / '.env')


class Logger:
    def __init__(self):
        instance = logging.Logger('Emailing')
        instance.setLevel(logging.DEBUG)
        handler = logging.StreamHandler()
        instance.addHandler(handler)
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s: %(message)s',
            datefmt='%Y-%m-%d %H:%M'
        )
        handler.setFormatter(formatter)
        self.instance = instance

    def debug(self, message, *args, **kwargs):
        return self.instance.debug(message, *args, **kwargs)


logger = Logger()
