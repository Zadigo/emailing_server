import redis
from emailing_server import logger


def redis_connection():
    instance = redis.Redis('localhost', 5679, '')
    try:
        instance.ping()
    except:
        logger.debug('Connection to Redis failed')
        return False
    else:
        return instance
