import asyncio
import dataclasses
import datetime
import json
import logging
from asyncio.tasks import sleep
from dataclasses import dataclass, field

import pytz
import redis

# POST http://api.example.com -> flask (email) -> redis
# WHILE server <-> redis -> IF campaigns -> Send email


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


class ModelMixin:
    def transform_date(self, d):
        return datetime.datetime.strptime(d, '%Y-%m-%d %H:%M:%S.%f')


@dataclass
class Step:
    id: int
    value: int
    send_date: str
    days: int

    def __hash__(self):
        return hash((self.id, self.send_date))


@dataclass
class Email(ModelMixin):
    id: int
    email: str
    current_step: int
    steps: list = field(default_factory=list)

    def __hash__(self):
        return hash((self.id, self.email))

    @property
    def get_steps(self):
        instances = []
        for step in self.steps:
            instances.append(Step(**step))
        return instances


@dataclass
class Campaign(ModelMixin):
    id: int
    reference: str
    name: str
    number_of_steps: int
    minutes: int
    active: bool = False
    emails: list = field(default_factory=list)

    def __hash__(self):
        return hash((self.id, self.reference, self.name))

    @property
    def get_emails(self):
        instances = []
        for email in self.emails:
            instances.append(Email(**email))
        return instances

    @property
    def get_date(self):
        return self.transform_date(self.start_date)


def get_date():
    return datetime.datetime.now(tz=pytz.UTC)


async def main():
    logger.debug('Starting server')
    start_date = get_date()

    campaign_queue = asyncio.Queue()
    emails_queue = asyncio.Queue()

    send_emails_every = 2
    next_sending_date = None

    async def read_database():
        with open('redis.json', encoding='utf-8') as f:
            data = json.load(f)
            for campaign in data:
                instance = Campaign(**campaign)
                if instance.active:
                    await campaign_queue.put(instance)
                await asyncio.sleep(1)

    async def get_emails():
        while not campaign_queue.empty():
            campaign = await campaign_queue.get()
            for email in campaign.get_emails:
                await emails_queue.put((campaign.minutes, email))
            logger.debug('Getting emails')
            await asyncio.sleep(1)

    async def send_emails():
        # TODO: Emails should be sent every x minutes
        next_date = None
        while not emails_queue.empty():
            current_date = get_date()
            wait_time, email = await emails_queue.get()
            for step in email.get_steps:
                if not step.value == email.current_step:
                    continue

                if next_date is None:
                    next_date = current_date + \
                        datetime.timedelta(minutes=wait_time)

                if current_date >= next_date:
                    next_date = None
                    logger.debug(f'Sending email: {email}')
            await asyncio.sleep(10)

    next_date = None
    while True:
        current_date = get_date()
        # if next_date is not None:
        #     logger.debug(current_date > next_date)
        if next_date is None:
            next_date = get_date() + datetime.timedelta(minutes=1)

        if current_date > next_date:
            next_date = get_date() + datetime.timedelta(minutes=1)

        # Check if there are campaigns
        # in the Redis database and
        # include them in the Queue
        await read_database()
        # When the "campaign_queue" has campaigns,
        # get all the emails and send if necessary
        await asyncio.gather(get_emails(), send_emails())

        logger.debug('Sleeping 5 seconds')
        await asyncio.sleep(5)


if __name__ == '__main__':
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.debug('Server stopped')
