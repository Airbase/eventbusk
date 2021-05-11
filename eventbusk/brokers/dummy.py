"""
Dummy broker for use cases where a real implementation of the event bus is not
required, eg. CI pipelines.
"""
from __future__ import annotations

import time
import logging
from contextlib import ContextDecorator
from dataclasses import dataclass
from typing import Optional, Callable

from .base import BaseConsumer, BaseProducer, DeliveryCallBackT, BaseBroker



logger = logging.getLogger(__name__)


__all__ = [
    "DummyConsumer",
    "DummyProducer",
]


@dataclass
class DummyBroker(BaseBroker):
    """
    Broker URI

    Basic url is of the format: dummy://

    Usage
    ------
    >>> broker = Broker("dummy://localhost:9092")
    """

    @classmethod
    def from_uri(cls, uri: str) -> DummyBroker:
        invalid_format = ValueError("Broker URI should be of the format 'dummy://'")

        if not uri.startswith("dummy://"):
            raise invalid_format

        return cls()


class DummyConsumer(BaseConsumer):
    """
    Dummy event consumer which simply loses all events!

    Example
    -------
    >>> with DummyConsumer(broker, topic, group) as consumer:
           ...
    """

    def __init__(self, broker: str, topic: str, group: str) -> None:
        super().__init__()
        self.broker = DummyBroker.from_uri(broker)
        self.topic = topic
        self.group = group

    def poll(self, timeout: int=1) -> Optional[str]:
        """
        Sleeps for the required timeout, and returns no message.
        """
        time.sleep(timeout)
        return None

    def ack(self, message: str) -> None:
        """
        Acknowledge event
        """


class DummyProducer(BaseProducer):
    """
    Dummy event producer.
    """
    def __init__(self, broker: str):
        super().__init__(broker)
        self.broker = DummyBroker.from_uri(broker)

    def produce(self, topic: str, value: str, flush: bool=True, on_delivery: DeliveryCallBackT=None, fail_silently: bool=False) -> None:
        """
        Only logs the message, does not deliver.
        """
        logger.info("Producing message.", extra={
            "topic": topic,
            "message": value,
            "flush": True,
        })
        # TODO: call
        # on_delivery
