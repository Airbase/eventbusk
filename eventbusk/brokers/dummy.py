"""
Dummy broker for use cases where a real implementation of the event bus is not
required, eg. CI pipelines.
"""
from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from typing import TYPE_CHECKING, Optional

from .base import BaseBrokerURI, BaseConsumer, BaseProducer

if TYPE_CHECKING:
    from .base import DeliveryCallBackT, MessageT

logger = logging.getLogger(__name__)


__all__ = [
    "BrokerURI",
    "Consumer",
    "Producer",
]


@dataclass
class BrokerURI(BaseBrokerURI):
    """
    Broker URI

    Basic url is of the format: dummy://

    Usage
    ------
    >>> broker = BrokerURI("dummy://localhost:9092")
    """

    @classmethod
    def from_uri(cls, uri: str) -> BrokerURI:
        """
        Instantiate from a URI like "dummy://"
        """
        invalid_format = ValueError("Broker URI should be of the format 'dummy://'")

        if not uri.startswith("dummy://"):
            raise invalid_format

        return cls()


class Consumer(BaseConsumer):
    """
    Dummy event consumer which simply loses all events!

    Example
    -------
    >>> with DummyConsumer(broker, topic, group) as consumer:
           ...
    """

    broker: BrokerURI

    def __init__(self, broker: str, topic: str, group: str) -> None:
        super().__init__()
        self.broker = BrokerURI.from_uri(broker)
        self.topic = topic
        self.group = group

    def poll(self, timeout: int = 1) -> Optional[MessageT]:
        """
        Sleeps for the required timeout, and returns no message.
        """
        time.sleep(timeout)

    def ack(self, message: MessageT) -> None:
        """
        Acknowledge event
        """


class Producer(BaseProducer):
    """
    Dummy event producer.
    """

    def __init__(self, broker: str):
        super().__init__(broker)
        self.broker = BrokerURI.from_uri(broker)

    def produce(  # pylint: disable=too-many-arguments
        self,
        topic: str,
        value: MessageT,
        flush: bool = True,
        on_delivery: DeliveryCallBackT = None,
        fail_silently: bool = False,
    ) -> None:
        """
        Only logs the message, does not deliver.
        """
        logger.info(
            "Producing message {value=}.",
            extra={
                "topic": topic,
                "value": value,
                "flush": True,
            },
        )
        # TODO: call # on_delivery
