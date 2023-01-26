"""
Base interface for event consumer and producers.
"""
from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from contextlib import ContextDecorator
from types import TracebackType
from typing import Callable, Optional, Union

from confluent_kafka import cimpl  # type: ignore

logger = logging.getLogger(__name__)


__all__ = [
    "BaseBrokerURI",
    "BaseConsumer",
    "BaseProducer",
]

# Type hints
# callback method `on_delivery` on the producer
DeliveryCallBackT = Callable[..., None]
MessageT = Union[str, bytes, cimpl.Message]


class BaseBrokerURI(ABC):
    """
    Base class that defines the interface for all broker URIs
    """

    @classmethod
    @abstractmethod
    def from_uri(cls, uri: str) -> BaseBrokerURI:
        """
        Return a instance created from a URI
        """


class BaseConsumer(ContextDecorator, ABC):
    """
    Base class for consumers

    All event consumers are exposed as a ContextDecorator, so it can be used via a
    `with` statement and any connections are automatically closed on exit.
    """

    broker: BaseBrokerURI
    topic: str
    group: str

    def __repr__(self) -> str:
        return (
            f"<{self.__class__.__name__}("
            f"broker=*, "
            f"topic={self.topic}, "
            f"group='{self.group}')>"
        )

    def __enter__(self) -> BaseConsumer:
        return self

    def __exit__(
        self,
        exc_type: Optional[type[BaseException]],
        exc_value: Optional[BaseException],
        exc_traceback: Optional[TracebackType],
    ) -> None:
        pass

    @abstractmethod
    def poll(self, timeout: int) -> Optional[MessageT]:  # type: ignore
        """
        Poll for a specified time in seconds for new messages
        """

    @abstractmethod
    def ack(self, message: str) -> None:
        """
        Acknowledge successful consumption of a message.
        """


class BaseProducer(ABC):
    """
    Base class for producers
    """

    def __repr__(self) -> str:
        return f"<{self.__class__.__name__}(" f"broker=*>"

    @abstractmethod
    def __init__(self, broker: str):
        super().__init__()

    @abstractmethod
    def produce(  # type: ignore # pylint: disable=too-many-arguments
        self,
        topic: str,
        value: MessageT,
        flush: bool = True,
        on_delivery: DeliveryCallBackT = None,
        fail_silently: bool = False,
    ) -> None:
        """
        Send a message on the specific topic.

        Arguments
        ----------
        topic:
            The name of the topic
        value:
            Serialized message to send.
        on_delivery:
            Callback function on delivery of a message.
        flush:
            Flush any pending messages after every send.
            Useful for brokers like Kafka which do batches.
        fail_silently:
            If True, ignore all delivery errors.
        """
