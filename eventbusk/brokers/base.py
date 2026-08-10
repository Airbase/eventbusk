"""Base interface for event consumer and producers."""

from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from collections.abc import Callable
from contextlib import ContextDecorator
from typing import TYPE_CHECKING, Self

from confluent_kafka import cimpl

if TYPE_CHECKING:
    from types import TracebackType

logger = logging.getLogger(__name__)


__all__ = [
    "BaseBrokerURI",
    "BaseConsumer",
    "BaseProducer",
]

# Type hints
# callback method `on_delivery` on the producer
type DeliveryCallback = Callable[..., None]
type Message = bytes | cimpl.Message


class BaseBrokerURI(ABC):
    """Base class that defines the interface for all broker URIs."""

    @classmethod
    @abstractmethod
    def from_uri(cls, uri: str) -> BaseBrokerURI:
        """Return a instance created from a URI."""


class BaseConsumer(ContextDecorator, ABC):
    """Base class for consumers.

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

    def __enter__(self) -> Self:
        return self

    def __exit__(  # pylint: disable=too-many-positional-arguments
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        exc_traceback: TracebackType | None,
    ) -> None:
        pass

    @abstractmethod
    def poll(self, timeout: int) -> Message | None:
        """Poll for a specified time in seconds for new messages."""

    @abstractmethod
    def ack(self, message: Message | None) -> None:
        """Acknowledge successful consumption of a message."""


class BaseProducer(ABC):
    """Base class for producers."""

    def __repr__(self) -> str:
        return f"<{self.__class__.__name__}(broker=*>"

    @abstractmethod
    def __init__(self, broker: str) -> None:
        super().__init__()

    @abstractmethod
    def produce(  # pylint: disable=too-many-arguments
        self,
        topic: str,
        value: bytes,
        *,
        headers: list[tuple[str, bytes]] | None = None,
        flush: bool = True,
        on_delivery: DeliveryCallback = None,
        fail_silently: bool = False,
    ) -> None:
        """Send a message on the specific topic.

        Arguments:
        ---------
        topic:
            The name of the topic
        value:
            Serialized message to send.
        headers:
            Optional message headers, as a list of (key, value) tuples. Used to
            carry out-of-band metadata such as trace context.
        on_delivery:
            Callback function on delivery of a message.
        flush:
            Flush any pending messages after every send.
            Useful for brokers like Kafka which do batches.
        fail_silently:
            If True, ignore all delivery errors.

        """
