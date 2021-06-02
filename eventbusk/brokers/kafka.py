from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Union

from confluent_kafka import Consumer as CConsumer
from confluent_kafka import KafkaError
from confluent_kafka import Producer as CProducer
from confluent_kafka import cimpl

from ..exceptions import ProducerError
from .base import BaseBrokerURI, BaseConsumer, BaseProducer, DeliveryCallBackT

# Delivery callback method `on_delivery` has the following type.
# DeliveryCallBackT = Callable[[KafkaError, cimpl.Message], None]


logger = logging.getLogger(__name__)


__all__ = [
    "BrokerURI",
    "Consumer",
    "Producer",
]

ConfigT = dict[str, Union[bool, int, str]]


@dataclass
class BrokerURI(BaseBrokerURI):
    """
    Broker URI

    Basic url is of the format: kafka://localhost:9092
    SASL support is enabled with the format: kafkas://user:pass@localhost:9092

    Usage
    ------
    >>> broker = BrokerURI("kafka://user:pass@localhost:9092")
    >>> broker.username

    """

    username: str
    password: str
    host: str
    port: int
    sasl: bool

    @classmethod
    def from_uri(cls, uri: str) -> BrokerURI:
        """
        Return an instance from a string URI
        """
        invalid_format = ValueError(
            "Broker URI(without SASL) should be of the format 'kafka://host:port' "
            "or 'kafkas://user:pass@host:port'"
        )

        if uri.startswith("kafka://"):
            sasl = False
        elif uri.startswith("kafkas://"):
            sasl = True
        else:
            raise invalid_format

        uri = uri.replace("kafka://", "").replace("kafkas://", "")
        parts = uri.split("@")

        if not sasl:
            if len(parts) > 1:
                raise invalid_format
            username, password = ("", "")
            domain_parts = parts[0].split(":")
            host, port = (domain_parts[0]), int(domain_parts[1])
        else:
            if len(parts) != 2:
                raise invalid_format

            username, password = parts[0].split(":")
            domain_parts = parts[1].split(":")
            host, port = (domain_parts[0]), int(domain_parts[1])
            if not (username and password):
                raise invalid_format

        if not (host and port):
            raise invalid_format

        return cls(
            username=username,
            password=password,
            host=host,
            port=port,
            sasl=sasl,
        )

    @property
    def default_config(self) -> ConfigT:
        """
        Default configuration for consumer or producer instances
        """
        props: ConfigT = {
            "bootstrap.servers": f"{self.host}:{self.port}",
        }
        if self.sasl:
            props.update(
                {
                    "sasl.mechanisms": "PLAIN",
                    "security.protocol": "SASL_SSL",
                    "sasl.username": self.username,
                    "sasl.password": self.password,
                }
            )
        return props.copy()


class Consumer(BaseConsumer):
    """
    Kafka consumer as a context manager.

    Automatically closes the consumer at the end of the context manager block.

    Example
    -------
    >>> with KafkaConsumer(broker, topic, group) as consumer:
           ...
    """

    def __init__(self, broker: str, topic: str, group: str):
        super().__init__()
        self.broker = BrokerURI.from_uri(broker)
        self.topic = topic
        self.group = group
        self._consumer: CConsumer = None

    def __repr__(self) -> str:
        return (
            f"<{self.__class__.__name__}("
            f"broker=*, "
            f"topic={self.topic}, "
            f"group='{self.group}')>"
        )

    def __enter__(self) -> Consumer:
        config = self.broker.default_config
        config.update(
            {
                "group.id": self.group,
                "auto.offset.reset": "latest",  # TODO: This will change per receiver
                "enable.auto.offset.store": False,  # TODO: autocommit?
            }
        )
        self._consumer = CConsumer(config)
        logger.info("Trying to subscribe")
        self._consumer.subscribe([self.topic])
        logger.info("Subscribed successfully")
        return self

    def __exit__(self, exc_type, exc_value, exc_traceback) -> None:
        self._consumer.close()

        if exc_type and exc_value and exc_traceback:
            logger.warning(
                "Kafka consumer error.",
                exc_info=True,
                extra=dict(
                    exc_type=exc_type, exc_value=exc_value, exc_traceback=exc_traceback
                ),
            )

    def poll(self, timeout: int):
        """
        Poll the topic for new messages
        """
        return self._consumer.poll(timeout)

    def ack(self, message):
        """
        Acknowledge the message
        """
        self._consumer.store_offsets(message=message)


class Producer(BaseProducer):
    """
    Kafka event producer.
    """

    def __init__(self, broker: str):
        super().__init__(broker)
        self.broker = BrokerURI.from_uri(broker)
        config = self.broker.default_config
        self._producer = CProducer(config)

    def produce(  # pylint: disable=too-many-arguments
        self,
        topic: str,
        value: str,
        flush: bool = True,
        on_delivery: DeliveryCallBackT = None,
         ffail_silently: bool = False,
    ) -> None:
        """
        Only logs the message, does not deliver.
        """
        logger.info(
            "Producing message.",
            extra={
                "topic": topic,
                "value": value,
                "flush": flush,
            },
        )
        try:
            # Trigger any available delivery report callbacks from previous produce
            self._producer.poll(0)
            self._producer.produce(topic=topic, value=value, on_delivery=on_delivery)
            if flush:
                self._producer.flush()
        except KafkaError as exc:
            if fail_silently:
                logger.warning(
                    "Error producing event.",
                    extra={"topic": topic, "flush": flush},
                    # Cannot add exc_info=True because of
                    # AttributeError: 'cimpl.KafkaError' object has no attribute '__traceback__'
                )
            else:
                raise ProducerError from exc
