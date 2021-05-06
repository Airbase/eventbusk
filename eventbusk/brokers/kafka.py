from __future__ import annotations

import logging
from contextlib import ContextDecorator
from dataclasses import dataclass

from confluent_kafka import Consumer, Producer

logger = logging.getLogger(__name__)


__all__ = [
    "KafkaConsumer",
    "KafkaProducer",
]


@dataclass
class Broker:
    """
    Broker URI

    Basic url is of the format: kafka://localhost:9092
    SASL support is enabled with the format: kafkas://user:pass@localhost:9092

    Usage
    ------
    >>> broker = Broker("kafka://user:pass@localhost:9092")
    >>> broker.username

    """

    username: str
    password: str
    host: str
    port: int
    sasl: bool

    @classmethod
    def from_uri(cls, uri: str) -> Broker:
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
            username=username, password=password, host=host, port=port, sasl=sasl
        )

    @property
    def default_props(self):
        props = {
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


class KafkaConsumer(ContextDecorator):
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
        self.broker = Broker.from_uri(broker)
        self.topic = topic
        self.group = group
        self.consumer: Consumer = None

    def __repr__(self):
        return (
            f"<{self.__class__.__name__}("
            f"broker=Broker(user=*, pass=*, host=*, port=*), "
            f"topic=Topic({self.topic}), "
            f"group='{self.group}')>"
        )

    def __enter__(self):
        props = self.broker.default_props
        props.update(
            {
                "group.id": self.group,
                "auto.offset.reset": "earliest",  # TODO: This will change per agent
                "enable.auto.offset.store": False,  # TODO: autocommit?
            }
        )
        self.consumer = Consumer(props)
        self.consumer.subscribe([self.topic])
        return self.consumer

    def __exit__(self, type, value, traceback):
        self.consumer.close()

        if type and value and traceback:
            logger.exception(f"KafkaConsumer error. [{self}]", exc_info=True)


def kafka_producer_factory(broker: str) -> Producer:
    broker_obj = Broker.from_uri(broker)
    props = broker_obj.default_props
    return Producer(props)


KafkaProducer = kafka_producer_factory
