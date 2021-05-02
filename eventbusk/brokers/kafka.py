from __future__ import annotations

from contextlib import ContextDecorator
from dataclasses import dataclass

from confluent_kafka import Consumer, Producer

__all__ = [
    "KafkaConsumer",
    "KafkaProducer",
]


@dataclass
class Broker:
    """
    Broker URI

    Usage
    ------
    >>> broker = Broker("kafka://user:pass@localhost:9092")
    >>> broker.username
    """

    username: str
    password: str
    host: str
    port: int
    ssl: bool

    @classmethod
    def from_uri(cls, uri: str) -> Broker:
        invalid_format = ValueError(
            "Broker URI should be of the format 'kafka[s]://user:pass@host:port'"
        )

        if uri.startswith("kafka://"):
            ssl = False
        elif uri.startswith("kafkas://"):
            ssl = True
        else:
            raise invalid_format

        uri = uri.replace("kafka://", "").replace("kafkas://", "")
        parts = uri.split("@")
        if len(parts) == 1:
            username = None
            password = None
            host, port = parts[0].split(":")
        elif len(parts) == 2:
            username, password = parts[0].split(":")
            host, port = parts[1].split(":")
        else:
            raise invalid_format

        return cls(username=username, password=password, host=host, port=port, ssl=ssl)

    def _build_props(self):
        props = {
            "bootstrap.servers": f"{self.host}:{self.port}",
            "sasl.mechanisms": "PLAIN",
            "security.protocol": "SASL_SSL" if self.ssl else "SASL_PLAINTEXT",
        }
        if self.username and self.password:
            props.update(
                {
                    "sasl.username": self.username,
                    "sasl.password": self.password,
                }
            )
        return props.copy()

    @property
    def consumer_props(self):
        props = self._build_props()
        props.update(
            {
                "group.id": self.group,
                "auto.offset.reset": "earliest",
                "enable.auto.offset.store": False,  # TODO: autocommit?
            }
        )
        return props

    @property
    def producer_props(self):
        # TODO: create topics off
        return self._build_props()


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
        props = self.broker.consumer_props
        self.consumer = Consumer(props)
        self.consumer.subscribe([self.topic])
        return self.consumer

    def __exit__(self, type, value, traceback):
        self.consumer.close()

        if type and value and traceback:
            logger.exception(
                f"{self.__class__.__name__} error. [{self}]", exc_info=True
            )


def kafka_producer_factory(broker: str) -> Producer:
    broker = Broker.from_uri(broker)
    props = broker.producer_props
    print(props)
    return Producer(props)


KafkaProducer = kafka_producer_factory
