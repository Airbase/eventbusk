"""
Generic interface for brokers
"""
from __future__ import annotations

import logging

from .base import BaseConsumer, BaseProducer, DeliveryCallBackT
from .dummy import Consumer as DummyConsumer
from .dummy import Consumer as DummyProducer
from .kafka import Consumer as KafkaConsumer
from .kafka import Producer as KafkaProducer

logger = logging.getLogger(__name__)


__all__ = ["Consumer", "Producer", "DeliveryCallBackT"]


def consumer_factory(broker: str, topic: str, group: str) -> BaseConsumer:
    """
    Return a consumer instance for the specied broker url
    """
    if broker.startswith("kafka"):
        return KafkaConsumer(broker=broker, topic=topic, group=group)
    elif broker.startswith("dummy"):
        return DummyConsumer(broker=broker, topic=topic, group=group)
    else:
        raise ValueError("Unsupported broker.")


Consumer = consumer_factory


def producer_factory(broker: str) -> BaseProducer:
    """
    Return a producer instance for the specied broker url
    """
    if broker.startswith("kafka"):
        return KafkaProducer(broker)
    elif broker.startswith("dummy"):
        return DummyProducer(broker)
    else:
        raise ValueError("Unsupported broker.")


Producer = producer_factory
