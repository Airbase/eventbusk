"""
Generic interface for brokers
"""

from .kafka import KafkaConsumer, KafkaProducer


__all__ = [
    "Consumer",
    "Producer",
]



def consumer_factory(broker: str, topic: str, group: str):
    """
    Return a consumer instance for the specied broker url
    """
    # TODO: Only kafka supported for now.
    return KafkaConsumer(broker=broker, topic=topic, group=group)

Consumer = consumer_factory


def producer_factory(broker: str):
    """
    Return a consumer instance for the specied broker url
    """
    return KafkaProducer(broker)


Producer = producer_factory
