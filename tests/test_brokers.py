import pytest

from eventbusk.brokers import Consumer, Producer
from eventbusk.brokers.dummy import DummyConsumer, DummyProducer, DummyBroker
from eventbusk.brokers.kafka import KafkaConsumer, KafkaProducer

# Factories
# ---------
@pytest.mark.parametrize(
    "broker,topic,group",
    [
        ("kafka://localhost:9092", "mytopic", "mygroup"),
        ("kafkas://username:password@localhost:9092", "mytopic", "mygroup"),
        ("dummy://localhost:9092", "mytopic", "mygroup"),
    ],
)
def test_consumer_factory(broker: str, topic: str, group: str) -> None:
    # Given broker uri, and optionally topic and group

    # When a consumer is instantiated with the broker and dummy topic, group
    consumer = Consumer(broker=broker, topic=topic, group=group)

    # Then check username/passsword is correctly set if supported
    if "username" in broker:
        assert consumer.broker.username == "username"
    if "password" in broker:
        assert consumer.broker.password == "password"

    if "kafka" in broker:
        assert isinstance(consumer, KafkaConsumer)
        assert consumer.broker.host == "localhost"
        assert consumer.broker.port == 9092
        assert consumer.topic == topic
        assert consumer.group == group
        if "kafkas" in broker:
            assert consumer.broker.sasl
        else:
            assert not consumer.broker.sasl
        assert consumer.broker.default_props
    elif "dummy" in broker:
        assert isinstance(consumer, DummyConsumer)
    else:
        raise ValueError("Unsupported broker.")

@pytest.mark.parametrize(
    "broker,topic,group",
    [
        ("", "mytopic", "mygroup"),
        ("kafka://username:password@localhost:9092", "mytopic", "mygroup"),
        ("kafka://:9092", "mytopic", "mygroup"),
        ("kafkas://localhost:9092", "mytopic", "mygroup"),
        ("kafkas://:pass@localhost:9092", "mytopic", "mygroup"),
        ("kafka://localhost:", "mytopic", "mygroup"),
        ("http://user:pass@localhost:9092", "mytopic", "mygroup"),
        ("dummyr://", "mytopic", "mygroup"),
    ],
)
def test_consumer_factory_bad_broker(broker: str, topic: str, group: str) -> None:
    """
    Test if consumer factory errors out on an invalid broker uri
    """
    # Given invalid broker uri

    # Then ensure it raises an exception
    with pytest.raises(ValueError):
        # When Consumer is instantiated
        with Consumer(broker=broker, topic=topic, group=group) as consumer:

            assert consumer is not None


@pytest.mark.parametrize(
    "broker",
    [
        "kafka://localhost:9092",
        "kafkas://username:password@localhost:9092",
        "dummy://",
    ],
)
def test_producer_factory(broker: str) -> None:
    """
    Test if producer factory returns the correct producer.
    """
    # Given a broker URI

    # When a producer is initialized
    producer = Producer(broker=broker)

    # Then ensure the correct producer is instantiated
    if "kafka" in broker:
        assert producer is not None
    elif "dummy" in broker:
        assert isinstance(producer, DummyProducer)
    else:
        raise ValueError("Unsupported broker.")


# Dummy broker
# -------------

def test_dummy_producer() -> None:
    """
    Test basic dummy producer functionality
    """
    producer = DummyProducer(broker="dummy://")
    producer.produce(topic="foo", value="lorem ipsum")


def test_dummy_consumer() -> None:
    """
    Test basic dummy consumer functionality
    """
    consumer = DummyConsumer(broker="dummy://", topic="mytopic", group="mygroup")
    assert isinstance(consumer.broker, DummyBroker)
    assert consumer.topic == "mytopic"
    assert consumer.group == "mygroup"

    consumer.poll(timeout=1)
    consumer.ack(message="lorem ipsum")


# Kafka broker
# -------------
def test_kafka_producer() -> None:
    """
    Test basic kafka producer functionality
    """
    producer = KafkaProducer(broker="kafka://localhost:9092")
    producer.produce(topic="foo", value="lorem ipsum")


def test_kafka_consumer() -> None:
    """
    Test basic kafka consumer functionality
    """
    consumer = KafkaConsumer(broker="kafka://localhost:9092", topic="mytopic", group="mygroup")
    # consumer# .produce(topic="foo", value="lorem ipsum")
