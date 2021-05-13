import pytest

from eventbusk.brokers import Consumer, Producer
from eventbusk.brokers.dummy import BrokerURI as DummyBrokerURI
from eventbusk.brokers.dummy import Consumer as DummyConsumer
from eventbusk.brokers.dummy import Producer as DummyProducer
from eventbusk.brokers.kafka import BrokerURI as KafkaBrokerURI
from eventbusk.brokers.kafka import Consumer as KafkaConsumer
from eventbusk.brokers.kafka import Producer as KafkaProducer


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
        assert consumer.broker.default_config
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
    print(type(consumer.broker), DummyBrokerURI)
    assert isinstance(consumer.broker, DummyBrokerURI)
    assert consumer.topic == "mytopic"
    assert consumer.group == "mygroup"

    consumer.poll(timeout=1)
    consumer.ack(message="lorem ipsum")


# Kafka broker
# -------------
def test_kafka_producer(mocker, topic="foo", value="lorem ipsum") -> None:
    """
    Test basic kafka producer functionality
    """
    # Given a Kafka producer that
    # mocks the underlying confluent producer so it doesn't try to connect
    cproducer = mocker.Mock()
    CProducer = mocker.patch(
        "eventbusk.brokers.kafka.CProducer", return_value=cproducer
    )
    producer = KafkaProducer(broker="kafka://localhost:9092")

    # When we produce an event
    producer.produce(topic=topic, value=value)
    assert cproducer._produce.called_once_with(topic="foo", value=value)


def test_kafka_consumer(mocker, message: str = "lorem ipsum", timeout: int = 0) -> None:
    """
    Test basic kafka consumer functionality
    """
    # Given a Kafka consumer with a mocked underlying Confluent consumer
    # to avoid making a connection
    cconsumer = mocker.Mock()
    cconsumer.poll.return_value = message
    CConsumer = mocker.patch(
        "eventbusk.brokers.kafka.CConsumer", return_value=cconsumer
    )
    with KafkaConsumer(
        broker="kafka://localhost:9092", topic="mytopic", group="mygroup"
    ) as consumer:

        # When a consumer is polled and a message is acknowledge
        message = consumer.poll(timeout=timeout)
        consumer.ack(message=message)

    # Then ensure underlying confluent consumer is correctly called
    cconsumer.poll.assert_called_once_with(timeout)
    cconsumer.store_offsets.assert_called_once_with(message=message)
