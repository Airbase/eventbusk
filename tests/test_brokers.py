from typing import Any

import pytest
from confluent_kafka import KafkaError  # type: ignore
from pytest_mock import MockerFixture

from eventbusk.brokers import Consumer, Producer
from eventbusk.brokers.dummy import BrokerURI as DummyBrokerURI
from eventbusk.brokers.dummy import Consumer as DummyConsumer
from eventbusk.brokers.dummy import Producer as DummyProducer
from eventbusk.brokers.kafka import BrokerURI as KafkaBrokerURI
from eventbusk.brokers.kafka import Consumer as KafkaConsumer
from eventbusk.brokers.kafka import Producer as KafkaProducer
from eventbusk.exceptions import ProducerError


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

    if "kafka" in broker:
        assert isinstance(consumer, KafkaConsumer)
    if isinstance(consumer, KafkaConsumer):
        # Then check username/passsword is correctly set if supported
        if "username" in broker:
            assert consumer.broker.username == "username"
        if "password" in broker:
            assert consumer.broker.password == "password"

        assert repr(consumer)
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

    if "dummy" in broker:
        assert isinstance(consumer, DummyConsumer)
        assert repr(consumer)


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
    ["kafka://localhost:9092", "kafkas://username:password@localhost:9092", "dummy://"],
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
        assert repr(producer)
        assert isinstance(producer, KafkaProducer)
    elif "dummy" in broker:
        assert repr(producer)
        assert isinstance(producer, DummyProducer)


@pytest.mark.parametrize(
    "broker",
    ["foobar://"],
)
def test_producer_factory_bad_broker(broker: str) -> None:
    """
    Test if producer factory raises exception on invalid broker URI
    """
    # Given a broker URI

    # Then ensure an exception is raised
    with pytest.raises(ValueError):
        # When a producer is initialized
        producer = Producer(broker=broker)


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
    assert isinstance(consumer.broker, DummyBrokerURI)
    assert consumer.topic == "mytopic"
    assert consumer.group == "mygroup"

    consumer.poll(timeout=1)
    consumer.ack(message="lorem ipsum")


# Kafka broker
# -------------
def test_kafka_broker_uri() -> None:
    """
    Test BrokerURI functionality
    """
    with pytest.raises(ValueError):
        KafkaBrokerURI.from_uri("foobar://localhost:9092")

    broker = KafkaBrokerURI.from_uri("kafka://localhost:9092")
    assert not broker.username
    assert not broker.password
    assert broker.host == "localhost"
    assert broker.port == 9092


def test_kafka_producer(
    mocker: MockerFixture, topic: str = "foo", value: str = "lorem ipsum"
) -> None:
    """
    Test producing to kafka
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
    # Then ensure underlying producer is called
    cproducer.produce.assert_called_once_with(
        topic="foo", value=value, on_delivery=None
    )
    cproducer.flush.assert_called_once()

    # When we produce an event and explicitly not flush
    producer.produce(topic=topic, value=value, flush=False)
    # Then ensure underlying flush is not called again
    cproducer.flush.assert_called_once()


def test_kafka_producer_error(
    mocker: MockerFixture, topic: str = "foo", value: str = "lorem ipsum"
) -> None:
    """
    Test kafka producer error handling
    """
    # Given a producer that errors out
    cproducer = mocker.Mock()

    def raise_exc(*args: Any, **kwargs: Any) -> None:
        raise KafkaError(KafkaError.BROKER_NOT_AVAILABLE)

    cproducer.produce.side_effect = raise_exc
    CProducer = mocker.patch(
        "eventbusk.brokers.kafka.CProducer", return_value=cproducer
    )
    producer = KafkaProducer(broker="kafka://localhost:9092")

    # When we produce an event
    with pytest.raises(ProducerError):
        producer.produce(topic=topic, value=value)

    # When we explicitly ask to fail silently
    # Then ensure we raise no exception
    producer.produce(topic=topic, value=value, fail_silently=True)
    cproducer.flush.assert_not_called()


def test_kafka_consumer(
    mocker: MockerFixture, message: str = "lorem ipsum", timeout: int = 0
) -> None:
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
        assert repr(consumer)

        # When a consumer is polled and a message is acknowledge
        msg = consumer.poll(timeout=timeout)
        consumer.ack(message=msg)

    # Then ensure underlying confluent consumer is correctly called
    cconsumer.poll.assert_called_once_with(timeout)
    cconsumer.store_offsets.assert_called_once_with(message=msg)
