import pytest

from eventbusk.brokers import Consumer, Producer


@pytest.mark.parametrize(
    "broker,topic,group",
    [
        ("kafka://localhost:9092", "mytopic", "mygroup"),
        ("kafkas://username:password@localhost:9092", "mytopic", "mygroup"),
    ],
)
def test_consumer_factory(broker, topic, group):
    consumer = Consumer(broker=broker, topic=topic, group=group)

    if "username" in broker:
        assert consumer.broker.username == "username"
    if "password" in broker:
        assert consumer.broker.password == "password"

    assert consumer.broker.host == "localhost"
    assert consumer.broker.port == 9092
    assert consumer.topic == topic
    assert consumer.group == group
    if "kafkas" in broker:
        assert consumer.broker.sasl
    else:
        assert not consumer.broker.sasl

    assert consumer.broker.default_props


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
    ],
)
def test_consumer_factory_bad_broker(broker, topic, group):
    with pytest.raises(ValueError):
        with Consumer(broker=broker, topic=topic, group=group) as consumer:
            assert consumer is not None


@pytest.mark.parametrize(
    "broker",
    [
        "kafka://localhost:9092",
        "kafkas://username:password@localhost:9092",
    ],
)
def test_producer_factory(broker):
    producer = Producer(broker=broker)
    assert producer is not None
