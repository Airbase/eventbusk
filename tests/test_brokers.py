import pytest

from eventbusk.brokers import Consumer, Producer


@pytest.mark.parametrize("broker,topic,group", [
    ("kafka://username:password@localhost:9092", "mytopic", "mygroup"),
    ("kafkas://username:password@localhost:9092", "mytopic", "mygroup"),
    ("kafka://localhost:9092", "mytopic", "mygroup"),
    ("kafkas://localhost:9092", "mytopic", "mygroup"),
])
def test_consumer_factory(broker, topic, group):
    consumer = Consumer(broker=broker, topic=topic, group=group)

    if "username" in broker:
        assert consumer.broker.username == "username"
    if "password" in broker:
        assert consumer.broker.password == "password"

    assert consumer.broker.host == "localhost"
    assert consumer.broker.port == "9092"
    assert consumer.topic == topic
    assert consumer.group == group
    if "kafkas" in broker:
        assert consumer.broker.ssl
    else:
        assert not consumer.broker.ssl

    assert consumer.broker.producer_props
    # assert consumer.broker.consumer_props  # TODO


@pytest.mark.parametrize("broker,topic,group", [
    ("http://user:pass@localhost:9092", "mytopic", "mygroup"),
    ("kafkas://:pass@localhost:9092", "mytopic", "mygroup"),
    ("kafka://localhost:", "mytopic", "mygroup"),
    ("kafka://:9092", "mytopic", "mygroup"),
    ("", "mytopic", "mygroup"),
])
def test_consumer_factory_bad_broker(broker, topic, group):
    with pytest.raises(ValueError):
        Consumer(broker=broker, topic=topic, group=group)
