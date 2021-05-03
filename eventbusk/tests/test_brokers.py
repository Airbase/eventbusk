import pytest
from eventbusk.brokers import Consumer, Producer


@pytest.mark.parametrize("broker,topic,group", [
    ("kafka://user:pass@localhost:9092", "topicA", "groupB"),
    ("kafkas://user:pass@localhost:9092", "topicA", "groupB"),
    ("kafka://localhost:9092", "topicA", "groupB"),
    ("kafkas://localhost:9092", "topicA", "groupB"),
])
def test_consumer_factory(broker, topic, group):
    consumer = Consumer(broker=broker, topic=topic, group=group)

    if consumer.broker.username:
        assert consumer.broker.username == "user"
        assert consumer.broker.password == "pass"

    assert consumer.broker.host == "localhost"
    assert consumer.broker.port == "9092"
    assert consumer.topic == topic
    assert consumer.group == group
    if "kafkas" in broker:
        assert consumer.broker.ssl
    else:
        assert not consumer.broker.ssl

    assert consumer.broker.producer_props
    #assert consumer.broker.consumer_props
