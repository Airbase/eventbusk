import pytest

from dataclasses import dataclass
from eventbusk import EventBus, Event

@dataclass
class Foo(Event):
    first: int

@dataclass
class Bar(Event):
    second: int



def test_bus_produce(mocker):
    # Given an instance of an event bus
    producer = mocker.Mock()
    Producer = mocker.patch("eventbusk.bus.Producer", return_value=producer)
    bus = EventBus(broker="kafka://localhost:9092")

    # Given events registered to certain topics
    bus.register_event("first_topic", Foo)
    bus.register_event("second_topic", Bar)
    foo_event = Foo(first=1)
    bar_event = Bar(second=1)

    # When we send events of a different types
    bus.send(foo_event)
    bus.send(bar_event)

    # Then check the underlying producer was correctly called with the right event json
    assert bus is not None
    producer.produce.assert_has_calls([
        mocker.call(topic="first_topic", value=b'{"first": 1}', on_delivery=mocker.ANY),
        mocker.call(topic="second_topic", value=b'{"second": 1}', on_delivery=mocker.ANY)
    ])
