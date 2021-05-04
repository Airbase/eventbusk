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
    bus = EventBus(broker="kafka://localhost:9092")
    producer = mocker.patch("eventbusk.bus.Producer", autospec=True)
    bus.register_event("first_topic", Foo)
    bus.register_event("second_topic", Bar)
    foo_event = Foo(first=1)
    bar_event = Bar(second=1)

    # When we send events of a different types
    print(bus._event_to_topic)
    bus.send(foo_event)
    bus.send(bar_event)

    # Then
    assert bus is not None
    producer.produce.assert_is_called()
