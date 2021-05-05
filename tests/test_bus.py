from dataclasses import dataclass

import pytest

from eventbusk import Event, EventBus


@dataclass
class Foo(Event):
    first: int


@dataclass
class Bar(Event):
    second: int


def test_bus_send(mocker):
    """
    Test basic producer
    """
    # Given an instance of an event bus
    producer = mocker.Mock()
    Producer = mocker.patch("eventbusk.bus.Producer", return_value=producer)
    bus = EventBus(broker="kafka://localhost:9092")

    # Given events registered to certain topics
    bus.register_event(topic="first_topic", event_type=Foo)
    bus.register_event(topic="second_topic", event_type=Bar)
    foo_event = Foo(first=1)
    bar_event = Bar(second=1)

    # When we send events of a different types
    def on_delivery(error, event):
        """
        Do nothing delivery handler
        """
        pass

    bus.send(foo_event, on_delivery)
    bus.send(bar_event, on_delivery)

    # Then check the underlying producer was correctly called with the right event json
    assert bus is not None
    producer.produce.assert_has_calls(
        [
            mocker.call(
                topic="first_topic", value=b'{"first": 1}', on_delivery=on_delivery
            ),
            mocker.call(
                topic="second_topic", value=b'{"second": 1}', on_delivery=on_delivery
            ),
        ]
    )


def test_bus_agent(mocker):
    """
    Test basic consumer
    """
    # Given an instance of an event bus
    bus = EventBus(broker="kafka://localhost:9092")

    # Given events registered to certain topics
    bus.register_event("first_topic", Foo)
    bus.register_event("second_topic", Bar)

    # When consumer agents are linked to certain event types.
    @bus.agent(event_type=Foo)
    def foo_processor(event):
        pass

    @bus.agent(event_type=Bar)
    def bar_processor(event):
        pass

    # Then ensure agents are correctly registered
    assert foo_processor in bus.agents
    assert bar_processor in bus.agents