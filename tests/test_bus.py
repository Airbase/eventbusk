"""
Test EventBus implementation
"""
from __future__ import annotations

import logging
from dataclasses import dataclass

from pytest_mock import MockerFixture

from eventbusk import Event, EventBus

logger = logging.getLogger(__name__)


@dataclass
class Foo(Event):
    """
    Dummy event
    """

    first: int


@dataclass
class Bar(Event):
    """
    Dummy event
    """

    second: int


def test_bus_send(mocker: MockerFixture) -> None:
    """
    Test basic producer
    """
    # Given an instance of an event bus
    producer = mocker.Mock()
    mocker.patch("eventbusk.bus.Producer", return_value=producer)
    bus = EventBus(broker="kafka://localhost:9092")

    # Given events registered to certain topics
    bus.register_event(topic="first_topic", event_type=Foo)
    bus.register_event(topic="second_topic", event_type=Bar)
    foo_event = Foo(first=1)
    bar_event = Bar(second=1)

    # When we send events of a different types
    def on_delivery(error: str, event: Event) -> None:
        """
        Do nothing delivery handler
        """
        logger.info(error, event)

    mocker.patch("eventbusk.bus.uuid.uuid4", return_value="foo_event_you_you_id")
    bus.send(foo_event, on_delivery)
    mocker.patch("eventbusk.bus.uuid.uuid4", return_value="bar_event_you_you_id")
    bus.send(bar_event, on_delivery)

    # Then check the underlying producer was correctly called with the right event json
    assert bus is not None
    producer.produce.assert_has_calls(
        [
            mocker.call(
                topic="first_topic",
                value=b'{"event_id": "foo_event_you_you_id", "first": 1}',
                flush=True,
                on_delivery=on_delivery,
            ),
            mocker.call(
                topic="second_topic",
                value=b'{"event_id": "bar_event_you_you_id", "second": 1}',
                flush=True,
                on_delivery=on_delivery,
            ),
        ]
    )


def test_bus_receive() -> None:
    """
    Test basic consumer
    """
    # Given an instance of an event bus
    bus = EventBus(broker="kafka://localhost:9092")

    # Given events registered to certain topics
    bus.register_event("first_topic", Foo)
    bus.register_event("second_topic", Bar)

    # When consumer receivers are linked to certain event types.
    @bus.receive(event_type=Foo)
    def foo_processor(event: Event) -> None:
        logger.info(event)

    @bus.receive(event_type=Bar)
    def bar_processor(event: Event) -> None:
        logger.info(event)

    # Then ensure receivers are correctly registered
    assert foo_processor in bus.receivers
    assert bar_processor in bus.receivers
