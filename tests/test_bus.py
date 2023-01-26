"""
Test EventBus implementation
"""
from __future__ import annotations

import logging
import uuid
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

    foo_event_uuid = uuid.uuid4()
    bar_event_uuid = uuid.uuid4()
    foo_event = Foo(first=1)
    foo_event.event_id = foo_event_uuid
    bar_event = Bar(second=1)
    bar_event.event_id = bar_event_uuid

    # When we send events of a different types
    def on_delivery(error: str, event: Event) -> None:
        """
        Do nothing delivery handler
        """
        logger.info(error, event)

    bus.send(foo_event, on_delivery)
    bus.send(bar_event, on_delivery)

    # Then check the underlying producer was correctly called with the right event json
    assert bus is not None
    producer.produce.assert_has_calls(
        [
            mocker.call(
                topic="first_topic",
                value=bytes(
                    f'{{"event_id": "{str(foo_event_uuid)}", "first": 1}}', "utf-8"
                ),
                flush=True,
                on_delivery=on_delivery,
            ),
            mocker.call(
                topic="second_topic",
                value=bytes(
                    f'{{"event_id": "{str(bar_event_uuid)}", "second": 1}}', "utf-8"
                ),
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
