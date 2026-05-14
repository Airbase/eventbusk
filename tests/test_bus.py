"""
Test EventBus implementation
"""

from __future__ import annotations

import json
import logging
import uuid
from dataclasses import dataclass
from unittest.mock import MagicMock

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


BROKER = "kafka://localhost:9092"


def _make_mock_consumer(event_data: dict) -> tuple[MagicMock, MagicMock]:
    """
    Create a mock Consumer that yields one message with the given event data,
    then raises KeyboardInterrupt to exit the receive loop.
    """
    message = MagicMock()
    message.error.return_value = None
    message.value.return_value = json.dumps(event_data).encode("utf-8")

    consumer = MagicMock()
    consumer.poll.side_effect = [message, KeyboardInterrupt]
    consumer.__enter__ = MagicMock(return_value=consumer)
    consumer.__exit__ = MagicMock(return_value=False)
    return consumer, message


def test_bus_send(mocker: MockerFixture) -> None:
    """
    Test basic producer
    """
    # Given an instance of an event bus
    producer = mocker.Mock()
    mocker.patch("eventbusk.bus.Producer", return_value=producer)
    bus = EventBus(broker=BROKER)

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

    bus.send(foo_event, on_delivery=on_delivery)
    bus.send(bar_event, on_delivery=on_delivery)

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
    bus = EventBus(broker=BROKER)

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


def test_hooks_default_to_empty_lists() -> None:
    """Hooks should default to empty lists when not provided."""
    bus = EventBus(broker=BROKER)
    assert not bus._before_receive_hooks  # pylint: disable=protected-access
    assert not bus._after_receive_hooks  # pylint: disable=protected-access


def test_hooks_stored_from_init() -> None:
    """Hooks passed at init should be stored as lists."""
    hook1 = MagicMock()
    hook2 = MagicMock()

    bus = EventBus(
        broker=BROKER,
        before_receive=[hook1, hook2],
        after_receive=[hook1],
    )

    assert bus._before_receive_hooks == [hook1, hook2]  # pylint: disable=protected-access
    assert bus._after_receive_hooks == [hook1]  # pylint: disable=protected-access


def test_hooks_execute_in_order_around_handler(mocker: MockerFixture) -> None:
    """before_receive hooks run before the handler, after_receive hooks run after."""
    manager = mocker.Mock()
    bus = EventBus(
        broker=BROKER,
        before_receive=[manager.before_hook],
        after_receive=[manager.after_hook],
    )
    bus.register_event("first_topic", Foo)

    @bus.receive(event_type=Foo)
    def foo_processor(event: Event) -> None:
        manager.handler(event)

    consumer, message = _make_mock_consumer({"first": 42})
    mocker.patch("eventbusk.bus.Consumer", return_value=consumer)

    foo_processor()  # pylint: disable=no-value-for-parameter

    assert manager.mock_calls == [
        mocker.call.before_hook(),
        mocker.call.handler(mocker.ANY),
        mocker.call.after_hook(),
    ]
    consumer.ack.assert_called_once_with(message=message)


def test_after_hooks_run_when_handler_raises(mocker: MockerFixture) -> None:
    """after_receive hooks run even when the handler raises; message is not acked."""
    before_hook = mocker.Mock()
    after_hook = mocker.Mock()

    bus = EventBus(
        broker=BROKER,
        before_receive=[before_hook],
        after_receive=[after_hook],
    )
    bus.register_event("first_topic", Foo)

    @bus.receive(event_type=Foo)
    def foo_processor(event: Event) -> None:
        raise ValueError("handler error")

    consumer, _ = _make_mock_consumer({"first": 42})
    mocker.patch("eventbusk.bus.Consumer", return_value=consumer)

    foo_processor()  # pylint: disable=no-value-for-parameter

    before_hook.assert_called_once()
    after_hook.assert_called_once()
    consumer.ack.assert_not_called()
