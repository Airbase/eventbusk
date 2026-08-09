"""Test EventBus implementation."""

from __future__ import annotations

import json
import logging
import sys
import uuid
from dataclasses import dataclass
from types import SimpleNamespace
from typing import TYPE_CHECKING
from unittest.mock import MagicMock

from eventbusk import Event, EventBus

if TYPE_CHECKING:
    from pytest_mock import MockerFixture

logger = logging.getLogger(__name__)


@dataclass
class Foo(Event):
    """Dummy event."""

    first: int


@dataclass
class Bar(Event):
    """Dummy event."""

    second: int


BROKER = "kafka://localhost:9092"


def _make_mock_consumer(event_data: dict) -> tuple[MagicMock, MagicMock]:
    """Create a mock Consumer that yields one message with the given event data,
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
    """Test basic producer."""
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
        """Do nothing delivery handler."""
        logger.info(error, event)

    bus.send(foo_event, on_delivery=on_delivery)
    bus.send(bar_event, on_delivery=on_delivery)

    # Then check the underlying producer was correctly called with the right event json
    assert bus is not None
    calls = producer.produce.call_args_list
    assert len(calls) == 2

    first_payload = json.loads(calls[0].kwargs["value"].decode("utf-8"))
    assert calls[0].kwargs["topic"] == "first_topic"
    assert calls[0].kwargs["flush"] is True
    assert calls[0].kwargs["on_delivery"] is on_delivery
    assert first_payload == {
        "event_id": str(foo_event_uuid),
        "trace_id": None,
        "parent_span_id": None,
        "first": 1,
    }

    second_payload = json.loads(calls[1].kwargs["value"].decode("utf-8"))
    assert calls[1].kwargs["topic"] == "second_topic"
    assert calls[1].kwargs["flush"] is True
    assert calls[1].kwargs["on_delivery"] is on_delivery
    assert second_payload == {
        "event_id": str(bar_event_uuid),
        "trace_id": None,
        "parent_span_id": None,
        "second": 1,
    }


def test_bus_send_populates_trace_context_from_active_ddtrace_span(
    mocker: MockerFixture,
) -> None:
    """When ddtrace span exists, send should embed trace + parent span ids."""
    producer = mocker.Mock()
    mocker.patch("eventbusk.bus.Producer", return_value=producer)

    fake_span = SimpleNamespace(trace_id=12345, span_id=67890)
    fake_tracer = SimpleNamespace(current_span=lambda: fake_span)
    ddtrace_module = SimpleNamespace(tracer=fake_tracer)
    mocker.patch.dict(sys.modules, {"ddtrace": ddtrace_module})

    bus = EventBus(broker=BROKER)
    bus.register_event(topic="first_topic", event_type=Foo)

    event = Foo(first=10)
    bus.send(event)

    payload = json.loads(
        producer.produce.call_args.kwargs["value"].decode("utf-8")
    )
    assert payload["trace_id"] == "12345"
    assert payload["parent_span_id"] == "67890"


def test_bus_receive() -> None:
    """Test basic consumer."""
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


class _StaleConnectionError(Exception):
    """Stand-in for a recoverable, connection-shaped error."""


class _StaleConnectionSubError(_StaleConnectionError):
    """A subclass of a registered exception type."""


def test_on_error_handlers_default_to_empty_dict() -> None:
    """on_error handlers should default to an empty dict when not provided."""
    bus = EventBus(broker=BROKER)
    assert not bus._on_error_handlers  # pylint: disable=protected-access


def test_on_error_handlers_stored_from_init() -> None:
    """on_error handlers passed at init should be stored as a dict."""
    handler = MagicMock()

    bus = EventBus(broker=BROKER, on_error={_StaleConnectionError: handler})

    handlers = bus._on_error_handlers  # pylint: disable=protected-access
    assert handlers == {_StaleConnectionError: handler}


def test_on_error_handler_not_called_when_handler_succeeds(
    mocker: MockerFixture,
) -> None:
    """on_error handlers must not run on the successful path (they're for
    recovery, not per-message cleanup).
    """
    on_error_handler = mocker.Mock()

    bus = EventBus(broker=BROKER, on_error={_StaleConnectionError: on_error_handler})
    bus.register_event("first_topic", Foo)

    @bus.receive(event_type=Foo)
    def foo_processor(_event: Event) -> None:
        pass

    consumer, message = _make_mock_consumer({"first": 42})
    mocker.patch("eventbusk.bus.Consumer", return_value=consumer)

    foo_processor()  # pylint: disable=no-value-for-parameter

    on_error_handler.assert_not_called()
    consumer.ack.assert_called_once_with(message=message)


def test_on_error_handler_not_called_for_unregistered_exception_type(
    mocker: MockerFixture,
) -> None:
    """A handler registered for one exception type must not run when the
    receiver raises an unrelated exception type, so unrelated receiver bugs
    don't pay the recovery cost.
    """
    on_error_handler = mocker.Mock()

    bus = EventBus(broker=BROKER, on_error={_StaleConnectionError: on_error_handler})
    bus.register_event("first_topic", Foo)

    @bus.receive(event_type=Foo)
    def foo_processor(event: Event) -> None:
        raise ValueError("unrelated handler bug")

    consumer, _ = _make_mock_consumer({"first": 42})
    mocker.patch("eventbusk.bus.Consumer", return_value=consumer)

    foo_processor()  # pylint: disable=no-value-for-parameter

    on_error_handler.assert_not_called()
    consumer.ack.assert_not_called()


def test_on_error_handler_called_with_matching_exception(
    mocker: MockerFixture,
) -> None:
    """A handler registered for an exception type runs, and is passed the raised
    exception instance, when the receiver raises exactly that type. Message is
    not acked, so it will be redelivered.
    """
    on_error_handler = mocker.Mock()

    bus = EventBus(broker=BROKER, on_error={_StaleConnectionError: on_error_handler})
    bus.register_event("first_topic", Foo)

    raised = _StaleConnectionError("connection is closed")

    @bus.receive(event_type=Foo)
    def foo_processor(event: Event) -> None:
        raise raised

    consumer, _ = _make_mock_consumer({"first": 42})
    mocker.patch("eventbusk.bus.Consumer", return_value=consumer)

    foo_processor()  # pylint: disable=no-value-for-parameter

    on_error_handler.assert_called_once_with(raised)
    consumer.ack.assert_not_called()


def test_on_error_handler_not_called_for_subclass_of_registered_exception(
    mocker: MockerFixture,
) -> None:
    """Matching is by exact type, not subclass: a handler registered for a
    base exception type does not run when the receiver raises a subclass of
    it. Callers that need to handle a subclass must register it explicitly.
    """
    on_error_handler = mocker.Mock()

    bus = EventBus(broker=BROKER, on_error={_StaleConnectionError: on_error_handler})
    bus.register_event("first_topic", Foo)

    @bus.receive(event_type=Foo)
    def foo_processor(event: Event) -> None:
        raise _StaleConnectionSubError("connection is closed")

    consumer, _ = _make_mock_consumer({"first": 42})
    mocker.patch("eventbusk.bus.Consumer", return_value=consumer)

    foo_processor()  # pylint: disable=no-value-for-parameter

    on_error_handler.assert_not_called()
    consumer.ack.assert_not_called()


def test_failing_on_error_handler_does_not_crash_receiver(
    mocker: MockerFixture,
) -> None:
    """A raising on_error handler must not crash the receiver loop; the message
    is still left unacked for redelivery.
    """
    failing_handler = mocker.Mock(side_effect=RuntimeError("handler boom"))

    bus = EventBus(broker=BROKER, on_error={_StaleConnectionError: failing_handler})
    bus.register_event("first_topic", Foo)

    @bus.receive(event_type=Foo)
    def foo_processor(event: Event) -> None:
        raise _StaleConnectionError("connection is closed")

    consumer, _ = _make_mock_consumer({"first": 42})
    mocker.patch("eventbusk.bus.Consumer", return_value=consumer)

    foo_processor()  # pylint: disable=no-value-for-parameter

    failing_handler.assert_called_once()
    consumer.ack.assert_not_called()


def test_bus_receive_sets_trace_fields_and_tags_consumer_span(
    mocker: MockerFixture,
) -> None:
    """Receiver should restore trace fields and tag active consumer span."""
    bus = EventBus(broker=BROKER)
    bus.register_event("first_topic", Foo)

    seen_events: list[Event] = []

    @bus.receive(event_type=Foo)
    def foo_processor(event: Event) -> None:
        seen_events.append(event)

    consumer, message = _make_mock_consumer(
        {
            "event_id": str(uuid.uuid4()),
            "trace_id": "111",
            "parent_span_id": "222",
            "first": 42,
        }
    )
    mocker.patch("eventbusk.bus.Consumer", return_value=consumer)

    span = mocker.Mock()
    fake_tracer = SimpleNamespace(
        current_root_span=lambda: span,
        current_span=lambda: None,
    )
    ddtrace_module = SimpleNamespace(tracer=fake_tracer)
    mocker.patch.dict(sys.modules, {"ddtrace": ddtrace_module})

    foo_processor()  # pylint: disable=no-value-for-parameter

    assert len(seen_events) == 1
    assert seen_events[0].trace_id == "111"
    assert seen_events[0].parent_span_id == "222"
    span.set_tag.assert_any_call("airbase.trace_id", "111")
    span.set_tag.assert_any_call("eventbus.trace_id", "111")
    span.set_tag.assert_any_call("eventbus.parent_span_id", "222")
    consumer.ack.assert_called_once_with(message=message)

