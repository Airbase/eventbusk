"""Test EventBus implementation."""

from __future__ import annotations

import json
import logging
import uuid
from contextlib import contextmanager
from dataclasses import dataclass
from typing import TYPE_CHECKING
from unittest.mock import MagicMock

from eventbusk import Event, EventBus, TracingConfig

if TYPE_CHECKING:
    from collections.abc import Callable, Iterator

    from pytest_mock import MockerFixture

    SpanManagerT = Callable[[str, str, "dict | None"], Iterator[MagicMock]]

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
        "first": 1,
    }

    second_payload = json.loads(calls[1].kwargs["value"].decode("utf-8"))
    assert calls[1].kwargs["topic"] == "second_topic"
    assert calls[1].kwargs["flush"] is True
    assert calls[1].kwargs["on_delivery"] is on_delivery
    assert second_payload == {
        "event_id": str(bar_event_uuid),
        "second": 1,
    }


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


# ---------------------------------------------------------------------------
# Tracing hooks
# ---------------------------------------------------------------------------


def _send_one(bus: EventBus, mocker: MockerFixture) -> dict:
    """Send a single Foo through `bus` and return the produce() kwargs.

    The event id is pinned so payloads from two different buses can be
    compared byte for byte.
    """
    producer = mocker.Mock()
    mocker.patch("eventbusk.bus.Producer", return_value=producer)
    bus.register_event(topic="first_topic", event_type=Foo)
    event = Foo(first=1)
    event.event_id = uuid.UUID("11111111-2222-3333-4444-555555555555")
    bus.send(event)
    return producer.produce.call_args.kwargs


def _make_span_manager() -> tuple[SpanManagerT, dict]:
    """Build a span_manager that records how and when it was called."""
    recorded: dict = {"order": []}

    @contextmanager
    def span_manager(
        event_fqn: str, receiver_fqn: str, trace_ctx: dict | None
    ) -> Iterator[MagicMock]:
        recorded["args"] = (event_fqn, receiver_fqn, trace_ctx)
        recorded["order"].append("enter")
        yield MagicMock()
        recorded["order"].append("exit")

    return span_manager, recorded


def test_tracing_config_defaults_to_no_hooks() -> None:
    """An empty TracingConfig should disable every hook."""
    config = TracingConfig()

    assert config.inject_trace is None
    assert config.extract_trace is None
    assert config.span_manager is None


def test_send_without_tracing_passes_no_headers(mocker: MockerFixture) -> None:
    """A bus with no tracing configured should not attach headers."""
    kwargs = _send_one(EventBus(broker=BROKER), mocker)

    assert kwargs["headers"] is None


def test_send_attaches_headers_from_inject_trace(mocker: MockerFixture) -> None:
    """Whatever inject_trace returns should reach the broker verbatim."""
    bus = EventBus(
        broker=BROKER,
        tracing=TracingConfig(
            inject_trace=lambda headers: (headers or []) + [("my-trace-id", b"abc")]
        ),
    )

    kwargs = _send_one(bus, mocker)

    assert kwargs["headers"] == [("my-trace-id", b"abc")]


def test_send_normalises_empty_headers_to_none(mocker: MockerFixture) -> None:
    """An injector that attaches nothing should not send an empty header list."""
    bus = EventBus(
        broker=BROKER, tracing=TracingConfig(inject_trace=lambda _headers: [])
    )

    kwargs = _send_one(bus, mocker)

    assert kwargs["headers"] is None


def test_tracing_leaves_the_message_body_untouched(mocker: MockerFixture) -> None:
    """Trace context must ride in headers only.

    This is what makes the feature safe to deploy and roll back: consumers
    running older code deserialise exactly the same payload.
    """
    untraced = _send_one(EventBus(broker=BROKER), mocker)
    traced = _send_one(
        EventBus(
            broker=BROKER,
            tracing=TracingConfig(
                inject_trace=lambda headers: (headers or []) + [("my-trace-id", b"abc")]
            ),
        ),
        mocker,
    )

    # Turning tracing on changes nothing about the body...
    assert traced["value"] == untraced["value"]
    # ...and the body carries only the event's own fields. Asserted explicitly
    # rather than by comparing the two payloads: a leak would show up in both
    # and the comparison above would still pass.
    assert set(json.loads(traced["value"].decode("utf-8"))) == {"event_id", "first"}


def test_receive_runs_handler_inside_the_span(mocker: MockerFixture) -> None:
    """span_manager should wrap the receiver, and be told what it is wrapping."""
    span_manager, recorded = _make_span_manager()
    bus = EventBus(
        broker=BROKER,
        tracing=TracingConfig(
            extract_trace=lambda _message: {"trace_id": "111"},
            span_manager=span_manager,
        ),
    )
    bus.register_event("first_topic", Foo)

    seen: list[Event] = []

    @bus.receive(event_type=Foo)
    def foo_processor(event: Event) -> None:
        recorded["order"].append("handler")
        seen.append(event)

    consumer, message = _make_mock_consumer({"first": 42})
    mocker.patch("eventbusk.bus.Consumer", return_value=consumer)

    foo_processor()  # pylint: disable=no-value-for-parameter

    # The handler runs between the span opening and closing, so the span
    # actually measures the receiver rather than sitting beside it.
    assert recorded["order"] == ["enter", "handler", "exit"]

    event_fqn, receiver_fqn, trace_ctx = recorded["args"]
    assert event_fqn.endswith("Foo")
    assert receiver_fqn.endswith("foo_processor")
    assert trace_ctx == {"trace_id": "111"}

    assert len(seen) == 1
    consumer.ack.assert_called_once_with(message=message)


def test_receive_spans_messages_that_carry_no_trace_context(
    mocker: MockerFixture,
) -> None:
    """Messages produced before tracing existed must still be traced locally.

    extract_trace returns None for them, but the receiver should still get a
    span, otherwise that work goes missing entirely.
    """
    span_manager, recorded = _make_span_manager()
    bus = EventBus(
        broker=BROKER,
        tracing=TracingConfig(
            extract_trace=lambda _message: None, span_manager=span_manager
        ),
    )
    bus.register_event("first_topic", Foo)

    @bus.receive(event_type=Foo)
    def foo_processor(event: Event) -> None:
        recorded["order"].append("handler")
        recorded["event"] = event

    consumer, message = _make_mock_consumer({"first": 42})
    mocker.patch("eventbusk.bus.Consumer", return_value=consumer)

    foo_processor()  # pylint: disable=no-value-for-parameter

    assert recorded["order"] == ["enter", "handler", "exit"]
    assert recorded["args"][2] is None
    # The message is still delivered intact, not just spanned.
    assert recorded["event"].first == 42
    consumer.ack.assert_called_once_with(message=message)


def test_a_broken_extract_trace_does_not_stop_the_consumer(
    mocker: MockerFixture,
) -> None:
    """A raising extractor must cost the trace, never the message.

    The receive loop's outer try only catches KeyboardInterrupt, so without a
    guard this exception would escape the loop and kill the consumer outright.
    """
    span_manager, recorded = _make_span_manager()

    def extract_trace(_message: object) -> None:
        raise RuntimeError("trace backend is down")

    bus = EventBus(
        broker=BROKER,
        tracing=TracingConfig(extract_trace=extract_trace, span_manager=span_manager),
    )
    bus.register_event("first_topic", Foo)

    @bus.receive(event_type=Foo)
    def foo_processor(event: Event) -> None:
        recorded["order"].append("handler")
        recorded["event"] = event

    consumer, message = _make_mock_consumer({"first": 42})
    mocker.patch("eventbusk.bus.Consumer", return_value=consumer)

    foo_processor()  # pylint: disable=no-value-for-parameter

    # The receiver still ran, inside a span, and the message was acked.
    assert recorded["order"] == ["enter", "handler", "exit"]
    assert recorded["event"].first == 42
    consumer.ack.assert_called_once_with(message=message)
    # Untraced, since that is all the failure should have cost us.
    assert recorded["args"][2] is None


def test_receive_is_handed_the_raw_message_to_extract_from(
    mocker: MockerFixture,
) -> None:
    """extract_trace should receive the broker message, so it can read headers."""
    extract_trace = mocker.Mock(return_value=None)
    bus = EventBus(broker=BROKER, tracing=TracingConfig(extract_trace=extract_trace))
    bus.register_event("first_topic", Foo)

    @bus.receive(event_type=Foo)
    def foo_processor(event: Event) -> None:
        logger.info(event)

    consumer, message = _make_mock_consumer({"first": 42})
    mocker.patch("eventbusk.bus.Consumer", return_value=consumer)

    foo_processor()  # pylint: disable=no-value-for-parameter

    extract_trace.assert_called_once_with(message)
