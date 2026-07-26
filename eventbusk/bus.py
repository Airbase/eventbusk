"""EventBus implementation."""

from __future__ import annotations

import json
import logging
import time
import uuid
from abc import ABC
from collections.abc import Callable
from dataclasses import asdict, dataclass, field
from functools import wraps

from .brokers import Consumer, DeliveryCallback, Producer
from .exceptions import AlreadyRegistered, ConsumerError, ProducerError, UnknownEvent

logger = logging.getLogger(__name__)


@dataclass
class Event(ABC):
    """Every new event must inherit this class and should be a dataclass.

    Example:
    -------
    @dataclass
    class MyEvent(Event):
        foo: int
        bar: str

    """

    event_id: uuid.UUID = field(default_factory=uuid.uuid4, init=False)


class EventJsonEncoder(json.JSONEncoder):
    """JSON encoder that additionally converts uuid to str."""

    def default(self, o):
        if isinstance(o, uuid.UUID):
            return str(o)
        return json.JSONEncoder.default(self, o)


type EventType = type[Event]
type Receiver = Callable[[Event], None]
type ReceiverWorker = Callable[[], None]
type ReceiverDecorator = Callable[[Receiver], ReceiverWorker]
type ErrorHandler = Callable[[Exception], None]
type ErrorHandlers = dict[type[Exception], ErrorHandler] | None


class EventBus:
    """An EventBus is an a concrete instance of an event bus.

    It is akin to a WSGI Application, or Celery instance.  A project might contain
    multiple instances of the bus connected to different brokers.

    Usage
    -----
    bus = EventBus(broker="kafka://user:pass@localhost:9092")

    @dataclass
    class MyEvent(Event):
        foo: int
        bar: str

    bus.register_event("mytopic", MyEvent)

    # Produce an event
    event = MyEvent(foo=1, bar="baz")
    bus.send(event)

    # Consume an event
    @bus.receive(event_type=MyEvent)
    def process(event):
        ...
    """

    def __init__(
        self,
        broker: str,
        *,
        on_error: ErrorHandlers = None,
    ) -> None:
        self.broker = broker
        # Maps an exception type to a handler that's called only when a receiver
        # raises that exact type (subclasses must be registered separately).
        # Lets callers react to specific, known-recoverable failures (e.g. a
        # stale DB connection) without paying any cost for exceptions they
        # don't care about, and without eventbusk needing to know what those
        # exception types are.
        self._on_error_handlers: dict[type[Exception], ErrorHandler] = on_error or {}
        # Lazily create on first send
        # This is done to avoid issues forking, causing flush to fail.
        # https://github.com/confluentinc/confluent-kafka-python/issues/1122
        # https://github.com/dpkp/kafka-python/issues/1098
        self.producer = None

        # Registries
        # Topic <--> Event type is a 1-1 relation right now, i.e. a topic can only
        # handle a single type of event. So we maintain a bidirectional map underneath
        # using two dictionaries.
        # The dictionaries store a link between topic name and fully qualified name of
        # the event class.
        self._topic_to_event: dict[str, str] = {}
        self._event_to_topic: dict[str, str] = {}
        self._receivers: set[ReceiverWorker] = set()

    @staticmethod
    def to_fqn(event_type: EventType | Receiver) -> str:
        """Returns 'fully qualified name' of an event class or an receiver, to identify
        them uniquely.
        """
        return f"{event_type.__module__}.{event_type.__qualname__}"

    def _find_error_handler(self, exc: Exception) -> ErrorHandler | None:
        """Returns the handler registered for exc's exact type. Subclasses of
        a registered type are not matched -- register them individually if
        needed. Returns None if nothing matches.
        """
        return self._on_error_handlers.get(type(exc))

    def register_event(self, topic: str, event_type: EventType) -> None:
        """Register an event to a bus.

        Each event is only linked to a single topic.
        """
        if self._topic_to_event.get(topic):
            raise AlreadyRegistered(
                f"Event with the topic '{topic}' has already been registered.",
            )

        # Create a bidict for 'topic' -> 'mymodule.MyEvent'
        class_fqn = self.to_fqn(event_type)
        self._topic_to_event[topic] = class_fqn
        self._event_to_topic[class_fqn] = topic

    def send(
        self,
        event: Event,
        *,
        on_delivery: DeliveryCallback = None,
        flush: bool = True,
        fail_silently: bool = False,
    ) -> None:
        """Send an event on the bus."""
        if self.producer is None:
            self.producer = Producer(broker=self.broker)

        event_fqn = self.to_fqn(event.__class__)
        # TODO: Ensure unknown event throws a error.
        topic = self._event_to_topic[event_fqn]
        data = json.dumps(asdict(event), cls=EventJsonEncoder).encode("utf-8")

        try:
            self.producer.produce(
                topic=topic,
                value=data,
                flush=flush,
                on_delivery=on_delivery,
            )
        except ProducerError:
            if fail_silently:
                logger.warning(
                    "Error producing event.",
                    extra={
                        "event": event_fqn,
                        "event_id": event.event_id,
                        "topic": topic,
                    },
                    exc_info=True,
                )
            else:
                raise

    @property
    def receivers(self) -> set[ReceiverWorker]:
        """Returns a set of receivers(consumers) of events."""
        return self._receivers

    # TODO: add group parameter?
    def receive(  # pylint: disable=too-complex
        self,
        event_type: EventType,
        poll_timeout: int = 1,
    ) -> ReceiverDecorator:
        """Decorator to convert a function into an receiver.

        An receiver is a simple function that consumes a specific event on the event
        bus.
        """
        event_fqn = self.to_fqn(event_type)
        if event_fqn not in self._event_to_topic:
            raise UnknownEvent(
                "Register the event to a topic using "
                f"`bus.register_event('foo_topic', {event_type})`",
            )

        def _outer(func: Receiver) -> ReceiverWorker:
            # TODO: Ensure group name does not clash
            group = self.to_fqn(func)
            receiver_fqn = self.to_fqn(func)
            topic = self._event_to_topic[event_fqn]
            log_context = {
                "event": event_fqn,
                "receiver": receiver_fqn,
                "topic": topic,
                "group": group,
            }

            @wraps(func)
            def wrapper() -> None:  # pylint: disable=too-many-branches
                with Consumer(broker=self.broker, topic=topic, group=group) as consumer:
                    # TODO: Max-number-of-tasks
                    while True:
                        try:
                            try:
                                message = consumer.poll(poll_timeout)
                            except ConsumerError:
                                msg = (
                                    "Error while consuming message. "
                                    "Topic might be blocked"
                                )
                                logger.exception(msg, exc_info=True, extra=log_context)
                                self.sleep(seconds=1, message=msg)
                                continue

                            # No message to consume.
                            if message is None:
                                continue

                            # TODO: Remove kafka Message dependency from here.
                            # How do we ack generic messages?
                            # Item "str" of "Union[str, Any, bytes]" has no attribute
                            # "error
                            msg_error = message.error()  # type: ignore
                            if msg_error:
                                msg = (
                                    "Error while consuming message. "
                                    "Topic might be blocked"
                                )
                                logger.warning(
                                    msg,
                                    extra={
                                        **log_context,
                                        "error": msg_error,
                                    },
                                )
                                self.sleep(seconds=1, message=msg)
                                continue

                            # Deserialise to the dataclass of the event
                            # TODO: Remove kafka Message dependency from here.
                            # Item "str" of "Union[str, Any, bytes]" has no attribute
                            # "value
                            msg_value = message.value().decode("utf-8")  # type: ignore
                            event_data = json.loads(msg_value)

                            if "event_id" in event_data:
                                try:
                                    event_id = uuid.UUID(event_data.pop("event_id"))
                                except ValueError:
                                    logger.exception(
                                        ("Error while converting str -> UUID "),
                                        extra={**log_context, "data": event_data},
                                        exc_info=True,
                                    )
                            else:
                                event_id = None

                            # TODO: Fix following
                            # Too many arguments for "Event"  [call-arg]
                            event = event_type(**event_data)  # type: ignore
                            event.event_id = event_id

                            try:
                                func(event)
                                success = True
                            except Exception as exc:  # pylint: disable=broad-except
                                logger.exception(
                                    (
                                        "Error while processing event. "
                                        "topic might be blocked"
                                    ),
                                    extra={**log_context, "data": event},
                                    exc_info=True,
                                )
                                # Only calls a handler when the raised exception's
                                # exact type is registered, so recovery work (e.g.
                                # resetting a stale DB connection) only runs for
                                # the specific failure it's meant for, not for
                                # every exception. The message below is left
                                # unacked, so it will be redelivered and
                                # reprocessed once whatever the handler fixed has
                                # taken effect.
                                handler = self._find_error_handler(exc)
                                if handler is not None:
                                    try:
                                        handler(exc)
                                    except Exception:  # pylint: disable=broad-except
                                        logger.exception(
                                            "Error in on_error handler.",
                                            extra=log_context,
                                            exc_info=True,
                                        )
                                success = False

                            if success:
                                # TODO: Fix following
                                #  Argument "message" to "ack" of "BaseConsumer" has
                                #  incompatible type "Union[str, Any, bytes]"; expected
                                #  "str" [arg-type]
                                logger.info(
                                    "Acknowledging message.",
                                    extra={**log_context, "event_id": event_id},
                                )
                                consumer.ack(message=message)  # type: ignore
                            else:
                                logger.warning(
                                    "Not acknowledging message.",
                                    extra={**log_context, "data": event},
                                )

                        except KeyboardInterrupt:
                            logger.info("Closing receiver.", extra=log_context)
                            break

            # Add to registry
            self._receivers.add(wrapper)
            return wrapper

        return _outer

    @staticmethod
    def sleep(seconds: int = 1, message: str = "") -> None:
        """Helper to sleep and log a custom message."""
        logger.info(f"Sleeping for {seconds}s. {message}")
        time.sleep(seconds)
