"""
EventBus implementation
"""
from __future__ import annotations

import json
import logging
import time
from abc import ABC
from dataclasses import asdict, dataclass
from functools import wraps
from typing import Callable, Type, Union

from confluent_kafka import KafkaError # type: ignore

from .brokers import Consumer, DeliveryCallBackT, Producer
from .exceptions import AlreadyRegistered, ConsumerError, UnknownEvent

logger = logging.getLogger(__name__)


@dataclass
class Event(ABC):
    """
    Every new event must inherit this class and should be a dataclass.

    Example
    -------
    @dataclass
    class MyEvent(Event):
        foo: int
        bar: str
    """


EventT = Type[Event]
ReceiverT = Callable[[Event], None]
ReceiverWrappedT = Callable[[], None]
ReceivedOuterT = Callable[[ReceiverT], ReceiverWrappedT]


class EventBus:
    """
    An EventBus is an a concrete instance of an event bus.

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

    def __init__(self, broker: str):
        self.broker = broker
        # TODO: Make Lazy create on first send?
        self.producer = Producer(broker)

        # Registries
        # Topic <--> Event type is a 1-1 relation right now, i.e. a topic can only
        # handle a single type of event. So we maintain a bidirectional map underneath
        # using two dictionaries.
        # The dictionaries store a link between topic name and fully qualified name of
        # the event class.
        self._topic_to_event: dict[str, str] = {}
        self._event_to_topic: dict[str, str] = {}
        self._receivers: set[ReceiverWrappedT] = set()

    @staticmethod
    def to_fqn(event_type: Union[EventT, ReceiverT]) -> str:
        """
        Returns 'fully qualified name' of an event class or an receiver, to identify
        them uniquely.
        """
        return f"{event_type.__module__}.{event_type.__qualname__}"

    def register_event(self, topic: str, event_type: EventT) -> None:
        """
        Register an event to a bus.

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
        on_delivery: DeliveryCallBackT = None,
        flush: bool = True,
        fail_silently: bool = False,
    ) -> None:
        """
        Send an event on the bus.
        """

        event_fqn = self.to_fqn(event.__class__)
        # TODO: Ensure unknown event throws a error.
        topic = self._event_to_topic[event_fqn]

        data = json.dumps(asdict(event)).encode("utf-8")
        try:
            self.producer.produce(
                topic=topic, value=data, flush=flush, on_delivery=on_delivery
            )
        # TODO: Replace with ProducerError
        except KafkaError as exc:
            if fail_silently:
                logger.warning(
                    "Error producing event.",
                    extra={"event": event_fqn, "topic": topic},
                    exc_info=True,
                )
            else:
                raise exc

    @property
    def receivers(self) -> set[ReceiverWrappedT]:
        """
        Returns a set of receivers(consumers) of events.
        """
        return self._receivers

    # TODO: add group parameter?
    def receive(self, event_type: EventT, poll_timeout: int = 1) -> ReceivedOuterT:
        """
        Decorator to convert a function into an receiver.

        An receiver is a simple function that consumes a specific event on the event
        bus.
        """
        event_fqn = self.to_fqn(event_type)
        if event_fqn not in self._event_to_topic.keys():
            raise UnknownEvent(
                "Register the event to a topic using "
                f"`bus.register_event('foo_topic', {event_type})`"
            )

        def _outer(func: ReceiverT) -> ReceiverWrappedT:
            # TODO: Ensure group name does not clash
            group = self.to_fqn(func)
            receiver_fqn = self.to_fqn(func)
            topic = self._event_to_topic[event_fqn]
            log_context = dict(
                event=event_fqn, receiver=receiver_fqn, topic=topic, group=group
            )

            @wraps(func)
            def wrapper() -> None:
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
                            #  Item "str" of "Union[str, Any, bytes]" has no attribute "error
                            msg_error =  message.error()  # type: ignore
                            if msg_error:
                                msg = (
                                    "Error while consuming message. "
                                    "Topic might be blocked"
                                )
                                logger.warning(
                                    msg,
                                    extra={
                                        **log_context,
                                        **{"error": msg_error},
                                    },
                                )
                                self.sleep(seconds=1, message=msg)
                                continue

                            # Deserialise to the dataclass of the event
                            # TODO: Remove kafka Message dependency from here.
                            #  Item "str" of "Union[str, Any, bytes]" has no attribute "value
                            event_data = json.loads(message.value().decode("utf-8")) # type: ignore

                            # TODO: Fix following
                            # Too many arguments for "Event"  [call-arg]
                            event = event_type(**event_data)  # type: ignore

                            try:
                                func(event)
                                success = True
                            except Exception:  # pylint: disable=broad-except
                                logger.exception(
                                    (
                                        "Error while processing event. "
                                        "topic might be blocked"
                                    ),
                                    extra={**log_context, **{"data": event}},
                                    exc_info=True,
                                )
                                success = False

                            if success:
                                # TODO: Fix following
                                #  Argument "message" to "ack" of "BaseConsumer" has incompatible type "Union[str, Any, bytes]"; expected "str"  [arg-type]
                                consumer.ack(message=message)  # type: ignore

                        except KeyboardInterrupt:
                            logger.info("Closing receiver.", extra=log_context)
                            break

            # Add to registry
            self._receivers.add(wrapper)
            return wrapper

        return _outer

    @staticmethod
    def sleep(seconds: int = 1, message: str = "") -> None:
        """
        Helper to sleep and log a custom message
        """
        logger.info(f"Sleeping for {seconds}s. {message}")
        time.sleep(seconds)
