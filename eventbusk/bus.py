from __future__ import annotations
from abc import ABC

import concurrent.futures
import json
import logging
import time
from dataclasses import asdict, dataclass
from functools import wraps
from typing import Callable, Union, Type
from urllib.parse import urlparse

from confluent_kafka import KafkaError

from .brokers import Consumer, Producer, DeliveryCallBackT
from .exceptions import AgentError, AlreadyRegistered, UnRegisteredEvent

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
AgentT = Callable[[Event], None]
AgentWrappedT = Callable[[], None]


class EventBus:
    """
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
    @bus.agent(event_type=MyEvent)
    def process(event):
        ...
    """

    def __init__(self, broker: str):
        self.broker = broker
        # TODO: Lazy create on first send
        self.producer = Producer(broker)

        # Registries
        # Topic <--> Event type is a 1-1 relation right now, i.e. a topic can only
        # handle a single type of event. So we maintain a bidirectional map underneath using
        # two dictionaries. The dictionaries store a link between topic name and fully qualified
        # name of the event class.
        self._topic_to_event: dict[str, str] = {}
        self._event_to_topic: dict[str, str] = {}
        self._agents: set[AgentWrappedT] = set()

    @staticmethod
    def _to_fqn(event_type: Union[EventT, AgentT]) -> str:
        """
        Returns 'fully qualified name' of an event class or an agent, to identify them uniquely.
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
        class_fqn = self._to_fqn(event_type)
        self._topic_to_event[topic] = class_fqn
        self._event_to_topic[class_fqn] = topic

    def send(self, event: Event, on_delivery: DeliveryCallBackT = None, flush: bool=True, fail_silently: bool=False) -> None:
        event_fqn = self._to_fqn(event.__class__)
        topic = self._event_to_topic[event_fqn]

        data = json.dumps(asdict(event)).encode("utf-8")
        try:
            self.producer.produce(topic=topic, value=data, on_delivery=on_delivery)
            if flush:
                self.producer.flush()
        except KafkaError as exc:
            if fail_silently:
                logger.warning(
                    f"Error producing event.",
                    extra={"event": event_fqn, "topic": topic},
                    exc_info=True,
                )
            else:
                raise exc

    @property
    def agents(self) -> set[AgentWrappedT]:
        return self._agents

    # TODO: add group parameter?
    def agent(self, event_type: EventT, poll_timeout: int = 1):
        """
        Decorator to convert a function into an agent.

        An agent is a simple function that consumes a specific event on the event bus.
        """
        event_fqn = self._to_fqn(event_type)
        if event_fqn not in self._event_to_topic.keys():
            raise UnRegisteredEvent(
                f"Register the event to a topic using `bus.register_event('foo_topic', {event_type})`"
            )

        def _outer(func: AgentT) -> AgentWrappedT:
            # TODO: Ensure group name does not clash
            group = self._to_fqn(func)
            agent_fqn = self._to_fqn(func)
            topic = self._event_to_topic[event_fqn]
            log_context = dict(
                event=event_fqn, agent=agent_fqn, topic=topic, group=group
            )

            @wraps(func)
            def wrapper() -> None:
                with Consumer(broker=self.broker, topic=topic, group=group) as consumer:
                    # TODO: Max-number-of-tasks
                    while True:
                        try:
                            serialized_event = consumer.poll(poll_timeout)

                            # No message to consume.
                            if serialized_event is None:
                                continue

                            if serialized_event.error():
                                logger.warning(
                                    f"Error consuming event.",
                                    extra={
                                        **log_context,
                                        **{"error": serialized_event.error()},
                                    },
                                )
                                self.sleep(1, "Error on last consumption, topic might be blocked.")
                                continue

                            # Deserialise to the dataclass of the event
                            event_data = json.loads(serialized_event.value().decode("utf-8"))
                            event = event_type(**event_data)

                            try:
                                func(event)
                                success = True
                            except Exception as exc:
                                logger.exception(
                                    f"Error while processing event. ",
                                    extra={**log_context, **{"data": serialized_event}},
                                    exc_info=True,
                                )
                                success = False

                            if success:
                                consumer.store_offsets(message=serialized_event)

                        except KeyboardInterrupt:
                            logger.info(f"Closing agent.", extra=log_context)
                            break

            # Add to registry
            self._agents.add(wrapper)

            return wrapper

        return _outer

    def sleep(self, seconds:int=1, message: str="") -> None:
        logger.info(f"Sleeping for {seconds}s. {message}")
        time.sleep(seconds)
