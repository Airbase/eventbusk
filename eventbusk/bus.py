from __future__ import annotations

import json
import logging
import time
from dataclasses import asdict, dataclass
from functools import wraps
from typing import Callable, Union
from urllib.parse import urlparse

from confluent_kafka import KafkaError

from .brokers import Consumer, Producer

logger = logging.getLogger(__name__)


class Event:
    """
    Every new event must inherit this class and should be a dataclass.

    Example
    -------
    @dataclass
    class MyEvent(Event):
        foo: int
        bar: str
    """


class EventBusError(Exception):
    pass


class AlreadyRegistered(EventBusError):
    pass


class AgentError(EventBusError):
    pass


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
        self.producer = Producer(broker)

        # Registries
        # Topic <--> Event type is a 1-1 relation right now, i.e. a topic can only
        # handle a single type of event. So we maintain a bidirectional map underneath using
        # two dictionaries. The dictionaries store a link between topic name and fully qualified
        # name of the event class.
        self._topic_to_event: dict[str, str] = {}
        self._event_to_topic: dict[str, str] = {}
        self._agents: set[Callable] = set()

    @staticmethod
    def _to_fqn(event_type: Union[type[Event], Callable]) -> str:
        """
        Returns 'fully qualified name' of an event class or an agent, to identify them uniquely.
        """
        return f"{event_type.__module__}.{event_type.__qualname__}"

    def register_event(self, topic: str, event_type: type[Event]):
        """
        Register an event to a bus.

        Each event is only linked to a single topic.
        """
        if self._topic_to_event.get(topic):
            raise AlreadyRegistered(
                f"Event with the same topic has already been registered. [{topic=}]"
            )

        # Create a bidict for 'topic' -> 'mymodule.MyEvent'
        class_fqn = self._to_fqn(event_type)
        self._topic_to_event[topic] = class_fqn
        self._event_to_topic[class_fqn] = topic

    def send(self, event: Event, on_delivery: Callable = None, fail_silently=False):
        event_fqn = self._to_fqn(event.__class__)
        topic = self._event_to_topic[event_fqn]

        if not on_delivery:

            def default_on_delivery(error, event):
                if error:
                    logger.error(f"Event: {event.value()} delivery failed: {error}")
                else:
                    logger.info(
                        f"Event: {event.value()} delivered to topic:{event.topic()} partition:{event.partition()}"
                    )

            on_delivery = default_on_delivery

        data = json.dumps(asdict(event)).encode("utf-8")
        try:
            self.producer.produce(topic=topic, value=data, on_delivery=on_delivery)
            # TODO: Do we need to flush?
            # self.producer.flush()
        except KafkaError as exc:
            if fail_silently:
                logger.warning(
                    f"Error producing event. [event={event.__class__.__name__} {topic=}]",
                    exc_info=True,
                )
            else:
                raise exc

    def _register_agent(self, agent):
        self._agents.add(agent)

    @property
    def agents(self) -> set[Callable]:
        return self._agents

    def agent(self, event_type: type[Event], poll_timeout: int = 1):
        """
        Decorator to convert a function into an agent.

        An agent is a simple function that consumes a specific event on the event bus.
        """

        def _action_decorator(func):
            # TODO: Ensure this does not clash
            group = self._to_fqn(func)

            @wraps(func)
            def wrapper(*args, **kwargs):
                agent_fqn = self._to_fqn(func)
                event_fqn = self._to_fqn(event.__class__)
                topic = self._event_to_topic[event_fqn]
                log_context = dict(
                    event=event_fqn, agent=agent_fqn, topic=topic, group=group
                )

                with Consumer(broker=self.broker, topic=topic, group=group) as consumer:
                    while True:
                        try:
                            serialized_event = consumer.poll(poll_timeout)

                            # No message to consume.
                            if serialized_event is None:
                                time.sleep(1)
                                continue

                            if serialized_event.error():
                                logger.warning(
                                    f"Error consuming event.",
                                    extra={
                                        **log_context,
                                        **{"error": serialized_event.error()},
                                    },
                                )
                                time.sleep(1)
                                continue

                            # Deserialise to the dataclass of the event
                            event = event_type(
                                **json.loads(serialized_event.value().decode("utf-8"))
                            )

                            try:
                                func(event=event)
                                success = True
                            except Exception as exc:
                                logger.exception(
                                    f"Error consuming event. ",
                                    extra={**log_context, **{"data": serialized_event}},
                                    exc_info=True,
                                )
                                success = False

                            if success:
                                consumer.store_offsets(message=serialised_event)
                            else:
                                time.sleep(1)

                        except KeyboardInterrupt:
                            logger.info(f"Closing agent.", extra=log_context)
                            break

            self._register_agent(wrapper)

            return wrapper

        return _action_decorator
