from __future__ import annotations

import json
import logging
from dataclasses import dataclass, asdict
from functools import wraps
from typing import Callable, Type, Union
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
    @bus.agent(event_class=MyEvent)
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
        self._agents = set()

    @staticmethod
    def _to_fqn(event_class: type[Event] | Callable) -> str:
        """
        Returns fully qualified name of an event class or an agent, to identify them uniquely.
        """
        return f"{event_class.__module__}.{event_class.__qualname__}"

    def register_event(self, topic: str, event_class: type[Event]):
        """
        Register an event to a bus.

        Each event is only linked to a single topic.
        """
        if self._topic_to_event.get(topic):
            raise AlreadyRegistered(
                f"Event with the same topic has already been registered. [{topic=}]"
            )

        # Create a bidict for 'topic' -> 'mymodule.MyEvent'
        class_fqn = self._to_fqn(event_class)
        self._topic_to_event[topic] = class_fqn
        self._event_to_topic[class_fqn] = topic

    def send(self, event: Event, on_delivery: Callable=None, fail_silently=False):
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
        _agents.add(agent)

    @property
    def agents(self) -> set[Callable]:
        return self._agents

    def agent(self, event_class: type[Event], poll_timeout: int = 1):
        """
        Decorator to convert a function into an agent.

        An agent is a simple function that consumes a specific event on the event bus.
        """

        def _action_decorator(func):
            # TODO: Ensure this does not clash
            consumer_group_name = self._to_fqn(func)

            @wraps(func)
            def wrapper(*args, **kwargs):
                event_fqn = self._to_fqn(event.__class__)
                topic = self._event_to_topic[event_fqn]

                with Consumer(
                    broker=self.broker, topic=topic, group=consumer_group_name
                ) as consumer:
                    while True:
                        try:
                            serialised_event = consumer.poll(poll_timeout)

                            # No message to consume.
                            if serialised_event is None:
                                logger.info(
                                    f"event={event_class.__name__} agent={func.__name__} topic={topic} consumer_group={consumer_group} event is None"
                                )
                                continue

                            # Error consuming.
                            if serialised_event.error():
                                logger.info(
                                    f"event={event_class.__name__} agent={func.__name__} topic={topic} consumer_group={consumer_group} event error {serialised_event.error()}"
                                )
                                continue

                            # Deserialise to the dataclass of the event
                            deserialised_event = event_class(
                                **json.loads(serialised_event.value().decode("utf-8"))
                            )

                            try:
                                logger.info(
                                    f"event={event_class.__name__} agent={func.__name__} topic={topic} consumer_group={consumer_group} event processing data={deserialised_event}"
                                )
                                func(event=deserialised_event)

                                logger.info(
                                    f"event={event_class.__name__} agent={func.__name__} topic={topic} consumer_group={consumer_group} event successfully processed data={deserialised_event}"
                                )
                            except:
                                # else do error handling
                                logger.exception(
                                    f"event={event_class.__name__} agent={func.__name__} topic={topic} consumer_group={consumer_group} event error data={deserialised_event}",
                                    exc_info=True,
                                )

                            # update offset
                            consumer.store_offsets(message=serialised_event)
                            # TODO: This will acknowledge messages which couldn't be processed too
                            # Add some retry mechanism in a later version.

                        except KeyboardInterrupt:
                            logger.info(f"Closing agent agent={func.__name__}")
                            break
                        except:
                            logger.exception(
                                f"event={event_class.__name__} agent={func.__name__} topic={topic} consumer_group={consumer_group} event error",
                                exc_info=True,
                            )

            self._register_agent(wrapper)

            return wrapper

        return _action_decorator
