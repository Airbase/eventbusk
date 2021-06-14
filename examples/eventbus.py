"""
Run workers for the receivers via

$>eventbusk worker -A eventbus:bus

To send an event on the bus you can open a separate python shell

>>> from eventbus import bus, Foo
>>> foo=Foo(foo=1)
>>> bus.send(foo)
"""
import logging
from dataclasses import dataclass

from eventbusk import Event, EventBus

logger = logging.getLogger(__name__)
bus = EventBus(broker="kafka://localhost:9092")


@dataclass
class Fooey(Event):
    """
    First type of event
    """

    foo_val: int


@dataclass
class Barzy(Event):
    """
    Second type of event
    """

    bar_val: str


bus.register_event("topic_foo", Fooey)
bus.register_event("topic_bar", Barzy)


# Consume an event
@bus.receive(event_type=Fooey)
def process_a(event: Event) -> None:
    """
    Consumer of Foeey events
    """
    logger.info(f"Foo: {event}")


@bus.receive(event_type=Barzy)
def process_b(event: Event) -> None:
    """
    Consumer of Barzy events
    """
    logger.info(f"Bar: {event}")
