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
class Foo(Event):
    foo: int


@dataclass
class Bar(Event):
    bar: str


bus.register_event("topic_foo", Foo)
bus.register_event("topic_bar", Bar)

# Consume an event
@bus.receive(event_type=Foo)
def process_a(event: Event) -> None:
    logger.info(f"Foo: {event}")


@bus.receive(event_type=Bar)
def process_b(event: Event) -> None:
    logger.info(f"Bar: {event}")
