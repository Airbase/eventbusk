import logging
from dataclasses import dataclass

import daiquiri

daiquiri.setup(level=logging.INFO)

logger = daiquiri.getLogger(__name__)


from eventbusk import Event, EventBus

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
@bus.agent(event_type=Foo)
def process_a(event):
    logger.info(f"Foo: {event}")


@bus.agent(event_type=Bar)
def process_b(event):
    logger.info(f"Bar: {event}")
