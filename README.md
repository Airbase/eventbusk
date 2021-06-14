# eventbusk - Event Bus Framework

- Version:
- [Download](https://github.com/Airbase/eventbusk/)
- [Source](https://github.com/Airbase/eventbusk/)
- Keywords: event-bus, distributed, stream, processing, data, queue, kafka, python

## Install

```bash
pip install git+https://github.com/Airbase/eventbusk.git@SM-event-bus
```

## Quick Start

```python
from eventbus import Event, EventBus

# create an app instance of the bus
bus = EventBus(broker="kafka://localhost:9092")

# define an event as a dataclass
@dataclass
class Foo(Event):
    foo: int

# register the event to a single topic
bus.register_event("topic_foo", Foo)

# Define an method that receives that event
@bus.receive(event_type=Foo)
def process_a(event):
    logger.info(f"Foo: {event}")


# Publish an event to the bus
foo = Foo(foo=1)
bus.send(f)
```

## Examples

See `examples/eventbus.py` for a concrete example.
You can run workers for all the receivers

```bash
eventbusk worker -A eventbus:bus
```
