# eventbusk - Event Bus Framework

Keywords: event-bus, distributed, stream, processing, data, queue, kafka, python

## Install

```bash
pip install git+https://github.com/Airbase/eventbusk.git
```

## Quick Start

```python
from eventbusk import Event, EventBus
from dataclasses import dataclass

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
bus.send(foo)
```

## Contributing

Pre-requisites include installing

- [Docker Desktop](https://www.docker.com/products/docker-desktop/)

You can start a Confluent Kafka server locally via docker by following
https://docs.confluent.io/platform/current/platform-quickstart.html

Next setup the project locally as follows

```bash
git clone git@github.com:Airbase/eventbusk.git
cd eventbusk
pre-commit install
source $(poetry env info -p)/bin/activate
poetry install --no-root
pip install --editable .
```

Now you can run the example project consumers. Ensure the topics in the example are created first.

```bash
cd examples
eventbusk worker -A eventbus:bus
```


You can also publish

```bash
python

>>> from eventbus import bus, Fooey
>>> bus.send(Fooey(foo_val="lorem ipsum"))
```
