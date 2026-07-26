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

### Setting up locally

- [uv](https://docs.astral.sh/uv/getting-started/installation/)
- [Docker Desktop](https://www.docker.com/products/docker-desktop/)

Set up the project locally:

```bash
git clone git@github.com:Airbase/eventbusk.git
cd eventbusk
uv sync --extra dev
pre-commit install
```

### Run example project
You will need a Confluent Kafka server locally via Docker by following
https://docs.confluent.io/platform/current/platform-quickstart.html

There's an examples/docker-compose.yml that we can use to run a Kafka broker in a separate terminal window

```bash
cd examples
docker-compose up
```

Now you can run the example project consumers. Ensure the topics in the example are created first.

```bash
# See examples/eventbus.py
uv run eventbusk worker -A eventbus:bus
```

You can also publish a sample message:

```bash
uv run python

>>> from eventbus import bus, Fooey, Barzy
>>> bus.send(Fooey(foo_val="lorem ipsum"))
>>> bus.send(Barzey(bar_val="dolor sit amet"))
```

### Code quality
After making code changes you can run some basic sanity checks as follows.

Run the tests:

```bash
uv run task test
```

Run the linter:

```bash
uv run task ruff
uv run task pylint
```

Format the code:

```bash
uv run task format
```

Run type checks:

```bash
uv run task typecheck
```

You can also choose running pre-commit manually, which runs all all of the above, among other things.

```bash
uv run pre-commit run --all-files
```
