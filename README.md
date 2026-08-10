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

## Tracing (optional)

A producer and a consumer are different processes, so by default nothing tells
you which `bus.send()` caused which receiver to run.

eventbusk can carry that link for you, but it does not know about Datadog,
OpenTelemetry, Jaeger or any other tool. You hand it up to three functions and
it calls them at the right moments. If you pass nothing, tracing is off and
nothing changes.

```python
from contextlib import contextmanager

from eventbusk import EventBus, TracingConfig


def inject_trace(headers):
    """Called on send. Return the headers to put on the outgoing message."""
    return (headers or []) + [("my-trace-id", b"abc123")]


def extract_trace(message):
    """Called on receive. Read the headers back off the incoming message.

    Return anything you like (or None) - it is passed straight to span_manager.
    """
    for key, value in message.headers() or []:
        if key == "my-trace-id":
            return {"trace_id": value.decode("utf-8")}
    return None


@contextmanager
def span_manager(event_fqn, receiver_fqn, trace_ctx):
    """Called on receive. Wraps the receiver so you can time/record it."""
    with my_tracer.start_span(receiver_fqn) as span:
        if trace_ctx:
            span.set_tag("trace_id", trace_ctx["trace_id"])
        yield span


bus = EventBus(
    broker="kafka://localhost:9092",
    tracing=TracingConfig(
        inject_trace=inject_trace,
        extract_trace=extract_trace,
        span_manager=span_manager,
    ),
)
```

Things worth knowing:

- **The message body is untouched.** Trace data travels in Kafka message
  headers, so the JSON your receivers deserialise is exactly the same with
  tracing on or off. You can turn this on (or roll it back) without worrying
  about messages already sitting in a topic.
- **Old messages are fine.** If a message arrives with no trace headers,
  `extract_trace` just returns `None` and `span_manager` is still called with
  `trace_ctx=None`, so receiver work stays visible either way.
- **Your callbacks should not raise.** Wrap the body of each one in
  `try`/`except` and fall back to doing nothing - losing a trace is always
  better than losing the message. If one raises anyway: `inject_trace` fails
  the `send()`; `extract_trace` is guarded, so eventbusk logs it and processes
  the event untraced; `span_manager` counts as a receiver failure, so the
  message is never acked and will be redelivered indefinitely.

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
uv run task lint
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
