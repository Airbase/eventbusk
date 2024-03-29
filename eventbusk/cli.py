"""
Command Line Interface
"""
from __future__ import annotations

import importlib
import logging
import os
import sys
import threading
from collections.abc import Callable, Generator
from contextlib import contextmanager, suppress
from types import ModuleType

import click
import cotyledon  # type: ignore

from .bus import EventBus

logger = logging.getLogger(__name__)


@contextmanager
def cwd_in_path() -> Generator[str | None, None, None]:
    """Context adding the current working directory to sys.path."""
    cwd = os.getcwd()
    if cwd in sys.path:
        yield None
    else:
        sys.path.insert(0, cwd)
        try:
            yield cwd
        finally:
            with suppress(ValueError):
                sys.path.remove(cwd)


def find_app(app: str, attr_name: str = "app") -> EventBus:
    """
    Import an EventBus instance based on a path name.

    Arguments
    ----------
    app: str
        Path to a module containing an EventBus instance.

    Keyword Arguments
    ------------------
    attr_name: str
       Name of the EventBus instance inside the app. Defaults to 'app'.
    """
    if ":" in app:
        module_name, attr_name = app.split(":")
    else:
        module_name = app

    with cwd_in_path():
        module = importlib.import_module(module_name, package=None)

    if not isinstance(module, ModuleType):
        raise AttributeError(f"Module f{module_name} not found or cannot be imported")

    # Find bus instance within the module
    found = getattr(module, attr_name)
    if not isinstance(found, EventBus):
        raise AttributeError(
            f"EventBus instance {attr_name} not found in {module_name}"
        )

    return found


@click.group()
def cli() -> None:
    """Main entry point."""


class Worker(cotyledon.Service):  # type: ignore
    """Process handling an event receiver."""

    def __init__(self, worker_id: int, receiver: Callable[..., None]) -> None:
        super().__init__(worker_id)
        self._shutdown = threading.Event()
        self.receiver = receiver
        self.name = EventBus.to_fqn(receiver)

    def run(self) -> None:
        logger.info(f"{self.name} running.")
        self.receiver()
        self._shutdown.wait()

    def terminate(self) -> None:
        logger.info(f"{self.name} terminating.")
        self._shutdown.set()
        sys.exit(0)

    def reload(self) -> None:
        logger.info(f"{self.name} reloading.")


@cli.command()
@click.option("--app", "-A", help="Path to EventBus instance. eg. 'mymodule:app'")
def worker(app: str) -> None:
    """
    Start consumer workers
    """
    bus = find_app(app)
    receivers = bus.receivers
    if not receivers:
        logger.error("No registered receivers to run.")
        return

    num_workers = len(receivers)
    logger.info(f"Found {num_workers} receivers.")

    manager = cotyledon.ServiceManager()
    with cwd_in_path():
        for receiver in receivers:
            manager.add(Worker, args=(receiver,))
        manager.run()
