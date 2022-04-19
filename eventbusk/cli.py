"""
Command Line Interface
"""
from __future__ import annotations

import importlib
import logging
import os
import sys
import threading
from contextlib import contextmanager, suppress
from types import ModuleType
from typing import Callable, Generator, Optional

import click
import cotyledon  # type: ignore

from .bus import EventBus

logger = logging.getLogger(__name__)


@contextmanager
def cwd_in_path() -> Generator[Optional[str], None, None]:
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
@click.option("--worker-number", "-w", help="Worker instance number")
@click.option("--total-workers", "-n", help="Total number of worker nodes")
def worker(app: str, worker_number: int = 1, total_workers: int = 1) -> None:
    """
    Start consumer workers
    """
    bus = find_app(app)
    # Sort by name to distribute receivers evenly across worker instances
    receivers: list[Callable] = sorted(
        list(bus.receivers),
        key=lambda receiver_fn: receiver_fn.__name__
    )
    if not receivers:
        logger.error("No registered receivers to run.")
        return

    if worker_number < 0:
        logger.error("Worker instance number cannot be less than 1")
        return
    if worker_number > total_workers:
        logger.error("Worker instance number cannot be > total workers")

    num_workers = len(receivers)
    logger.info(f"Found {num_workers} receivers.")

    # Every n-th receiver (n = total_workers) starting from `worker_number`
    # will be run on current worker instance
    worker_receivers = [
        receivers[i-1]
        for i in range(worker_number, total_workers + 1, total_workers)
    ]

    manager = cotyledon.ServiceManager()
    with cwd_in_path():
        for receiver in worker_receivers:
            manager.add(Worker, args=(receiver,))
        manager.run()
