"""
Command Line Interface
"""
from __future__ import annotations

import click
import importlib
from types import ModuleType

from eventbusk import EventBus



def find_app(app: str, attr_name: str="app") -> EventBus:
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
    if ':' in app:
        module_name, attr_name = app.split(":")
    else:
        module_name = app

    module = importlib.import_module(module_name)
    if not isinstance(module, types.ModuleType):
        raise AttributeError(f"Module f{module_name} not found or cannot be imported")

    # Find bus instance within the module
    found = getattr(module, attr_name)
    if not isinstance(found, EventBus):
        raise AttributeError(f"EventBus instance {attr_name} not found in {module_name}")

    return found


@click.group()
def cli():
    """Main entry point."""


@cli.command()
@click.option('--app', help="Path to EventBus instance. eg. 'mymodule:app'")
def worker(app):
    """
    Start consumer workers
    """
    bus = find_app(app)
    click.echo(bus)

    agents = bus.agents
    num_workers = len(agents)
    if len(agents) > 0:
        with concurrent.futures.ProcessPoolExecutor(
            max_workers=num_workers
        ) as executor:
            futures = [executor.submit(agent) for agent in agents]
            for future in concurrent.futures.as_completed(futures):
                future.result()
    else:
        logger.error("No registered agents to run.")


if __name__ == '__main__':
    cli()
