"""
CLI Interface
"""

import click


@click.group()
def cli():
    """Main entry point."""


@cli.command()
@click.option('--app', help="Path to EventBus instance. eg. 'myapp:bus'")
def worker(app):
    """
    Start consumer workers
    """
    click.echo(app)
    #
    agents = get_agents()

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
