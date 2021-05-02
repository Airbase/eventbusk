import concurrent.futures
import logging

import django

django.setup()

from .events import get_agents

logger = logging.getLogger(__name__)


if __name__ == "__main__":
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
