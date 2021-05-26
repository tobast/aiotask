import asyncio
import argparse
import logging
import uuid
import time
import random
from aiotask.task_manager import TaskManager
from aiotask.executor import ProcessPoolExecutor

logger = logging.getLogger(__name__)


def dummy_executor_fct(args):
    tid, task = args
    logger.debug("Executing %s - %d", tid, task)
    time.sleep(0.1)
    return (tid, task + 1)


class DummyExecutor(ProcessPoolExecutor):
    task_fct = dummy_executor_fct


async def process_tasks(task_mgr, tasks_seq):
    async def upon_finished(tid, payload, task):
        result = await task
        logger.info("[%s] Task completed: %d -> %d", tid, payload, result)

    tasks = {uuid.uuid4(): task for task in tasks_seq}
    aiotasks = []
    for tid, task in tasks.items():
        if random.randint(0, 9) == 0:
            await asyncio.sleep(max(0, random.gauss(0.1, 0.05)))
        aiotasks.append(
            asyncio.create_task(upon_finished(tid, task, task_mgr.run_task(tid, task)))
        )

    await asyncio.wait(
        aiotasks,
        timeout=3,
    )


def main():
    parser = argparse.ArgumentParser("aiotaskmgr")
    parser.add_argument(
        "-v",
        "--verbose",
        action="store_const",
        const=logging.INFO,
        default=logging.WARNING,
        dest="loglevel",
    )
    parser.add_argument(
        "-g", "--debug", action="store_const", const=logging.DEBUG, dest="loglevel"
    )
    parser.add_argument("tasks", nargs="+", type=int, help="Tasks to be submitted.")
    args = parser.parse_args()
    logging.basicConfig(level=args.loglevel)

    task_mgr = TaskManager()
    executor = DummyExecutor(task_mgr)
    logger.debug("Starting executor…")
    executor.start()

    asyncio.run(process_tasks(task_mgr, args.tasks))

    logger.debug("Closing executor…")
    executor.close()
    executor.join()
    logger.debug("Executor finished execution.")


if __name__ == "__main__":
    main()
