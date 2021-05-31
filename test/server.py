import asyncio
import argparse
import logging
import time
import pickle
import random
import signal
from aiotask import server
from aiotask.task_manager import TaskManager
from aiotask.executor import ProcessPoolExecutor

logger = logging.getLogger(__name__)


def dummy_executor_fct(args):
    tid, btask = args
    task = pickle.loads(btask)
    # logger.debug("Executing %s - %d", tid, task)
    time.sleep(max(0, random.gauss(0.1, 0.1)))

    return (tid, pickle.dumps(task + 1))


class DummyExecutor(ProcessPoolExecutor):
    task_fct = dummy_executor_fct


async def serve_forever(serv):
    await serv.listen()
    await serv.serve()


def run_server():
    parser = argparse.ArgumentParser("aiotask_server")
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

    listen_options = parser.add_mutually_exclusive_group()
    listen_options.add_argument("-p", "--port", type=int, default=4242)
    listen_options.add_argument("-u", "--unix")

    args = parser.parse_args()

    logging.basicConfig(level=args.loglevel)
    logging.getLogger("aiotask.protocol").setLevel(logging.INFO)
    logging.getLogger("aiotask.server").setLevel(logging.INFO)

    listen_on = (None, args.port) if not args.unix else "unix:" + args.unix

    task_mgr = TaskManager()
    executor = DummyExecutor(task_mgr)
    logger.debug("Starting executor…")
    executor.start()

    serv = server.Server(task_mgr, listen_on)

    loop = asyncio.get_event_loop()
    try:
        logger.debug("Serving.")
        loop.run_until_complete(serve_forever(serv))
        logger.debug("Done serving.")
    except KeyboardInterrupt:
        logger.info("Interrupted, closing server.")
    finally:
        logger.debug("Closing executor…")
        executor.close()
        executor.join()
        logger.debug("Executor finished execution.")
        loop.run_until_complete(serv.close())


if __name__ == "__main__":
    run_server()
