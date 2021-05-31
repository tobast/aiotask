import asyncio
import argparse
import logging
import pickle
from aiotask import client

logger = logging.getLogger(__name__)


async def run_client_tasks(cli):
    async def run_remote_task(task):
        task_res = asyncio.create_task(task.run())
        while True:
            try:
                res = pickle.loads(
                    await asyncio.wait_for(asyncio.shield(task_res), timeout=1)
                )
                logger.info(
                    "[%s] Task %d finished: %d",
                    task.task_id,
                    pickle.loads(task.payload),
                    res,
                )
                return
            except asyncio.TimeoutError:
                try:
                    status = await asyncio.wait_for(task.query_status(), timeout=1)
                    logger.debug(
                        "[%s] Task %d is %s",
                        task.task_id,
                        pickle.loads(task.payload),
                        status,
                    )
                except asyncio.TimeoutError:
                    logger.error(
                        "[%s] Task %d did not get status update (last known: %s)",
                        task.task_id,
                        pickle.loads(task.payload),
                        task.status,
                    )

    await cli.connect()
    run_task = asyncio.create_task(cli.run())

    srv_tasks = [client.RemoteTask(pickle.dumps(val), cli) for val in range(1000)]

    run_srv_tasks = [asyncio.create_task(run_remote_task(task)) for task in srv_tasks]

    await asyncio.wait(run_srv_tasks)
    await cli.close()

    await run_task


def run_client():
    parser = argparse.ArgumentParser("aiotask_client")
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
    listen_ip = listen_options.add_argument_group()
    listen_ip.add_argument("-d", "--host", type=str, default="::1")
    listen_ip.add_argument("-p", "--port", type=int, default=4242)
    listen_options.add_argument("-u", "--unix")

    args = parser.parse_args()

    logging.basicConfig(level=args.loglevel)
    logging.getLogger("aiotask.protocol").setLevel(logging.INFO)

    connect_to = (args.host, args.port) if not args.unix else "unix:" + args.unix
    cli = client.Client(connect_to)

    loop = asyncio.get_event_loop()
    try:
        loop.run_until_complete(run_client_tasks(cli))
    except KeyboardInterrupt:
        loop.run_until_complete(cli.close())
    finally:
        loop.close()


if __name__ == "__main__":
    run_client()
