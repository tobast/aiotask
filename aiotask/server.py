import logging
import asyncio
from . import constants
from . import protocol
from .executor import BaseExecutor
from .task_manager import TaskManager

logger = logging.getLogger(__name__)


class Server:
    """ A processing server """

    def __init__(self, task_mgr: TaskManager, listen_on=None):
        self.listen_on = listen_on
        self._server = None
        self.task_mgr = task_mgr
        self._serve_task = None

        if isinstance(self.listen_on, str) and self.listen_on.startswith("unix:"):
            self.is_unix = True
            self.listen_on = self.listen_on[len("unix:") :]

        else:  # assume IPv4/IPv6
            self.is_unix = False

    async def listen(self, start_serving=True):
        loop = asyncio.get_running_loop()
        proto = lambda: ServerProtocol(asyncio.get_running_loop(), self.task_mgr)
        if not self.is_unix:
            host, port = self.listen_on or (None, None)
            self._server = await loop.create_server(
                proto,
                host=host,
                port=port,
                start_serving=start_serving,
            )
        else:
            self._server = await loop.create_unix_server(
                proto,
                path=self.listen_on,
                start_serving=start_serving,
            )

    async def serve(self):
        """ Run the server until cancelled """
        assert self._server is not None
        async with self._server:
            self._serve_task = asyncio.create_task(self._server.serve_forever())
            try:
                logger.debug("Start serving.")
                await self._serve_task
            except asyncio.CancelledError:
                pass
            finally:
                self._serve_task = None
        logger.debug("Done serving.")
        await self._server.wait_closed()
        logger.debug("Server closed.")

    async def close(self):
        assert self._server is not None
        if self._serve_task is not None:
            self._serve_task.cancel()
        else:
            self._server.close()
        await self._server.wait_closed()


class ServerProtocol(protocol.BaseProtocol):
    def __init__(self, event_loop, task_mgr: TaskManager, *args, **kwargs):
        super().__init__(event_loop, *args, **kwargs)
        logger.info("New server spawned")
        self.task_mgr = task_mgr
        self._tasks_running = {}

    async def _process_enqueue(self, tlv: protocol.TlvValue):
        tid, payload = tlv.split_uuid()
        task_coro = asyncio.create_task(self.task_mgr.run_task(tid, payload))
        logger.debug("[TASK %s] Enqueuing", tid)
        self._tasks_running[tid] = task_coro
        task_result = await task_coro
        logger.debug("[TASK %s] Done", tid)
        result_payload = tid.bytes + task_result
        out_tlv = protocol.TlvValue(constants.TlvType.DONE, result_payload)
        self.emit_tlv(out_tlv)

    def _process_status_query(self, tlv: protocol.TlvValue):
        tid, _ = tlv.split_uuid()
        status = self.task_mgr.task_status(tid)
        result_payload = tid.bytes + bytes([int(status)])
        out_tlv = protocol.TlvValue(constants.TlvType.UPDATE, result_payload)
        self.emit_tlv(out_tlv)

    def tlv_received(self, tlv: protocol.TlvValue):
        if tlv.typ == constants.TlvType.ENQUEUE:
            self._event_loop.create_task(self._process_enqueue(tlv))
        elif tlv.typ == constants.TlvType.STATUS:
            self._process_status_query(tlv)
        else:
            logger.warning("Received invalid TLV of type %d. Ignoring.", tlv.typ)

    def connection_lost(self, exn):
        super().connection_lost(exn)
        # Cancel all running tasks
        for task in self._tasks_running.values():
            task.cancel()
        self.task_mgr.cancel_tasks(self._tasks_running.keys())
