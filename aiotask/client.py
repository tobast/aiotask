import logging
import asyncio
import uuid
from . import constants
from . import protocol

logger = logging.getLogger(__name__)


class RemoteTask:
    """ A task to be run on a remote server """

    def __init__(self, payload, connection: "Client", loop=None):
        self.payload = payload
        self.connection = connection
        self.event_loop = loop or asyncio.get_running_loop()
        self.task_id = uuid.uuid4()
        self._result_available = self.event_loop.create_future()
        self._status_update_future = None
        self.status = constants.TaskStatus.NOT_SUBMITTED

        self.connection.protocol.register_task(self)

    async def run(self):
        """ Run the task on the remote server. Return its result. """
        self.connection.protocol.run_task(self)
        self.status = constants.TaskStatus.IN_QUEUE
        return await self._result_available

    async def query_status(self):
        """ Ask the remote end for task status. """
        if self.status in [
            constants.TaskStatus.NOT_SUBMITTED,
            constants.TaskStatus.DONE,
        ]:
            return self.status
        if self._status_update_future is not None:
            return await self._status_update_future
        self._status_update_future = self.event_loop.create_future()
        self.connection.protocol.query_task_status(self)
        status = await self._status_update_future
        self._status_update_future = None
        return status


class Client:
    """ A client connection to a processing server """

    def __init__(self, remote_addr):
        self.remote_addr = remote_addr
        self._transport = None
        self._protocol = None
        self._done_running = None

        if isinstance(self.remote_addr, str) and self.remote_addr.startswith("unix:"):
            self.is_unix = True
            self.remote_addr = self.remote_addr[len("unix:") :]

        else:  # assume IPv4/IPv6
            self.is_unix = False

    async def connect(self):
        loop = asyncio.get_running_loop()
        self._done_running = loop.create_future()
        protocol_factory = lambda: ClientProtocol(
            asyncio.get_running_loop(), self._done_running
        )
        if not self.is_unix:
            host, port = self.remote_addr
            self._transport, self._protocol = await loop.create_connection(
                protocol_factory,
                host=host,
                port=port,
            )
            conn_str = "{}:{}".format(host, port)
        else:
            self._transport, self._protocol = await loop.create_unix_connection(
                protocol_factory,
                path=self.remote_addr,
            )
            conn_str = "unix:{}".format(self.remote_addr)
        await self._protocol.await_connection_made()
        logger.debug("Connected to %s", conn_str)

    async def run(self):
        """ Returns when connection to the server is closed """
        assert self._done_running is not None
        await self._done_running

    @property
    def protocol(self):
        return self._protocol

    async def close(self):
        assert self._done_running is not None
        self._transport.close()
        await self.run()


class ClientProtocol(protocol.BaseProtocol):
    def __init__(self, event_loop, done_running, *args, **kwargs):
        super().__init__(event_loop, *args, **kwargs)
        self._done_running = done_running
        self.tasks = {}
        self.connected = False
        self._connected_future = self._event_loop.create_future()

    def register_task(self, task: RemoteTask):
        self.tasks[task.task_id] = task

    def run_task(self, task: RemoteTask):
        """ Send a task to be run to the server """
        self.register_task(task)
        tlv = protocol.TlvValue(
            constants.TlvType.ENQUEUE, task.task_id.bytes + task.payload
        )
        self.emit_tlv(tlv)

    def query_task_status(self, task: RemoteTask):
        """ Ask the server for an update on the task status """
        assert task._status_update_future is not None
        tlv = protocol.TlvValue(constants.TlvType.STATUS, task.task_id.bytes)
        self.emit_tlv(tlv)

    def connection_made(self, transport):
        super().connection_made(transport)
        self._connected_future.set_result(None)
        self.connected = True

    async def await_connection_made(self):
        """ Yields when the connection was made. """
        if self.connected:
            return
        await self._connected_future

    def connection_lost(self, exn):
        super().connection_lost(exn)
        self._done_running.set_result(True)

    def tlv_received(self, tlv: protocol.TlvValue):
        if tlv.typ == constants.TlvType.UPDATE:
            tid, status = tlv.split_uuid()
            task = self.tasks.get(tid, None)
            if task is None:
                logger.warning("Received an update for unknown task %s", tid)
                return  # ignore

            try:
                status_int = int.from_bytes(status, "big")
                status = constants.TaskStatus(status_int)
            except ValueError:
                logger.warning(
                    "Received an update for task %s with bad status %d", tid, status_int
                )
                return  # ignore
            task.status = status
            future = task._status_update_future
            if future is not None:
                future.set_result(status)

        if tlv.typ == constants.TlvType.DONE:
            tid, result = tlv.split_uuid()
            task = self.tasks.get(tid, None)
            if task is None:
                logger.warning("Received a result for unknown task %s", tid)
                return  # ignore
            task.status = constants.TaskStatus.DONE
            task._result_available.set_result(result)
            del self.tasks[tid]
