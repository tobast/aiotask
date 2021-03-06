""" Manages the tasks to be run and is queried by the executor """

import asyncio
import logging
import uuid
import typing as ty
from threading import Condition
from aiotask import constants

logger = logging.getLogger(__name__)


class TaskManager:
    class TaskState:
        def __init__(
            self,
            tid: uuid.UUID,
            task,
            future: ty.Optional[asyncio.Future] = None,
            status: constants.TaskStatus = constants.TaskStatus.NOT_SUBMITTED,
        ):
            self.tid = tid
            self.task = task
            self.future: asyncio.Future = (
                future or asyncio.get_running_loop().create_future()
            )
            self.status = status

    class UsedTID(Exception):
        def __init__(self, tid: uuid.UUID):
            super().__init__()
            self.tid = tid

        def __str__(self):
            return "Task ID {} already in use".format(self.tid)

    class TaskCancelled(Exception):
        def __init__(self, tid: uuid.UUID):
            self.tid = tid

        def __str__(self):
            return "Task {} has been cancelled".format(self.tid)

    def __init__(self):
        self.tasks: ty.Dict[uuid.UUID, self.TaskState] = {}
        self.pending: ty.List[uuid.UUID] = []
        self.tasks_lock: Condition = Condition()
        self.tasks_done: ty.Set[uuid.UUID] = set()
        self._event_loop: ty.Optional[asyncio.events.AbstractEventLoop] = None

    def set_event_loop(self, event_loop):
        """Sets the event loop on which this task manager relies. This **must** be
        called if the event loop changes during the execution of the task manager. If a
        task is run before this method is called, the running event loop is picked."""
        self._event_loop = event_loop

    def task_status(self, tid: uuid.UUID) -> constants.TaskStatus:
        if tid in self.tasks_done:
            return constants.TaskStatus.DONE
        with self.tasks_lock:
            if tid not in self.tasks:
                return constants.TaskStatus.NOT_FOUND
            if tid in self.pending:
                return constants.TaskStatus.IN_QUEUE
            return constants.TaskStatus.PROCESSING

    def _add_task(self, tid: uuid.UUID, task: bytes) -> asyncio.Future:
        if tid in self.tasks:
            raise self.UsedTID(tid)
        new_task = self.TaskState(tid, task, status=constants.TaskStatus.IN_QUEUE)
        with self.tasks_lock:
            self.pending.append(tid)
            self.tasks[tid] = new_task
            self.tasks_lock.notify()
        return new_task.future

    async def run_task(self, tid: uuid.UUID, task: bytes):
        if self._event_loop is None:
            self.set_event_loop(asyncio.get_running_loop())
        return await self._add_task(tid, task)

    def cancel_tasks(self, tids: ty.Sequence[uuid.UUID]):
        """Cancel the execution of a list of tasks. If their execution is already
        scheduled, their result will be ignored."""
        with self.tasks_lock:
            for tid in tids:
                try:
                    self.pending.remove(tid)
                except ValueError:
                    pass
                try:
                    self.tasks.pop(tid)
                except KeyError:
                    pass

    def task_complete(self, task_id: uuid.UUID, task_result: bytes):
        """ To be called upon task completion. Thread-safe. """

        def set_future_result(future, result):
            if not future.cancelled():
                future.set_result(result)

        try:
            self.tasks_done.add(task_id)
            task = self.tasks.pop(task_id)
            assert self._event_loop is not None  # `run_task` should occur before
            self._event_loop.call_soon_threadsafe(
                set_future_result, task.future, task_result
            )
        except KeyError:
            pass  # was cancelled

    def iterate_tasks(
        self, timeout: ty.Optional[float] = 0.1, max_iterate: ty.Optional[int] = None
    ):
        """Iterate over tasks, popping them from the pending tasks on the go. Yields
        pairs (task_id, task). Returns when all tasks are consumed. Thread-safe.

        When the tasks are exhausted, wait `timeout` seconds for a new task before
        returning -- use 0 to avoid this behaviour, or None to wait forever.

        Yield at most `max_iterate` values before returning anyway (unless `None`).
        This allows checking eg. whether the thread should continue running."""

        MAX_BUFFER = 128
        iterated = 0

        logger.debug("Iterating on tasks")

        while True:
            to_yield = []
            with self.tasks_lock:
                if not self.pending and not self.tasks_lock.wait(timeout):
                    logger.debug("No pending tasks")
                    break
                logger.debug("Found %d pending tasks", len(self.pending))
                to_yield = self.pending[:MAX_BUFFER]
                self.pending = self.pending[MAX_BUFFER:]
            iterated += len(to_yield)
            for tid in to_yield:
                task = self.tasks.get(tid, None)
                if task is not None:
                    yield (tid, task)
                # else, the task was cancelled.
            if max_iterate is not None and iterated >= max_iterate:
                logger.debug("Stopping iteration")
                break
        logger.debug("Done iterating on tasks")

    def wait_tasks(self, timeout: ty.Optional[float] = None) -> bool:
        """ Wait for the presence of pending tasks. Blocking, thread-safe. """
        with self.tasks_lock:
            if self.pending:
                return True
            if timeout is None:
                while not self.pending:
                    self.tasks_lock.wait()
                return True
            return self.tasks_lock.wait(timeout=timeout)
