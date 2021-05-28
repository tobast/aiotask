""" Manages the tasks to be run and is queried by the executor """

import asyncio
import logging
import uuid
from typing import Dict, List, Optional, Sequence, Set
from threading import Condition
from aiotask import constants

logger = logging.getLogger(__name__)


class TaskManager:
    class TaskState:
        def __init__(
            self,
            tid: uuid.UUID,
            task,
            future: Optional[asyncio.Future] = None,
            status: constants.TaskStatus = 0,
        ):
            self.tid = tid
            self.task = task
            self.future = future or asyncio.get_running_loop().create_future()
            self.status = status

    class UsedTID(Exception):
        def __init__(self, tid: uuid.UUID):
            super().__init__()
            self.tid = tid

        def __str__(self):
            return "Task ID {} already in use".format(self.tid)

    class TaskCancelled(Exception):
        def __init__(self, tid):
            self.tid = tid

        def __str__(self):
            return "Task {} has been cancelled".format(self.tid)

    def __init__(self):
        self.tasks: Dict[uuid.UUID, self.TaskState] = {}
        self.pending: List[uuid.UUID] = []
        self.tasks_lock = Condition()
        self.tasks_done: Set[uuid.UUID] = set()
        self._event_loop = None

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

    def _add_task(self, tid: uuid.UUID, task) -> asyncio.Future:
        if tid in self.tasks:
            raise self.UsedTID(tid)
        new_task = self.TaskState(tid, task, status=constants.TaskStatus.IN_QUEUE)
        with self.tasks_lock:
            self.pending.append(tid)
            self.tasks[tid] = new_task
            self.tasks_lock.notify()
        return new_task.future

    async def run_task(self, tid: uuid.UUID, task):
        if self._event_loop is None:
            self.set_event_loop(asyncio.get_running_loop())
        return await self._add_task(tid, task)

    def cancel_tasks(self, tids: Sequence[uuid.UUID]):
        """Cancel the execution of a list of tasks. If their execution is already
        scheduled, their result will be ignored."""
        with self.tasks_lock:
            for tid in tids:
                try:
                    self.pending.remove(tid)
                    self.tasks.pop(tid)
                except ValueError:
                    pass

    def task_complete(self, task_id: uuid.UUID, task_result):
        """ To be called upon task completion. Thread-safe. """
        try:
            self.tasks_done.add(task_id)
            task = self.tasks.pop(task_id)
            self._event_loop.call_soon_threadsafe(task.future.set_result, task_result)
        except KeyError:
            self._event_loop.call_soon_threadsafe(
                task.future.set_exception, self.__class__.TaskCancelled(task_id)
            )

    def iterate_tasks(self, timeout=0.1, max_iterate=None):
        """Iterate over tasks, popping them from the pending tasks on the go. Returns
        when all tasks are consumed. Thread-safe.

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
            for elt in to_yield:
                yield elt
            if max_iterate is not None and iterated >= max_iterate:
                logger.debug("Stopping iteration")
                break
        logger.debug("Done iterating on tasks")

    def wait_tasks(self, timeout=None) -> bool:
        """ Wait for the presence of pending tasks. Blocking, thread-safe. """
        with self.tasks_lock:
            if timeout is None:
                while not self.pending:
                    return self.tasks_lock.wait()
            else:
                return self.tasks_lock.wait(timeout=timeout)
