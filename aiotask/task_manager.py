""" Manages the tasks to be run and is queried by the executor """

import asyncio
import logging
import uuid
from typing import Dict, List, Optional
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

    def __init__(self):
        self.tasks: Dict[uuid.UUID, self.TaskState] = {}
        self.pending: List[uuid.UUID] = []
        self.tasks_lock = Condition()
        self._event_loop = None

    def set_event_loop(self, event_loop):
        """Sets the event loop on which this task manager relies. This **must** be
        called if the event loop changes during the execution of the task manager. If a
        task is run before this method is called, the running event loop is picked."""
        self._event_loop = event_loop

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

    def task_complete(self, task_id, task_result):
        """ To be called upon task completion. Thread-safe. """
        task = self.tasks[task_id]
        self._event_loop.call_soon_threadsafe(task.future.set_result, task_result)
        del task

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
                logger.debug("Waiting for pending tasks in iteration")
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
        logger.debug("Waiting for pending tasks in iteration")
        with self.tasks_lock:
            if timeout is None:
                while not self.pending:
                    return self.tasks_lock.wait()
            else:
                return self.tasks_lock.wait(timeout=timeout)
