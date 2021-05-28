import logging
from threading import Thread
import itertools
import multiprocessing as mp
from . import task_manager

logger = logging.getLogger(__name__)


class BaseExecutor(Thread):
    """ A base class for executors -- class that actually runs the tasks. """

    """ The function to be called on task values. This function is called with two
    arguments: the task id, and the task value. It must return a tuple of
    `(task_id, task_result)`. """
    task_fct = None

    def __init__(self, task_mgr: task_manager.TaskManager):
        super().__init__()
        self.task_mgr = task_mgr
        self._run = True

    def close(self):
        """Closes the executor after its current tasks are processed. Non-blocking.
        The thread will exit after it has finished its tasks."""
        self._run = False

    def __del__(self):
        self.close()

    def _close(self):
        """ Clean-up of the environment before exiting the thread """
        pass

    def run(self):
        while self._run:
            if self.task_mgr.wait_tasks(timeout=1):
                self.run_tasks()

        self._close()

    def run_tasks(self):
        """ Actually runs all the available tasks at this moment. """
        pass


def dummy(args):
    tid, task = args
    return (tid, task + 1)


class ProcessPoolExecutor(BaseExecutor):
    pool_class = mp.Pool

    def __init__(
        self, task_mgr: task_manager.TaskManager, pool_args=None, pool_kwargs=None
    ):
        super().__init__(task_mgr)
        self.pool = self.__class__.pool_class(*(pool_args or []), **(pool_kwargs or {}))

    def close_pool(self):
        if self.pool is not None:
            self.pool.close()
            self.pool.join()
            self.pool = None

    def _close(self):
        super()._close()
        self.close_pool()

    def _tasks_iterator(self):
        def tid_to_args(tid, task):
            return (tid, task.task)

        return itertools.starmap(
            tid_to_args, self.task_mgr.iterate_tasks(max_iterate=1024)
        )

    def run_tasks(self):
        assert self.pool is not None

        for task_id, task_result in self.pool.imap_unordered(
            self.__class__.task_fct,
            self._tasks_iterator(),
        ):
            self.task_mgr.task_complete(task_id, task_result)
