from __future__ import (
        print_function, division, absolute_import, unicode_literals)

from .task_manager import TaskManager


class CeleryTaskManager(TaskManager):
    def enqueue_inner(self, task):
        raise NotImplementedError
