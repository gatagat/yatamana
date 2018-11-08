"""
yatamana
--------

Task manager simplifying submitting tasks to (HPC) clusters in a
cluster-agnostic way.
"""

from __future__ import (
        print_function, division, absolute_import, unicode_literals)

__all__ = [
        'CeleryTaskManager',
        'ChunkOfTasksTask',
        'LocalTaskManager',
        'SgeTaskManager',
        'SlurmTaskManager',
        'Task',
        'TaskManager',
        'FileExistsFinishedMixin',
        'get_task_manager'
        ]

from .task import Task, FileExistsFinishedMixin
from .chunk_of_tasks_task import ChunkOfTasksTask
from .task_manager import TaskManager
from .task_manager_factory import get_task_manager
from .celery_task_manager import CeleryTaskManager
from .sge_task_manager import SgeTaskManager
from .slurm_task_manager import SlurmTaskManager
from .local_task_manager import LocalTaskManager
