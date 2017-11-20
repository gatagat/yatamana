from .chunk_of_tasks_task import ChunkOfTasksTask
from .local_task_manager import LocalTaskManager
from .sge_task_manager import SgeTaskManager
from .slurm_task_manager import SlurmTaskManager
from .task import Task, FileExistsFinishedMixin
from .task_manager import TaskManager
from .task_manager_factory import get_task_manager

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


class CeleryTaskManager(TaskManager):
    def enqueue_inner(self, task):
        raise NotImplementedError
