from .task import TaskTask
from .task_manager import TaskManager
from .sge_task_manager import SgeTaskManager
from .slurm_task_manager import SlurmTaskManager

__all__ = ['Task', 'TaskManager', 'SgeTaskManager', 'SlurmTaskManager',
           'CeleryTaskManager']


class CeleryTaskManager(TaskManager):
    def enqueue_inner(self, task):
        raise NotImplementedError
