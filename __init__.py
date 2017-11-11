from .task import Task
from .chunk_of_tasks_task import ChunkOfTasksTask
from .task_manager import TaskManager
from .sge_task_manager import SgeTaskManager
from .slurm_task_manager import SlurmTaskManager

__all__ = [
        'Task', 'ChunkOfTasksTask', 'TaskManager', 'SgeTaskManager',
        'SlurmTaskManager', 'CeleryTaskManager']


class CeleryTaskManager(TaskManager):
    def enqueue_inner(self, task):
        raise NotImplementedError
