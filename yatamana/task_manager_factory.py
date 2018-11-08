from __future__ import (
        division, print_function, unicode_literals, absolute_import)
import json
from .local_task_manager import LocalTaskManager
from .sge_task_manager import SgeTaskManager
from .slurm_task_manager import SlurmTaskManager


def get_task_manager(setup_file, **kwargs):
    '''Create a task manager of a correct type.

    Parameters
    ----------
    setup_file : string
        File name of the setup file.
    kwargs : dict
        Additional kwargs.

    Returns
    -------
    manager : TaskManager
        Created task manager.
    '''
    setup = json.load(open(setup_file))
    manager = setup['manager'].lower()
    if manager == 'slurm':
        return SlurmTaskManager(setup_file, **kwargs)
    elif manager == 'sge':
        return SgeTaskManager(setup_file, **kwargs)
    elif manager == 'local':
        return LocalTaskManager(setup_file, **kwargs)
    else:
        raise ValueError('Unknown task manager: %s', manager)
