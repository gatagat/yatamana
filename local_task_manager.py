from __future__ import (
        division, print_function, unicode_literals, absolute_import)
import logging
import threading
from collections import OrderedDict
from .utils import which
from .task_manager import TaskManager


class LocalTaskManager(TaskManager):
    '''Task manager to run tasks locally in a blocking fashion.
    '''

    default_submit_command = which('bash')

    def __init__(self, setup_file, **kwargs):
        super(LocalTaskManager, self).__init__(setup_file, **kwargs)
        self.last_job_id = 0
        self._last_job_id_lock = threading.Lock()

    def map_opts(self, opts):
        '''Map resolved task options into local options.

        Parameters
        ----------
        opts : dict-like
            Resolved options to be mapped.

        Returns
        -------
        mapped : dict-like
            Options mapped into command-line options for sbatch.
        '''
        log = logging.getLogger(self.__class__.__name__)
        mapped = OrderedDict()
        for name, value in opts.items():
            if name == 'raw':
                mapped['raw'] = [value]
            elif name == 'current_working_directory':
                pass
            elif name == 'name':
                pass
            elif name == 'dependencies':
                pass
            else:
                log.warning('Cannot map option: %s', name)
        return mapped

    def get_job_id(self, output):
        '''Get fake job ID.

        Parameters
        ----------
        output : string
            Ignored.

        Returns
        -------
        job_id : int
            Fake job ID.
        '''
        with self._last_job_id_lock:
            self.last_job_id += 1
        return self.last_job_id
