from __future__ import (
        division, print_function, unicode_literals, absolute_import)
import logging
from copy import deepcopy
from .task import Task


class ChunkOfTasksTask(Task):
    def __init__(self, tasks):
        super(ChunkOfTasksTask, self).__init__()
        if not tasks:
            raise ValueError('There has to be atleast one task in a chunk.')
        self.tasks = tasks

    def resolve_opts(self, values, update_self=True):
        '''Resolve options of all the contained tasks and itself.

        Parameters
        ----------
        values : dict-like
            Values to use for format string resolution.
        update_self : bool
            Whether or not to update self.opts.

        Returns
        -------
        resolved : dict-like
            Resolved options. Also assigned to this task.
        '''
        cores = 0
        dependencies = set()
        memory = 0
        modules = None
        walltime = 0
        for i, task in enumerate(self.tasks):
            task.resolve_opts(values)
            cores = max(task.opts.get('cores', 0), cores)
            memory = max(task.opts.get('memory', 0), memory)
            dependencies |= set(task.opts.get('dependencies', []))
            walltime += task.opts.get('walltime', 0)
            modules_i = set(task.opts.get('modules', []))
            if i == 0:
                modules = modules_i
            else:
                if modules != modules_i:
                    log = logging.getLogger(self.__class__.__name__)
                    log.warning(
                        'Tasks in chunk request differing modules: %s vs. %s',
                        list(modules), list(modules_i))
        resolved = deepcopy(self.tasks[0].opts)
        if cores > 0:
            resolved['cores'] = cores
        if dependencies:
            resolved['dependencies'] = dependencies
        if memory > 0:
            resolved['memory'] = memory
        if walltime:
            resolved['walltime'] = walltime
        if update_self is True:
            self.opts = resolved
        return resolved

    def render_command(self):
        # TODO: allow setting the separator in the config. The values could be:
        # ' && '
        # '\n'
        # '\nrv=$?\nif [ $rv != 0 ]; then log Error $rv; exit $rv; fi\n'
        #
        return ' && \\\n'.join([task.render_command() for task in self.tasks])
