from __future__ import (
        division, print_function, unicode_literals, absolute_import)
import logging
from copy import deepcopy
from .task import Task


class ChunkOfTasksTask(Task):
    """ Task containing multiple tasks.

    Parameters
    ----------
    tasks : list of Task
        Tasks to put inside this chunk of tasks.
    """
    def __init__(self, tasks):
        super(ChunkOfTasksTask, self).__init__()
        if not tasks:
            raise ValueError('There has to be atleast one task in a chunk.')
        self.tasks = tasks

    def resolve_opts(self, values, update_self=True):
        """Resolve options of all the contained tasks and itself.

        To resolve the contained tasks, their :py:meth:`Task.resolve_opts`
        method is called and the information accumulated.  The number of cores
        and memory are set to the maxima of their respective individual values.
        The dependencies and environmental modules are set to their respective
        unions.  The compute time is set to a sum of the individual values.

        Parameters
        ----------
        values : dict-like
            Values to use for format string resolution.
        update_self : bool
            Whether or not to update `self.opts`.

        Returns
        -------
        resolved : dict-like
            Resolved options. Also assigned to this task.
        """
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
        """ Render the commands of all contained tasks into a single string.

        Returns
        -------
        command : str
            Rendered command.
        """
        # TODO: allow setting the separator in the config. The values could be:
        # ' && '
        # '\n'
        # '\nrv=$?\nif [ $rv != 0 ]; then log Error $rv; exit $rv; fi\n'
        #
        return ' && \\\n'.join([task.render_command() for task in self.tasks])

    def get_runner_prefix(self):
        '''Return a prefix for the runner file.'''
        if not self.tasks:
            return 'EmptyChunkOfTasks'
        prefix = 'ChunkOf'
        last_name = None
        last_count = 0
        for task in self.tasks:
            name = task.__class__.__name__
            if last_name == name:
                last_count += 1
            else:
                if last_count > 0:
                    prefix += '%sx%d' % (last_name, last_count)
                last_name = name
                last_count = 1
        if last_count > 0:
            prefix += '%sx%d' % (last_name, last_count)
        return prefix
