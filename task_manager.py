from __future__ import (
        division, print_function, unicode_literals, absolute_import)
import os
import json
import logging
from copy import deepcopy
from datetime import datetime
from tempfile import NamedTemporaryFile
from .utils import run_cmd, make_salt, makedirs, make_executable
from .task import Task
from .chunk_of_tasks_task import ChunkOfTasksTask


class TaskManager(object):
    '''Base class for all task managers.

    See also
    --------
    SgeTaskManager
    SlurmTaskManager
    '''
    def __init__(self, setup_file, dryrun=False, **kwargs):
        self.log = logging.getLogger(self.__class__.__name__)
        self.dryrun = dryrun
        self.kwargs = json.load(open(setup_file))
        self.kwargs.update(kwargs)
        if 'salt' not in self.kwargs:
            self.kwargs['salt'] = make_salt(6)
        if 'submit_command' not in self.kwargs:
            cmd = self.__class__.default_submit_command
            self.kwargs['submit_command'] = cmd
            self.log.info('Using default submit command: %s', cmd)
        tmp = self.kwargs.get('shared_tmp')
        if tmp is None:
            self.runner_dir = 'run'
        else:
            self.runner_dir = os.path.expandvars(os.path.join(tmp, 'run'))

    def enqueue(self, task):
        '''Enqueue a given task to be computed.

        Parameters
        ----------
        task : Task | iterable of Task-s
            One or more tasks to be computed.

        If mupltiple tasks are passed in this function, a single wrapper task
        ChunkOfTasksTask is created and enqueued. All the tasks land inside a
        single compute job.

        Returns
        -------
        enqueued_task : Task
            Enqueued single task or ChunkOfTasksTask in the case of multiple
            tasks.
        '''
        if not issubclass(task.__class__, Task):
            tasks = task
            for task in tasks:
                task.update_defaults(self.get_task_defaults(
                    task.__class__.__name__))
            task = ChunkOfTasksTask(tasks)
        task.update_defaults(self.get_task_defaults(task.__class__.__name__))
        return self.enqueue_inner(task)

    def enqueue_chunked(self, tasks, n=5):
        """Enqueue in chunks.

        Iterator returning enqueued jobs.

        Parameters
        ----------
        tasks - iterable of tasks
        n - number of tasks per chunk
        """
        chunk = []
        for task in tasks:
            chunk += [task]
            if len(chunk) >= n:
                yield self.enqueue(chunk)
                chunk = []
        if chunk:
            yield self.enqueue(chunk)

    def enqueue_inner(self, task):
        '''Actually enque task.'''
        assert task.job_id is None
        log = logging.getLogger(self.__class__.__name__)
        task.resolve_opts(self.kwargs)
        log_filename = task.opts.get('log_filename')
        if log_filename is not None:
            makedirs(os.path.dirname(log_filename))
        enqueue_cmd = [
                self.kwargs['submit_command']] + [
                o for oo in self.map_opts(task.opts).values() for o in oo]
        runner_name = self.make_runner(task)
        log.info('Prepared a runner file: %s', runner_name)
        enqueue_cmd += [runner_name]
        with open(runner_name, 'a') as fw:
            footer = [
                    '',
                    '#',
                    '# Created at %s' % datetime.now(),
                    '# In %s' % os.getcwd(),
                    '# Command planned:',
                    '# %s' % ' '.join(enqueue_cmd),
                    '#']
            fw.write('\n'.join(footer))
        if self.dryrun is True:
            log.info('Would run %s', enqueue_cmd)
            task.job_id = -1
        else:
            out = run_cmd(enqueue_cmd)
            task.job_id = self.get_job_id(out)
            log.info('Enqueued %s' % task)
        return task

    def make_runner(self, task):
        '''Create a runner script in a temporary file.

        Parameters
        ----------
        task : Task
            Task for which a runner script is created.

        Returns
        -------
        runner_name : string
            Filename of the created runner script.
        '''
        log = logging.getLogger(self.__class__.__name__)
        setup = self.get_runner_setup()
        makedirs(self.runner_dir)
        with NamedTemporaryFile(
                mode='w', suffix='.sh',
                prefix=task.__class__.__name__ + '-', dir=self.runner_dir,
                delete=False) as fw:
            template = setup.get('template')
            if template is None:
                log.error('Missing a runner.template section')
                raise ValueError('Missing a runner.template section')
            template = '\n'.join(template)
            fw.write(task.render_runner(template))
        make_executable(fw.name)
        return fw.name

    def get_runner_setup(self):
        '''Get a runner section of the setup.'''
        runner_setup = self.kwargs.get('runner')
        if runner_setup is None:
            self.log.error('Missing a runner section')
            raise ValueError('Missing a runner section')
        return runner_setup

    def get_task_defaults(self, clsname):
        '''Get default opts for a task of a given class.

        Options are collected from the general setup in runner.opts and
        specific setup in tasks.clsname. The latter overwrite the former.

        Parameters
        ----------
        clsname : string
            Name of the task class.

        Returns
        -------
        defaults : dict-like
            Default options.
        '''
        log = logging.getLogger(self.__class__.__name__)
        opts = self.get_runner_setup().get('opts', {})
        opts = opts.copy()
        setup = self.kwargs.get('tasks', {})
        setup = setup.get(clsname)
        if setup is None:
            log.warning('Missing a tasks.%s section', clsname)
        else:
            opts.update(setup)
        opts = deepcopy(opts)
        return opts
