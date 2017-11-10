import os
from datetime import datetime
import logging
from collections import OrderedDict

from .utils import run_cmd, makedirs, which
from cluster_manager import ClusterManager
from cluster_task import ClusterTask


class SgeClusterManager(ClusterManager):
    '''Cluster manager for the Sun Grid Engine.
    '''

    default_submit_command = which('qsub')
    default_log_filename = '$JOB_NAME.$JOB_ID.log'

    def __init__(self, setup_file, **kwargs):
        super(SgeClusterManager, self).__init__(setup_file, **kwargs)
        log = logging.getLogger(self.__class__.__name__)
        tmp = self.kwargs.get('shared_tmp')
        if tmp is None:
            self.runner_dir = 'run'
        else:
            self.runner_dir = os.path.expandvars(os.path.join(tmp, 'run'))

    def resolve_opts(self, opts):
        log = logging.getLogger(self.__class__.__name__)
        resolved = OrderedDict(
            shell=['-S', '/bin/bash'])
        jobname = None
        log_dir = None
        for name, value in opts.items():
            if name == 'current_working_directory':
                resolved[name] = ['-cwd']
            elif name == 'log_directory':
                log_dir = os.path.expandvars(value % self.kwargs)
                resolved[name] = [
                    '-j', 'yes',
                    '-o', os.path.join(
                        log_dir, '$JOB_NAME.$JOB_ID.log')
                    ]
            elif name == 'name':
                jobname = value % self.kwargs
                resolved[name] = ['-N', jobname]
            elif name == 'cores':
                resolved[name] = ['-pe', 'smp', str(value)]
            elif name == 'dependencies':
                resolved[name] = [
                        '-hold_jid', ','.join([str(_.job_id) for _ in value])]
            else:
                log.error('Unknown task option: %s', name)
        return jobname, log_dir, resolved


    def enqueue_inner(self, task):
        log = logging.getLogger(self.__class__.__name__)
        if not issubclass(task.__class__, ClusterTask):
            all_tasks = task
            task = task[0]
        else:
            all_tasks = [task]
        for task in all_tasks:
            self.update_opts(task)
        runner_setup = self.kwargs.get('runner')
        if runner_setup is None:
            log.error('Missing a runner section')
            raise RuntimeError
        runner_opts = runner_setup.get('opts')
        jobname, log_dir, opts = self.resolve_opts(all_tasks, opts=runner_opts)
        if log_dir is not None:
            if makedirs(log_dir):
                log.info('Created log directory: %s', log_dir)
        enqueue_cmd = [self.qsub_path] + opts
        runner_name = self.make_runner(runner_setup, all_tasks)
        log.info('Prepared a runner file: %s', runner_name)
        enqueue_cmd += [runner_name]
        with open(runner_name, 'a') as fw:
            fw.write('\n# Submitted %s using:\n# %s\n' % (
                datetime.now(), ' '.join(enqueue_cmd)))
        if self.dryrun is True:
            log.info('Would run %s', enqueue_cmd)
        else:
            out = run_cmd(enqueue_cmd)
            task.job_id = int(out.split(' ')[2])
            log.info('Enqueued %s' % task)
        return task
