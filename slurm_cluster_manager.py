import os
from datetime import datetime
import logging
from math import ceil
from collections import OrderedDict
from copy import deepcopy

from .utils import run_cmd, makedirs, which
from cluster_manager import ClusterManager, parse_walltime
from cluster_task import ClusterTask


def slurm_format_walltime(seconds):
    seconds = int(ceil(seconds))
    minutes = seconds // 60
    seconds -= minutes * 60
    hours = minutes // 60
    minutes -= hours * 60
    days = hours // 24
    hours -= days * 24
    return '%02d-%02d:%02d:%02d' % (days, hours, minutes, seconds)


class SlurmClusterManager(ClusterManager):
    '''Cluster manager for Slurm.
    '''

    default_submit_command = which('sbatch')
    default_log_name = '%x.%j.log'

    def __init__(self, setup_file, **kwargs):
        super(SlurmClusterManager, self).__init__(setup_file, **kwargs)
        log = logging.getLogger(self.__class__.__name__)
        tmp = self.kwargs.get('shared_tmp')
        if tmp is None:
            self.runner_dir = 'run'
        else:
            self.runner_dir = os.path.expandvars(os.path.join(tmp, 'run'))

    def map_opts(self, opts):
        mapped = OrderedDict()
        for name, value in opts.items():
            if name == 'raw':
                mapped['raw'] = [value]
            elif name == 'working_directory':
                mapped[name] = ['-D', value]
            elif name == 'log_directory':
                mapped[name] = ['-o', value]
            elif name == 'name':
                mapped[name] = ['-J', value]
            elif name == 'walltime':
                mapped[name] = ['-t', slurm_format_walltime(value)]
            elif name == 'cores':
                mapped[name] = ['-n', str(value)]
                cores = max(cores, value)
            elif name == 'memory':
                mapped[name] = ['--mem=%dG' % value]
            elif name == 'dependencies':
                mapped[name] = ['-d', ':'.join(
                    ['afterok'] + [str(job_id) for job_id in value])]
            elif name == 'modules':
                pass
            else:
                log.error('Cannot map option: %s', name)
        return mapped

    def resolve_opts(self, chunk, opts=None):
        # TODO: split resolve into resolve which would be cluster
        # agnostic and format/use - which would transform the opts
        # into cluster-specific representation.
        # - resolved opts would be assigned back to task and could be
        #   reused
        # - transformed opts would only be needed _on_ submission
        # - all the merging of opts for tasks inside a chunk would
        #   also be done only on submission
        log = logging.getLogger(self.__class__.__name__)
        cores = 0
        dependencies = set()
        jobname = None
        log_dir = None
        memory = 0
        modules = []
        walltime = []
        if opts is None:
            opts = {}
        for i, task in enumerate(chunk):
            resolved = OrderedDict()
            for name, value in opts.items() + task.opts.items():
                if name == 'raw':
                    resolved['raw'] = [value]
                elif name == 'current_working_directory':
                    resolved[name] = ['-D', '.']
                elif name == 'log_directory':
                    log_dir = os.path.expandvars(value % self.kwargs)
                    resolved[name] = ['-o', os.path.join(log_dir, '%x.%j.log')]
                elif name == 'name':
                    jobname_i = value % self.kwargs
                    resolved[name] = ['-J', jobname_i]
                    if jobname is None:
                        jobname = jobname_i
                elif name == 'walltime':
                    walltime_i = parse_walltime(value)
                    resolved[name] = ['-t', slurm_format_walltime(walltime_i)]
                    walltime += [walltime_i]
                elif name == 'cores':
                    resolved[name] = ['-n', str(value)]
                    cores = max(cores, value)
                elif name == 'memory':
                    resolved[name] = ['--mem=%dG' % value]
                    memory = max(memory, value)
                elif name == 'dependencies':
                    dependencies_i = [_.job_id for _ in value]
                    resolved[name] = ['-d', ':'.join(
                        ['afterok'] + [str(_) for _ in dependencies_i])]
                    dependencies |= set(dependencies_i)
                elif name == 'modules':
                    if i == 0:
                        modules = value
                        resolved[name] = modules
                    else:
                        same = len(modules) == len(value)
                        if same:
                            for mm, mv in zip(modules, value):
                                same &= mm == mv
                        if not same:
                            log.warning(
                                    'Differing modules in chunk: %s vs. %s',
                                    modules, value)
                else:
                    log.error('Unknown task option: %s', name)
            task.opts = resolved
        return cores, dependencies, jobname, log_dir, memory, walltime

    def resolve_opts(self, chunk, opts=None):
        '''Resolve options for a chunk of tasks.

        Parameters
        ----------
        chunk : iterable of ClusterTask
            Tasks whose options should be resolved and combined.

        Returns
        -------
        jobname : string
            Name of the job.
        log_dir : string
            Logging directory.
        opts : list of strings
            Resolved options prepared for job submission.
        '''
        cores, dependencies, jobname, log_dir, memory, walltime = \
            self._resolve_opts(chunk, opts=opts)
        chunk_opts = deepcopy(chunk[0].opts)
        if walltime:
            walltime = sum(walltime)
            chunk_opts['walltime'] = ['-t', slurm_format_walltime(walltime)]
        if cores > 0:
            chunk_opts['cores'] = ['-n', str(cores)]
        if memory > 0:
            chunk_opts['memory'] = ['--mem=%dG' % memory]
        if dependencies:
            chunk_opts['dependencies'] = ['-d', ':'.join(
                ['afterok'] + [str(_) for _ in dependencies])]
        # XXX: misusing resolved opts - would also be solved by
        # splitting resolve into resolve and render.
        chunk_opts = [o for name, oo in chunk_opts.items()
                      for o in oo if name != 'modules']
        return jobname, log_dir, chunk_opts

    def update_opts(self, task):
        log = logging.getLogger(self.__class__.__name__)
        setup = self.kwargs.get('tasks')
        if setup is None:
            log.warning('Missing a tasks section')
            return
        cls_name = task.__class__.__name__
        setup = setup.get(cls_name)
        if setup is None:
            log.warning('Missing a tasks.%s section', cls_name)
            return
        setup = deepcopy(setup)
        for key, value in setup.items():
            if key not in task.opts:
                task.opts[key] = value

    def enqueue_inner(self, task):
        log = logging.getLogger(self.__class__.__name__)

        runner_setup = self.kwargs.get('runner')
        if runner_setup is None:
            log.error('Missing a runner section')
            raise RuntimeError
        runner_opts = runner_setup.get('opts')

        hey this all seems pretty cluster agnostic ==> isnt it enough to define map_opts in this class?

        prepare_task_opts_defaults = \
                something like self.update_opts + runner_opts + self.kwargs(?)
        task.resolve_opts(tasks)
        mapped_opts = self.map_opts(task.opts)
        enqueue_cmd = [self.kwargs['submit_command']] + mapped_opts
        runner_name = self.make_runner(runner_setup, task)
        log.info('Prepared a runner file: %s', runner_name)
        enqueue_cmd += [runner_name]
        with open(runner_name, 'a') as fw:
            fw.write('\n# Submitted %s using:\n# %s\n' % (
                datetime.now(), ' '.join(enqueue_cmd)))
        if self.dryrun is True:
            log.info('Would run %s', enqueue_cmd)
        else:
            out = run_cmd(enqueue_cmd)
            task[0].job_id = int(out.split(' ')[3])
            log.info('Enqueued %s' % task[0])
        return task[0]
