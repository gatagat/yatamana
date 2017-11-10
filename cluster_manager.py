import json
import logging
import numbers
from tempfile import NamedTemporaryFile

from .utils import make_salt, makedirs, make_executable

from .cluster_task import ChunkOfTasksTask


def render_runner_script(template, commands=None, modules=None):
    contents = '\n'.join(template)
    if modules is None:
        modules = ''
    else:
        modules = ' '.join(modules)
    if commands is None:
        commands = ''
    else:
        commands = ' && '.join(commands)
    contents = contents % {'commands': commands, 'modules': modules}
    return contents


def parse_walltime(value):
    '''Parse walltime of form dd-hh:mm:ss.

    Acceptable time formats include:

    - minutes (int)
    - "minutes"
    - "minutes:seconds"
    - "hours:minutes:seconds"
    - "days-hours"
    - "days-hours:minutes"
    - "days-hours:minutes:seconds"

    Returns
    =======
    seconds  :   int
        Walltime in seconds.
    '''
    if isinstance(value, numbers.Integral):
        minutes = int(value)
        return minutes * 60
    days, hours, minutes, seconds = 0, 0, 0, 0
    if '-' in value:
        days, value = value.split('-')
        if ':' in value:
            value = value.split(':')
        else:
            value = [value]
        hours = int(value[0])
        if len(value) > 1:
            minutes = int(value[1])
        if len(value) > 2:
            seconds = int(value[2])
    elif ':' in value:
        value = value.split(':')
        if len(value) == 2:
            minutes = int(value[0])
        else:
            hours = int(value[0])
            minutes = int(value[1])
        seconds = int(value[-1])
    else:
        minutes = int(value)
    return (((days * 24 + hours) * 60) + minutes) * 60 + seconds


class ClusterManager(object):
    '''Base class for all cluster managers.

    See also:
    =========
    SgeClusterManager
    SlurmClusterManager
    '''
    def __init__(self, setup_file, dryrun=False, **kwargs):
        log = logging.getLogger(self.__class__.__name__)
        self.dryrun = dryrun
        self.kwargs = json.load(open(setup_file))
        self.kwargs.update(kwargs)
        if 'salt' not in self.kwargs:
            self.kwargs['salt'] = make_salt(6)
        if 'submit_command' not in self.kwargs:
            cmd = self.__class__.default_submit_command
            self.kwargs['submit_command'] = cmd
            log.info('Using default submit command: %s', cmd)

    def enqueue(self, task):
        if not issubclass(tasks.__class__, ClusterTask):
            tasks = task
            for task in tasks:
                if task.__class__ != tasks[0].__class__:
                    log.error('Only chunks with tasks of a single type are '
                              'allowed. Found %s and %s.',
                              task.__class__.__name__,
                              tasks[0].__class__.__name__)
                    raise ValueError('Multiple task type in a chunk.')
            task = ChunkOfTasksTask(tasks)
        return self.enqueue_inner(task)

    def make_runner(self, setup, task):
        log = logging.getLogger(self.__class__.__name__)
        makedirs(self.runner_dir)
        with NamedTemporaryFile(
                mode='w', suffix='.sh',
                prefix=task.__class__.__name__ + '-', dir=self.runner_dir,
                delete=False) as fw:
            template = setup.get('template')
            if template is None:
                log.error('Missing a runner.template section')
                raise RuntimeError
            fw.write(render_runner_script(
                template=template,
                commands=task.render_cmd(),
                modules=tasks.opts.get('modules')))
        make_executable(fw.name)
        return fw.name

    def resolve_opts(self, opts, defaults=None):
        log = logging.getLogger(self.__class__.__name__)
        if defaults is None:
            defaults = {}
        resolved = OrderedDict()
        for name, value in defaults.items() + opts.items():
            if name == 'raw':
                resolved['raw'] = value
            elif name == 'working_directory':
                resolved[name] = '.'
            elif name == 'log_directory':
                log_filename = self.__class__.default_log_filename
                resolved[name] = os.path.join(os.path.expandvars(
                    value % self.kwargs), log_filename)
            elif name == 'name':
                resolved[name] = value % self.kwargs
            elif name == 'walltime':
                resolved[name] = parse_walltime(value)
            elif name == 'cores':
                resolved[name] = value
            elif name == 'memory':
                resolved[name] = value
            elif name == 'dependencies':
                resolved[name] = [_.job_id for _ in value]
            elif name == 'modules':
                resolved[name] = value
            else:
                log.error('Cannot resolve option: %s', name)
        return resolved
