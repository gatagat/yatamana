import os
import numbers
import logging
from collections import OrderedDict


def parse_walltime(value):
    '''Parse walltime in the setup file.

    Parameters
    ----------
    value : int | string
        Walltime specified in any of the formats below.

    Acceptable time formats include:

    - minutes (int)
    - "minutes"
    - "minutes:seconds"
    - "hours:minutes:seconds"
    - "days-hours"
    - "days-hours:minutes"
    - "days-hours:minutes:seconds"

    Returns
    -------
    seconds : int
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


class Task(object):
    '''Base class for all tasks to be submitted.
    '''
    def __init__(self):
        self.opts = OrderedDict([])
        self.job_id = None
        name = self.__class__.__name__
        if name.endswith('Task'):
            name = name[:-len('Task')]
        name += '-%(salt)s'
        self.opts['name'] = name
        self.defaults = OrderedDict(current_working_directory=True)

    def update_defaults(self, defaults):
        self.defaults.update(defaults)

    def is_finished(self):
        return False

    def __repr__(self):
        return r'%s[name=%s, id=%s]' % (
                self.__class__.__name__,
                self.opts['name'],
                self.job_id)

    def resolve_opts(self, values, update_self=True):
        '''Resolve tasks's options.

        Parameters
        ----------
        values : dict-like
            Values to use for format string resolution.
        update_self : bool
            Whether or not to update self.opts.

        Returns
        -------
        resolved : dict-like
            Resolved options.
        '''
        log = logging.getLogger(self.__class__.__name__)
        opts = self.defaults.copy()
        opts.update(self.opts)
        resolved = OrderedDict()
        for name, value in opts.items():
            if name == 'raw':
                resolved['raw'] = value
            elif name == 'current_working_directory':
                resolved[name] = True
            elif name == 'log_filename':
                resolved[name] = os.path.expandvars(os.path.join(
                    value % values))
            elif name == 'name':
                resolved[name] = value % values
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
        if update_self is True:
            self.opts = resolved
        return resolved

    def render_command(self):
        '''Render task's command into a string.'''
        return ' '.join(self.command)

    def render_runner(self, template):
        '''Render a runner script according to the template.

        Parameters
        ----------
        template : format string
            Template of the runner script.

        The template can contain:
            - %(modules)s
            - %(command)s

        Returns
        -------
        runner_script : string
            Rendered runner script.
        '''
        contents = '\n'.join(template)
        modules = self.opts.get('modules')
        if modules is None:
            modules = ''
        else:
            modules = ' '.join(modules)
        contents = contents % {
                'command': self.render_command(),
                'modules': modules}
        return contents