from __future__ import (
        division, print_function, unicode_literals, absolute_import)

import os
from collections import OrderedDict
from .utils import parse_walltime


class Task(object):
    """Base class for all tasks to be submitted.
    """
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
        """Update the defaults.

        Parameters
        ----------
        defaults : dict
            New values for some of the defaults.
        """
        self.defaults.update(defaults)

    def is_finished(self):
        """Check whether the task has already successfully finished.

        Returns
        -------
        finished : bool
            The default implementation always returns False.
        """
        return False

    def __repr__(self):
        return r'%s[name=%s, id=%s]' % (
                self.__class__.__name__,
                self.opts['name'],
                self.job_id)

    def resolve_opts(self, values, update_self=True):
        """Resolve tasks's options.

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
        """
        opts = self.defaults.copy()
        opts.update(self.opts)
        resolved = OrderedDict()
        if 'name' in opts:
            resolved['name'] = opts['name'] % values
        for name, value in opts.items():
            if name == 'current_working_directory':
                resolved[name] = True
            elif name == 'log_filename':
                tmp = values.copy()
                tmp['job_name'] = resolved['name']
                tmp['job_id'] = '%(job_id)s'
                resolved[name] = os.path.expandvars(os.path.join(
                    value % tmp))
            elif name == 'name':
                pass
            elif name == 'walltime':
                resolved[name] = parse_walltime(value)
            elif name == 'dependencies':
                if value:
                    resolved[name] = [_.job_id for _ in value]
            else:
                resolved[name] = value
        if update_self is True:
            self.opts = resolved
        return resolved

    def render_command(self):
        """Render task's command into a string.

        Returns
        -------
        command : str
            Rendered command.
        """
        return ' '.join(self.command)

    def render_runner(self, template):
        """Render a runner script according to the template.

        Parameters
        ----------
        template : str
            Template of the runner script as a format string.  The template can
            contain ``'%(modules)s'`` and ``'%(command)s'``.

        Returns
        -------
        runner_script : str
            Rendered runner script.
        """
        modules = self.opts.get('modules')
        if modules is None:
            modules = ''
        else:
            modules = ' '.join(modules)
        contents = template % {
                'command': self.render_command(),
                'modules': modules}
        return contents

    def get_runner_prefix(self):
        """Return a prefix for the runner file.
        """
        return self.__class__.__name__
