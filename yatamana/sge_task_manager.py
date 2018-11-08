from __future__ import (
        print_function, division, absolute_import, unicode_literals)

import logging
from collections import OrderedDict
from .utils import which
from .task_manager import TaskManager


class SgeTaskManager(TaskManager):
    """Task manager for the Sun Grid Engine (SGE).

    Handle the specifics of SGE, the universal part of the job is done by
    :obj:`TaskManager`.

    Attributes
    ----------
    default_submit_command : str
        Full path to the binary that submits to the cluster. Set to the path to
        `qsub`.

    Parameters
    ----------
    setup_file : str
        Filename of the setup file with SGE options to use.
    kwargs
        Additional configuration options. See :obj:`TaskManager`.

    See Also
    --------
    TaskManager
    """

    default_submit_command = which('qsub')

    def __init__(self, setup_file, **kwargs):
        super(SgeTaskManager, self).__init__(setup_file, **kwargs)

    def map_opts(self, opts):
        """Map resolved task options into SGE-specific options.

        Parameters
        ----------
        opts : dict-like
            Resolved options to be mapped.

        Returns
        -------
        mapped : dict-like
            Options mapped into command-line options for qsub.
        """
        log = logging.getLogger(self.__class__.__name__)
        mapped = OrderedDict()
        for name, value in opts.items():
            if name == 'raw':
                mapped['raw'] = value
            elif name == 'current_working_directory':
                mapped[name] = ['-cwd']
            elif name == 'log_filename':
                value = value.replace('%(job_id)s', '$JOB_ID')
                mapped[name] = ['-j', 'yes', '-o', value]
            elif name == 'name':
                mapped[name] = ['-N', value]
            elif name == 'cores':
                mapped[name] = ['-pe', 'smp', str(value)]
            elif name == 'dependencies':
                mapped[name] = ['-hold_jid', ','.join(
                    [str(job_id) for job_id in value])]
            elif name == 'modules':
                pass
            else:
                log.error('Cannot map option: %s', name)
        return mapped

    def get_job_id(self, output):
        """Get job ID from the submission ouput.

        Parameters
        ----------
        output : string
            Output of qsub.

        Returns
        -------
        job_id : int
            Extracted job ID.
        """
        return int(output.split(' ')[2])
