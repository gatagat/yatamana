import logging
from math import ceil
from collections import OrderedDict
from .utils import which
from .task_manager import TaskManager


def format_time(seconds):
    '''Format time interval the slurm way.

    Parameters
    ----------
    seconds : int
        Number of seconds.

    Returns
    -------
    s : string
        Formatted time interval.
    '''
    seconds = int(ceil(seconds))
    minutes = seconds // 60
    seconds -= minutes * 60
    hours = minutes // 60
    minutes -= hours * 60
    days = hours // 24
    hours -= days * 24
    return '%02d-%02d:%02d:%02d' % (days, hours, minutes, seconds)


class SlurmTaskManager(TaskManager):
    '''Task manager for Slurm.
    '''

    default_submit_command = which('sbatch')

    def __init__(self, setup_file, **kwargs):
        super(SlurmTaskManager, self).__init__(setup_file, **kwargs)

    def map_opts(self, opts):
        '''Map resolved task options into slurm-specific options.

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
                mapped['raw'] = value
            elif name == 'current_working_directory':
                mapped[name] = ['-D', '.']
            elif name == 'log_filename':
                value = value.replace('%(job_id)s', '%j')
                mapped[name] = ['-o', value]
            elif name == 'name':
                mapped[name] = ['-J', value]
            elif name == 'walltime':
                mapped[name] = ['-t', format_time(value)]
            elif name == 'qos':
                mapped[name] = ['--qos=' + value]
            elif name == 'cores':
                mapped[name] = ['-n', str(value)]
            elif name == 'memory':
                mapped[name] = ['--mem-per-cpu=%dG' % value]
            elif name == 'dependencies':
                mapped[name] = ['-d', ':'.join(
                    ['afterok'] + [str(job_id) for job_id in value])]
            elif name == 'modules':
                pass
            else:
                log.error('Cannot map option: %s', name)
        return mapped

    def get_job_id(self, output):
        '''Get job ID from the submission ouput.

        Parameters
        ----------
        output : string
            Output of sbatch.

        Returns
        -------
        job_id : int
            Extracted job ID.
        '''
        return int(output.split(' ')[3])
