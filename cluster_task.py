import os
from collections import OrderedDict


class ClusterTask(object):
    '''Base class for all tasks to be submitted.
    '''
    def __init__(self):
        self.opts = OrderedDict(
            current_working_directory=True,
            log_directory=os.path.join('%(shared_tmp)s', 'logs'))
        self.job_id = None
        name = self.__class__.__name__
        if name.endswith('Task'):
            name = name[:-len('Task')]
        name += '-%(salt)s'
        self.opts['name'] = name

    def is_finished(self):
        return False

    def __repr__(self):
        return r'%s[name=%s, id=%s]' % (
                self.__class__.__name__,
                self.opts['name'],
                self.job_id)

    def resolve_opts(self, opts, defaults=..., default_log_filename=..., optionally even the manager or at least its kwargs for the sake of chunkoftasktask):
        raise


    def render_cmd(self):
        return ' '.join(self.cmd)


    def render_script(self, template):
        ...the whole render script would maybe make more sense here
        - it just has to receive the template


class ChunkOfTasksTask(ClusterTask):
    def __init__(self, tasks) 
        self.tasks = tasks

    def resolve_opts(self, ...):
        for task in self.tasks:
            task.resolve_opts(...)
        self.opts = ... merge


    ...the config for ChunkOfTasksTask would allow for deciding how
    the individual tasks are merged ' && ' or ';\n'
    ...basically this part of rendering the string would be here

    def render_cmd(self):
        return ' && '.join([task.render_cmd() for task in self.tasks])
