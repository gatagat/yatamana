#!/usr/bin/env python

import os
import sys
import logging

from yatamana import SgeTaskManager, SlurmTaskManager, Task


def setup_log(level=logging.WARNING):
    logging.basicConfig()
    log = logging.getLogger()
    log.handlers = []
    try:
        handler = logging.StreamHandler(stream=sys.stderr)
    except TypeError:
        handler = logging.StreamHandler(strm=sys.stderr)
    formatter = logging.Formatter(
            fmt='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            datefmt='%Y.%m.%d %H:%M:%S'
        )
    handler.setFormatter(formatter)
    log.addHandler(handler)
    log.setLevel(level)
    return log


class TestTask(Task):
    def __init__(self, param, param_default='123'):
        super(TestTask, self).__init__()
        self.command = ['./test_run.py', str(param), str(param_default)]

    def is_finished(self):
        return False


class Test2Task(Task):
    def __init__(self, param, param_default='123'):
        super(Test2Task, self).__init__()
        self.command = ['./test_run2.py', str(param), str(param_default)]

    def is_finished(self):
        return False


def test_sge():
    task_manager = SgeTaskManager(setup_file='sge-setup.json')
    task = TestTask('param')
    if not task.is_finished():
        task_manager.enqueue(task)
    task2 = Test2Task('param')
    task2.opts['dependencies'] = [task]
    if not task2.is_finished():
        task_manager.enqueue(task2)


def test_slurm():
    task_manager = SlurmTaskManager(setup_file='slurm-setup.json')
    task = TestTask('param')
    if not task.is_finished():
        task_manager.enqueue(task)
    task2 = Test2Task('param')
    task2.opts['dependencies'] = [task]
    if not task2.is_finished():
        task_manager.enqueue(task2)


if __name__ == '__main__':
    setup_log(logging.DEBUG)
    if os.environ.get('IMPIMBA_MACHINE_NAME') == 'IMPIMBA-2':
        test_slurm()
    else:
        test_sge()
