yatamana - yet another task manager
===================================

Yatamana simplifies submitting tasks to (HPC) clusters in a cluster-agnostic
way.  As of now there is an initial support for Sun Grid Engine and Slurm.

In the future, local Celery local blocking execution will be added.


Usage
-----

1. Instantiate a subclass of TaskManager, e.g., SlurmTaskManager
2. Subclass `Task` setting its command field.
3. Ask the task manager to enqueue the task.

Runner script and task options are defined through a simple json setup file.

As of now, there is no automated serialization of the Task or its data. The
Task is expected to prepare a command line to be run in its `command` field,
which is then embedded inside a runner script submitted to the cluster.

### Json setup file

Setup files have to contain at least a runner script template under
`runner.template` key.

- `runner.template` - a list of lines of the runner script to use
- `opts.tasks.SomeTask` - options for a given class of tasks

Task options include:

- `cores` - number of requested cores
- `current_working_directory` - run in the current working directory [default]
- `log_filename`
- `memory` - requested memory in GB
- `walltime` - maximum runtime in minutes (integer) or:
        "minutes"
        "minutes:seconds"
        "hours:minutes:seconds"
        "days-hours"
        "days-hours:minutes"
        "days-hours:minutes:seconds"
- `raw` - raw options passed to the submission process as they are


Minimal example
---------------

setup.json:

    {
      "runner": {
        "template": [
          "#!/bin/bash",
          "%(command)s"
        ]
      },
      "tasks": {
        "SimpleTask": {
          "cores": 3
        }
      }
    }

submit.py:

    from yatamana import Task, SgeTaskManager

    class SimpleTask(Task):
        def __init__(self):
            super(SimpleTask, self).__init__()
            self.command = ['echo Running on $(hostname) at $(date)']

    if __name__ == '__main__':
        manager = SgeTaskManager('setup.json')
        manager.enqueue(SimpleTask())
