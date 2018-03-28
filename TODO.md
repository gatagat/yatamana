- Is it worth to introduce automatic serialization of the Task? For pure python
  tasks, this would remove the need for a command-line interface, i would
  instead deserialize and run. For non-python jobs, i would deserialize, create
  the runner script from the template and run it using the subprocess module.

- ? How many jobids can i use as dependencies (-hold_jid/-d)? Are there any limits?

- ? Can i use array jobs
    - some templating + job args in a file + line per task(?)
    - even easier just create a file with all the arguments per task - task-$ARRAY_INDEX.args

- ? How to make chunking work with array jobs?

- Keep a db of submitted jobs
  - Is this worth the mess? Would it allow restarts?
