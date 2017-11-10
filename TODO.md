? can i use array jobs
  - some templating + job args in a file + line per task(?)
  - even easier just create a file with all the arguments per task - task-$ARRAY_INDEX.args
? how many jobids can i put into -hold_jid / -D
? how to make chunking work with array jobs??

- bootstrap function - git clone or rsync to src/ and create a submit there and
  just run it - repeatable
  - introduce project dir
- keep a db of submitted jobs - would allow restarts?

- split resolve into resolve two parts - cluster-agnostic and cluster-specific
- for chunking:
  - maybe a better approach would be to have a class ChunkOfTasksTask(ClusterTask)
    - this would receive the resolved opts etc
    - it would also receive the job_id
    - it would be returned from enqueue
