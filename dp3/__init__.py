"""
# Dynamic Profile Processing Platform (DP³)

Platform directory structure:

- [Worker][dp3.worker] - The main worker process.

- [API][dp3.api] - The HTTP API implementation.

- [Binaries][dp3.bin] under `dp3.bin` - Executables for running the platform.

- [Common][dp3.common] - Common modules which are used throughout the platform.
    - [Config][dp3.common.config], [EntitySpec][dp3.common.entityspec] and
    [AttrSpec][dp3.common.attrspec] - Models for reading, validation and representing
    platform configuration of entities and their attributes.
    [datatype][dp3.common.datatype] is also used within this context.
    - [Scheduler][dp3.common.scheduler] - Allows modules to run callbacks at specified times
    - [Task][dp3.common.task.Task] - Model for a single task processed by the platform
    - [Utils][dp3.common.utils] - Auxiliary utility functions

- [Database.EntityDatabase][dp3.database.database] - A wrapper responsible for communication
with the database server.

- [HistoryManagement.HistoryManager][dp3.history_management.history_manager] - Module responsible
for managing history saved in database, currently to clean old data.

- [Snapshots][dp3.snapshots] - [SnapShooter][dp3.snapshots.snapshooter], a module responsible
for snapshot creation and running configured data correlation and fusion hooks,
and [Snapshot Hooks][dp3.snapshots.snapshot_hooks], which manage the registered hooks and their
dependencies on one another.

- [TaskProcessing][dp3.task_processing] - Module responsible for task
[distribution][dp3.task_processing.task_distributor],
[processing][dp3.task_processing.task_executor] and running configured
[hooks][dp3.task_processing.task_hooks]. Task distribution is possible due to the
[task queue][dp3.task_processing.task_queue].
"""
