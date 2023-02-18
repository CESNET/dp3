"""
A module to store program-level globals, like loaded configuration or
instances of main components.

Its contents are set up at run-time during initialization.

Attributes:
    config_base_path: Path to directory containing platform config
    app_name: Name of the application, used when naming various structures of the platform
    config: A dictionary that contains the platform config
    model_spec: Specification of the platform's model (entities and attributes)
    running: Whether the application is running
    scheduler: reference to the global Scheduler instance
    db: reference to the global EntityDatabase instance, use to communicate with the database
    hm: global HistoryManager instance
    ss: reference to the global SnapShooter instance, use to register correlation and fusion hooks
    te: reference to the global TaskExecutor instance, use to register various processing hooks
    td: reference to the global TaskDistributor instance, use to register various processing hooks
    daemon_stop_lock: Lock used to control when the program stops. (see [dp3.worker][])
"""
import threading

from dp3.common.config import ModelSpec
from dp3.common.scheduler import Scheduler
from dp3.database.database import EntityDatabase
from dp3.history_management.history_manager import HistoryManager
from dp3.snapshots.snapshooter import SnapShooter
from dp3.task_processing.task_distributor import TaskDistributor
from dp3.task_processing.task_executor import TaskExecutor

config_base_path: str
app_name: str
config: dict
model_spec: ModelSpec
running: bool = False
scheduler: Scheduler
db: EntityDatabase
hm: HistoryManager
ss: SnapShooter
te: TaskExecutor
td: TaskDistributor
daemon_stop_lock: threading.Lock
