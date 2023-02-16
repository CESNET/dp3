"""
A module to store program-level globals, like loaded configuration or
instances of main components.

Its contents are set up at run-time during initialization.
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
