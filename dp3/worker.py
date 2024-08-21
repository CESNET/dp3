#!/usr/bin/env python3
"""Code of the main worker process.

Don't run directly. Import and run the main() function.
"""
import inspect
import logging
import os
import signal
import sys
import threading
from functools import partial
from importlib import import_module

from event_count_logger import DummyEventGroup, EventCountLogger
from pydantic import ValidationError

from dp3.common.callback_registrar import CallbackRegistrar, reload_module_config
from dp3.common.config import PlatformConfig
from dp3.common.control import Control, ControlAction, refresh_on_entity_creation
from dp3.core.collector import GarbageCollector
from dp3.core.link_manager import LinkManager
from dp3.core.updater import Updater
from dp3.history_management.telemetry import Telemetry
from dp3.task_processing.task_queue import TaskQueueWriter

if __name__ == "__main__":
    print("Don't run this file directly. Use 'bin/worker' instead.", file=sys.stderr)
    sys.exit(1)

from .common import scheduler
from .common.base_module import BaseModule
from .common.config import ModelSpec, read_config_dir
from .database.database import EntityDatabase
from .history_management.history_manager import HistoryManager
from .snapshots.snapshooter import SnapShooter
from .task_processing.task_distributor import TaskDistributor
from .task_processing.task_executor import TaskExecutor


def load_modules(
    modules_dir: str,
    enabled_modules: str,
    log: logging.Logger,
    registrar: CallbackRegistrar,
    platform_config: PlatformConfig,
) -> dict[str, BaseModule]:
    """Load plug-in modules

    Import Python modules with names in 'enabled_modules' from 'modules_dir' directory
    and return all found classes derived from BaseModule class.
    """
    # Get list of all modules available in given folder
    # [:-3] is for removing '.py' suffix from module filenames
    available_modules = []
    for item in os.scandir(modules_dir):
        # A module can be a Python file or a Python package
        # (i.e. a directory with "__init__.py" file)
        if item.is_file() and item.name.endswith(".py"):
            available_modules.append(item.name[:-3])  # name without .py
        if item.is_dir() and "__init__.py" in os.listdir(os.path.join(modules_dir, item.name)):
            available_modules.append(item.name)

    log.debug(f"Available modules: {', '.join(available_modules)}")
    log.debug(f"Enabled modules: {', '.join(enabled_modules)}")

    # Check if all desired modules are in modules folder
    missing_modules = set(enabled_modules) - set(available_modules)
    if missing_modules:
        log.fatal(
            "Some of desired modules are not available (not in modules folder), "
            f"specifically: {missing_modules}"
        )
        sys.exit(2)

    # Do imports of desired modules from 'modules' folder
    # (rewrite sys.path to modules_dir, import all modules and rewrite it back)
    log.debug("Importing modules ...")
    sys.path.insert(0, modules_dir)
    imported_modules: list[tuple[str, str, type[BaseModule]]] = [
        (module_name, name, obj)
        for module_name in enabled_modules
        for name, obj in inspect.getmembers(import_module(module_name))
        if inspect.isclass(obj) and BaseModule in obj.__bases__
    ]
    del sys.path[0]

    # Loaded modules dict will contain main classes from all desired modules,
    # which has BaseModule as parent
    modules_main_objects = {}
    for module_name, _, obj in imported_modules:
        # Append instance of module class (obj is class --> obj() is instance)
        # --> call init, which registers handler
        module_config = platform_config.config.get(f"modules.{module_name}", {})
        modules_main_objects[module_name] = obj(platform_config, module_config, registrar)
        log.info(f"Module loaded: {module_name}:{obj.__name__}")

    return modules_main_objects


def main(app_name: str, config_dir: str, process_index: int, verbose: bool) -> None:
    """
    Run worker process.
    Args:
        app_name: Name of the application to distinct it from other DP3-based apps.
            For example, it's used as a prefix for RabbitMQ queue names.
        config_dir: Path to directory containing configuration files.
        process_index: Index of this worker process. For each application
            there must be N processes running simultaneously, each started with a
            unique index (from 0 to N-1). N is read from configuration
            ('worker_processes' in 'processing_core.yml').
        verbose: More verbose output (set log level to DEBUG).
    """
    ##############################################
    # Initialize logging mechanism
    threading.current_thread().name = f"MainThread-{process_index}"
    LOGFORMAT = "%(asctime)-15s,%(threadName)s,%(name)s,[%(levelname)s] %(message)s"
    LOGDATEFORMAT = "%Y-%m-%dT%H:%M:%S"

    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO, format=LOGFORMAT, datefmt=LOGDATEFORMAT
    )
    log = logging.getLogger()

    # Disable INFO and DEBUG messages from some libraries
    logging.getLogger("requests").setLevel(logging.WARNING)
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("amqpstorm").setLevel(logging.WARNING)

    ##############################################
    # Load configuration
    config_base_path = os.path.abspath(config_dir)
    log.debug(f"Loading config directory {config_base_path}")

    # Whole configuration should be loaded
    config = read_config_dir(config_base_path, recursive=True)
    try:
        model_spec = ModelSpec(config.get("db_entities"))
    except ValidationError as e:
        log.fatal("Invalid model specification: %s", e)
        sys.exit(2)

    # Print whole attribute specification
    log.debug(model_spec)

    num_processes = config.get("processing_core.worker_processes")

    platform_config = PlatformConfig(
        app_name=app_name,
        config_base_path=config_base_path,
        config=config,
        model_spec=model_spec,
        process_index=process_index,
        num_processes=num_processes,
    )
    ##############################################
    # Create instances of core components
    log.info(f"***** {app_name} worker {process_index} of {num_processes} start *****")

    # EventCountLogger
    ecl = EventCountLogger(
        platform_config.config.get("event_logging.groups"),
        platform_config.config.get("event_logging.redis"),
    )
    elog = ecl.get_group("te") or DummyEventGroup()
    elog_by_src = ecl.get_group("tasks_by_src") or DummyEventGroup()

    db = EntityDatabase(config, model_spec, num_processes, process_index, elog)
    if process_index == 0:
        db.update_schema()
    else:
        db.await_updated_schema()

    global_scheduler = scheduler.Scheduler()
    task_executor = TaskExecutor(db, platform_config, elog, elog_by_src)
    snap_shooter = SnapShooter(
        db,
        TaskQueueWriter(app_name, num_processes, config.get("processing_core.msg_broker")),
        platform_config,
        global_scheduler,
        elog,
    )
    updater = Updater(
        db,
        TaskQueueWriter(app_name, num_processes, config.get("processing_core.msg_broker")),
        platform_config,
        global_scheduler,
        elog,
    )
    registrar = CallbackRegistrar(global_scheduler, task_executor, snap_shooter, updater)

    LinkManager(db, platform_config, registrar)
    HistoryManager(db, platform_config, registrar)
    Telemetry(db, platform_config, registrar)
    GarbageCollector(db, platform_config, registrar)

    # Lock used to control when the program stops.
    daemon_stop_lock = threading.Lock()
    daemon_stop_lock.acquire()

    # Signal handler releasing the lock on SIGINT or SIGTERM
    def sigint_handler(signum, frame):
        log.debug(
            "Signal {} received, stopping worker".format(
                {signal.SIGINT: "SIGINT", signal.SIGTERM: "SIGTERM"}.get(signum, signum)
            )
        )
        daemon_stop_lock.release()

    signal.signal(signal.SIGINT, sigint_handler)
    signal.signal(signal.SIGTERM, sigint_handler)
    signal.signal(signal.SIGABRT, sigint_handler)

    task_distributor = TaskDistributor(task_executor, platform_config, registrar, daemon_stop_lock)

    control = Control(platform_config)
    control.set_action_handler(ControlAction.make_snapshots, snap_shooter.make_snapshots)
    control.set_action_handler(
        ControlAction.refresh_on_entity_creation,
        partial(refresh_on_entity_creation, task_distributor, task_executor),
    )
    modules = {}
    control.set_action_handler(
        ControlAction.refresh_module_config,
        partial(reload_module_config, log, platform_config, modules),
    )
    global_scheduler.register(control.control_queue.watchdog, second="15,45")

    ##############################################
    # Load all plug-in modules

    module_dir = config.get("processing_core.modules_dir")
    module_dir = os.path.abspath(os.path.join(config_base_path, module_dir))

    loaded_modules = load_modules(
        module_dir,
        config.get("processing_core.enabled_modules"),
        log,
        registrar,
        platform_config,
    )
    modules.update(loaded_modules)

    ################################################
    # Initialization completed, run ...

    # Run update manager thread
    log.info("***** Initialization completed, starting all modules *****")

    # Run modules that have their own threads (TODO: there are no such modules, should be kept?)
    # (if they don't, the start() should do nothing)
    for module in loaded_modules.values():
        module.start()

    core_modules = [
        updater,  # Updater will throw exceptions when misconfigured (best start first)
        task_distributor,  # TaskDistributor (which starts TaskExecutors in several worker threads)
        db,
        snap_shooter,
        control,
        global_scheduler,
    ]
    running_core_modules = []

    try:
        for module in core_modules:
            module.start()
            running_core_modules.append(module)

        # Wait until someone wants to stop the program by releasing this Lock.
        # It may be a user by pressing Ctrl-C or some program module.
        # (try to acquire the lock again,
        # effectively waiting until it's released by signal handler or another thread)
        if os.name == "nt":
            # This is needed on Windows in order to catch Ctrl-C, which doesn't break the waiting.
            while not daemon_stop_lock.acquire(timeout=1):
                pass
        else:
            daemon_stop_lock.acquire()
    except Exception as e:
        log.exception(e)

    ################################################
    # Finalization & cleanup
    # Set signal handlers back to their defaults,
    # so the second Ctrl-C closes the program immediately
    signal.signal(signal.SIGINT, signal.SIG_DFL)
    signal.signal(signal.SIGTERM, signal.SIG_DFL)
    signal.signal(signal.SIGABRT, signal.SIG_DFL)

    log.info("Stopping running components ...")
    for module in reversed(running_core_modules):
        module.stop()

    for module in loaded_modules.values():
        module.stop()

    log.info("***** Finished, main thread exiting. *****")
    logging.shutdown()
