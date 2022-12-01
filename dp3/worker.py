#!/usr/bin/env python3
"""Code of the main worker process.

Don't run directly. Import and run the main() function.
"""
import inspect
import logging
from importlib import import_module
import sys
import os
import signal
import threading
from importlib import import_module

if __name__ == "__main__":
    import sys
    print("Don't run this file directly. Use 'bin/worker' instead.", file=sys.stderr)
    sys.exit(1)

from .common.config import read_config_dir, load_attr_spec
from .common import scheduler
#from .common.base_module import BaseModule
from .database.database import EntityDatabase
from .task_processing.task_executor import TaskExecutor
from .task_processing.task_distributor import TaskDistributor
#from .history_management.history_manager import HistoryManager
from . import g


# def load_modules(modules_dir: str, enabled_modules: dict, log: logging.RootLogger) -> list:
#     """Load plug-in modules

#     Import Python modules with names in 'enabled_modules' from 'modules_dir' directory and return all found classes
#     derived from BaseModule class.
#     """
#     # Get list of all modules available in given folder
#     # [:-3] is for removing '.py' suffix from module filenames
#     available_modules = []
#     for item in os.scandir(modules_dir):
#         # A module can be a Python file or a Python package (i.e. a directory with "__init__.py" file)
#         if item.is_file() and item.name.endswith(".py"):
#             available_modules.append(item.name[:-3]) # name without .py
#         if item.is_dir() and "__init__.py" in os.listdir(os.path.join(modules_dir, item.name)):
#             available_modules.append(item.name)
#     log.debug(f"Available modules: {', '.join(available_modules)}")
#     log.debug(f"Enabled modules: {', '.join(enabled_modules)}")

#     # check if all desired modules are in modules folder
#     missing_modules = (set(enabled_modules) - set(available_modules))
#     if missing_modules:
#         log.fatal(f"Some of desired modules are not available (not in modules folder), specifically: {missing_modules}")
#         sys.exit(2)
#     # do imports of desired modules from 'modules' folder
#     # (rewrite sys.path to modules_dir, import all modules and rewrite it back)
#     log.debug("Importing modules ...")
#     sys.path.insert(0, modules_dir)
#     imported_modules = [import_module(module_name) for module_name in enabled_modules]
#     del sys.path[0]
#     # final list will contain main classes from all desired modules, which has BaseModule as parent
#     modules_main_objects = []
#     for module in imported_modules:
#         for _, obj in inspect.getmembers(module):
#             if inspect.isclass(obj) and BaseModule in obj.__bases__:
#                 # append instance of module class (obj is class --> obj() is instance) --> call init, which
#                 # registers handler
#                 modules_main_objects.append(obj())
#                 log.info(f"Module loaded: {module.__name__}:{obj.__name__}")

#     return modules_main_objects


def main(app_name: str, config_dir: str, process_index: int, verbose: bool) -> None:
    """
    Run worker process.

    :param app_name: Name of the application to distinct it from other
        DP3-based apps. For example, it's used as a prefix for RabbitMQ queue
        names.
    :param config_dir: Path to directory containing configuration files.
    :param process_index: Index of this worker process. For each application
        there must be N processes running simultaneously, each started with a
        unique index (from 0 to N-1). N is read from configuration
        ('worker_processes' in 'processing_core.yml').
    :param verbose: More verbose output (set log level to DEBUG).
    """
    ##############################################
    # Initialize logging mechanism
    LOGFORMAT = "%(asctime)-15s,%(threadName)s,%(name)s,[%(levelname)s] %(message)s"
    LOGDATEFORMAT = "%Y-%m-%dT%H:%M:%S"

    logging.basicConfig(level=logging.DEBUG if verbose else logging.INFO, format=LOGFORMAT, datefmt=LOGDATEFORMAT)
    log = logging.getLogger()

    # Disable INFO and DEBUG messages from some libraries
    logging.getLogger("requests").setLevel(logging.WARNING)
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("amqpstorm").setLevel(logging.WARNING)

    ##############################################
    # Load configuration
    g.config_base_path = os.path.abspath(config_dir)
    log.debug(f"Loading config directory {g.config_base_path}")

    # Whole configuration should be loaded
    config = read_config_dir(g.config_base_path, recursive=True)
    attr_spec = load_attr_spec(config.get("db_entities"))

    print(attr_spec)

    num_processes = config.get('processing_core.worker_processes')
    assert (isinstance(num_processes, int) and num_processes > 0),\
        "Number of processes ('num_processes' in config) must be a positive integer"
    assert (isinstance(process_index, int) and process_index >= 0), "Process index can't be negative"
    assert (process_index < num_processes), "Process index must be less than total number of processes"

    ##############################################
    # Create instances of core components
    # Save them to "g" ("global") module so they can be easily accessed from everywhere (in the same process)
    log.info("***** {} worker {} of {} start *****".format(app_name, process_index, num_processes))

    g.app_name = app_name
    g.config = config
    g.attr_spec = attr_spec
    g.running = False
    g.scheduler = scheduler.Scheduler()
    g.db = EntityDatabase(config.get("database"), attr_spec)
    #g.hm = HistoryManager(g.db, attr_spec, process_index, num_processes, config.get("history_manager"))
    #te = TaskExecutor(g.db, attr_spec)
    #g.td = TaskDistributor(config, process_index, num_processes, te)

    ##############################################
    # Load all plug-in modules

    working_directory = os.path.dirname(__file__) 
    core_modules_dir = os.path.abspath(os.path.join(working_directory, 'core_modules'))
    custom_modules_dir = config.get('processing_core.modules_dir')
    custom_modules_dir = os.path.abspath(os.path.join(g.config_base_path, custom_modules_dir))

    #core_module_list = load_modules(core_modules_dir, {}, log)
    #custom_module_list = load_modules(custom_modules_dir, config.get('processing_core.enabled_modules'), log)

    module_list = []#core_module_list + custom_module_list

    # Lock used to control when the program stops.
    g.daemon_stop_lock = threading.Lock()
    g.daemon_stop_lock.acquire()

    # Signal handler releasing the lock on SIGINT or SIGTERM
    def sigint_handler(signum, frame):
        log.debug("Signal {} received, stopping worker".format(
            {signal.SIGINT: "SIGINT", signal.SIGTERM: "SIGTERM"}.get(signum, signum)))
        g.daemon_stop_lock.release()

    signal.signal(signal.SIGINT, sigint_handler)
    signal.signal(signal.SIGTERM, sigint_handler)
    signal.signal(signal.SIGABRT, sigint_handler)

    ################################################
    # Initialization completed, run ...

    # Run update manager thread
    log.info("***** Initialization completed, starting all modules *****")
    g.running = True

    # Run modules that have their own threads (TODO: there are no such modules, should be kept?)
    # (if they don't, the start() should do nothing)
    for module in module_list:
        module.start()

    # start TaskDistributor (which starts TaskExecutors in several worker threads)
    #g.td.start()

    # Run scheduler
    g.scheduler.start()

    # Wait until someone wants to stop the program by releasing this Lock.
    # It may be a user by pressing Ctrl-C or some program module.
    # (try to acquire the lock again, effectively waiting until it's released by signal handler or another thread)
    if os.name == "nt":
        # This is needed on Windows in order to catch Ctrl-C, which doesn't break the waiting.
        while not g.daemon_stop_lock.acquire(timeout=1):
            pass
    else:
        g.daemon_stop_lock.acquire()


    ################################################
    # Finalization & cleanup
    # Set signal handlers back to their defaults, so the second Ctrl-C closes the program immediately
    signal.signal(signal.SIGINT, signal.SIG_DFL)
    signal.signal(signal.SIGTERM, signal.SIG_DFL)
    signal.signal(signal.SIGABRT, signal.SIG_DFL)

    log.info("Stopping running components ...")
    g.running = False
    g.scheduler.stop()
    #g.td.stop()
    for module in module_list:
        module.stop()

    log.info("***** Finished, main thread exiting. *****")
    logging.shutdown()
