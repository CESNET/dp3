#!/usr/bin/env python3
import logging
from importlib import import_module
import os
import inspect
import threading
import signal

from common.config import read_config_dir, load_attr_spec
from common import scheduler
from database.database import EntityDatabase
from task_processing.task_executor import TaskExecutor
from task_processing.task_distributor import TaskDistributor
import g

MODULES_FOLDER = os.path.abspath(os.path.join(os.path.dirname(os.path.abspath(__file__)), 'modules'))
MODULES_IMPORT_PATH = "modules" if os.getcwd().endswith("src") else "src.modules"
BASE_MODULE_CLASS_NAME = "BaseModule"


def load_modules(module_names_list):
    # [:-3] is for removing '.py' suffix from module filenames
    available_modules = [filename[:-3] for filename in os.listdir(os.path.join(os.getcwd(), MODULES_FOLDER)) if
                         filename.endswith(".py")]

    # check if all desired modules are in modules folder
    missing_modules = (set(module_names_list) - set(available_modules))
    assert not missing_modules, f"some of desired modules are not available (not in modules folder), specifically: " \
                                f"{missing_modules}"
    # do imports of desired modules from 'modules' folder
    imported_modules = [import_module(MODULES_IMPORT_PATH + "." + module_name) for module_name in module_names_list]
    # final list will contain main classes from all desired modules, which has BaseModule as parent
    modules_main_objects = []
    for module in imported_modules:
        for _, obj in inspect.getmembers(module):
            if inspect.isclass(obj):
                for class_base in obj.__bases__:
                    if class_base.__name__ == BASE_MODULE_CLASS_NAME:
                        # append instance of module class (obj is class --> obj() is instance) --> call init, which
                        # registers handler
                        modules_main_objects.append(obj())

    return modules_main_objects


def main(cfg_dir, process_index, verbose):
    ##############################################
    # Initialize logging mechanism
    LOGFORMAT = "%(asctime)-15s,%(threadName)s,%(name)s,[%(levelname)s] %(message)s"
    LOGDATEFORMAT = "%Y-%m-%dT%H:%M:%S"

    logging.basicConfig(level=logging.DEBUG if verbose else logging.INFO, format=LOGFORMAT, datefmt=LOGDATEFORMAT)
    log = logging.getLogger()

    # Disable INFO and DEBUG messages from requests.urllib3 library, which is used by some modules
    logging.getLogger("requests").setLevel(logging.WARNING)
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("amqpstorm").setLevel(logging.WARNING)

    ##############################################
    # Load configuration
    log.debug(f"Loading config file {cfg_dir}")

    # whole configuration should be loaded
    config = read_config_dir(cfg_dir, recursive=True)
    attr_spec = load_attr_spec(config["db_entities"])

    num_processes = config['processing_core'].get('worker_processes')
    assert (isinstance(num_processes,
                       int) and num_processes > 0), "Number of processes ('num_processes' in config) must be a positive integer"
    assert (isinstance(process_index, int) and process_index >= 0), "Process index can't be negative"
    assert (process_index < num_processes), "Process index must be less than total number of processes"

    ##############################################
    # Create instances of core components
    # Save them to "g" ("global") module so they can be easily accessed from everywhere
    log.info("***** Worker {}/{} start *****".format(process_index, num_processes))

    g.config = config
    g.config_base_path = os.path.dirname(os.path.abspath(cfg_dir))
    g.scheduler = scheduler.Scheduler()
    g.db = EntityDatabase(config["database"], attr_spec)
    te = TaskExecutor(g.db, attr_spec)
    g.td = TaskDistributor(config, process_index, num_processes, te)

    ##############################################
    # Load all plug-in modules

    module_list = load_modules(config['processing_core']['enabled_modules'])

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

    # start TaskExecutor
    g.td.start()

    # Run scheduler
    g.scheduler.start()

    # Wait until someone wants to stop the program by releasing this Lock.
    # It may be a user by pressing Ctrl-C or some program module.
    # (try to acquire the lock again, effectively waiting until it's released by signal handler or another thread)
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
    g.td.stop()
    for module in module_list:
        module.stop()

    log.info("***** Finished, main thread exiting. *****")
    logging.shutdown()


if __name__ == "__main__":
    import argparse

    # Parse arguments
    parser = argparse.ArgumentParser(
        prog="worker.py",
        description="Main worker process of the processing platform. There are usually multiple workers running in "
                    "parallel. "
    )
    parser.add_argument('process_index', metavar='INDEX', type=int,
        help='Index of the worker process')
    parser.add_argument('-c', '--config', metavar='DIRECTORY_NAME', default='/etc/adict/config',
        help='Path to configuration directory (default: /etc/adict/config)')
    parser.add_argument('-v', '--verbose', action="store_true", help="Verbose mode", default=False)
    args = parser.parse_args()

    # Run main code
    main(args.config, args.process_index, args.verbose)
