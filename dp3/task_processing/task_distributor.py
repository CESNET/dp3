import logging
import queue
import threading
import time
from collections import Iterable
from copy import deepcopy
from datetime import datetime
from typing import Callable

from dp3.common.config import HierarchicalDict
from dp3.task_processing.task_executor import TaskExecutor
from .task_queue import TaskQueueReader, TaskQueueWriter
from .. import g


class TaskDistributor:
    def __init__(self, config: HierarchicalDict, process_index: int, num_processes: int,
                 task_executor: TaskExecutor) -> None:
        assert (isinstance(process_index, int) and isinstance(num_processes, int)), "num_processes and process_index " \
                                                                                    "must be int"
        assert (num_processes >= 1), "number of processed muse be positive number"
        assert (0 <= process_index < num_processes), "process index must be smaller than number of processes"

        self.log = logging.getLogger("TaskDistributor")

        self.process_index = process_index
        self.num_processes = num_processes

        self.rabbit_params = config.get('processing_core.msg_broker', {})

        self.entity_types = list(config.get('db_entities').keys()) # List of configured entity types

        self.running = False

        # List of worker threads for processing the update requests
        self._worker_threads = []
        self.num_threads = config.get('processing_core.worker_threads', 8)

        # Internal queues for each worker
        self._queues = [queue.Queue(10) for _ in range(self.num_threads)]

        # Connections to main task queue
        # Reader - reads tasks from a pair of queues (one pair per process) and distributes them to worker threads
        self._task_queue_reader = TaskQueueReader(self._distribute_task, g.app_name, self.process_index, self.rabbit_params)
        # Writer - allows modules to write new tasks
        self._task_queue_writer = TaskQueueWriter(g.app_name, self.num_processes, self.rabbit_params)
        self.task_executor = task_executor
        # Object to store thread-local data (e.g. worker-thread index) (each thread sees different object contents)
        self._current_thread_data = threading.local()

        # Number of restarts of threads by watchdog
        self._watchdog_restarts = 0
        # Register watchdog to scheduler
        g.scheduler.register(self._watchdog, second="*/30")

    def register_handler(self, func: Callable, etype: str, triggers: Iterable[str], changes: Iterable[str]) -> None:
        """
        Hook a function (or bound method) to specified attribute changes/events. Each function must be registered only
        once!
        :param func: function or bound method (callback)
        :param etype: entity type (only changes of attributes of this etype trigger the func)
        :param triggers: set/list/tuple of attributes whose update trigger the call of the method (update of any one of the attributes will do)
        :param changes: set/list/tuple of attributes the method call may update (may be None)
        :return: None
        """
        if etype not in self.entity_types:
            raise ValueError("Unknown entity type '{}'".format(etype))
        # Check types (because common error is to pass string instead of 1-tuple)
        if not isinstance(triggers, Iterable) or isinstance(triggers, str):
            raise TypeError('Argument "triggers" must be iterable and must not be str.')
        if changes is not None and (not isinstance(changes, Iterable) or isinstance(changes, str)):
            raise TypeError('Argument "changes" must be iterable and must not be str.')
        self.task_executor.register_handler(func, etype, triggers, changes)

    def request_update(self, etype="", ekey="", attr_updates=None, events=None, data_points=None, create=None, delete=False, src="", tags=None, ttl_token=None):
        """
        Request an update of one or more attributes of an entity record.

        Put given requests into the main task queue to be processed by some of the workers.
        Requests may request changes of some attribute or they may issue events.

        :param etype: entity type (eg. 'ip')
        :param ekey: entity key
        :param attr_updates: TODO
        :param events: list of events to issue (just plain strings, as event parameters are not needed, may be added in the future if needed)
        :param data_points: list of data points, which will be saved in the database
        :param create: true = create a new record if it doesn't exist yet; false = don't create a record if it
                    doesn't exist (like "weak" in NERD); not set = use global configuration flag "auto_create_record" of the entity type
        :param delete: delete the record
        :param src: name of source module, mostly for logging
        :param tags: tags for logging (number of tasks per time interval by tag)
        :param ttl_token: time to live token
        :return: None
        """
        # Put task to priority queue, so this can never block due to full queue
        self._task_queue_writer.put_task(etype, ekey, attr_updates, events, data_points, create, delete, src, tags, ttl_token, True)

    def start(self) -> None:
        """Run the worker threads and start consuming from TaskQueue."""
        self.log.info("Connecting to RabbitMQ")
        self._task_queue_reader.connect()
        self._task_queue_reader.check()  # check presence of needed queues
        self._task_queue_writer.connect()
        self._task_queue_writer.check()  # check presence of needed exchanges

        self.log.info("Starting {} worker threads".format(self.num_threads))
        self.running = True
        self._worker_threads = [
            threading.Thread(target=self._worker_func, args=(i,), name="Worker-{}-{}".format(self.process_index, i)) for
            i in range(self.num_threads)]
        for worker in self._worker_threads:
            worker.start()

        self.log.info("Starting consuming tasks from main queue")
        self._task_queue_reader.start()

    def stop(self) -> None:
        """
        Stop the manager
        """
        self.log.info("Waiting for worker threads to finish their current tasks ...")
        # Thread for printing debug messages about worker status
        threading.Thread(target=self._dbg_worker_status_print, daemon=True).start()

        # Stop receiving new tasks from global queue
        self._task_queue_reader.stop()

        # Signalize stop to worker threads
        self.running = False

        # Wait until all workers stopped
        for worker in self._worker_threads:
            worker.join()

        self._task_queue_reader.disconnect()
        self._task_queue_writer.disconnect()

        # Cleanup
        self._worker_threads = []

    def _distribute_task(self, msg_id, etype, ekey, attr_updates, events, data_points, create, delete, src, tags, ttl_token):
        """
        Puts given task into local queue of the corresponding thread.

        Called by TaskQueueReader when a new task is received from the global queue.

        :param msg_id: unique ID of the message, used to acknowledge it
        :param etype: entity key (e.g. 'ip', 'asn')
        :param ekey: entity identifier (e.g. '1.2.3.4', 2852)
        :param attr_updates: list of update requests (n-tuples)
        """
        # Distribute tasks to worker threads by hash of (etype,ekey)
        index = hash((etype, ekey)) % self.num_threads
        self._queues[index].put((msg_id, etype, ekey, attr_updates, events, data_points, create, delete, src, tags, ttl_token))

    def _worker_func(self, thread_index):
        """
        Main worker function.

        Run as a separate thread. Read its local task queue and calls
        processing function of TaskExecutor to process each task.

        Tasks are assigned to workers based on hash of entity key, so each
        entity is always processed by the same worker. Therefore, all requests
        modifying a particular entity are done sequentially and no locking is
        necessary.
        """
        # Store index to thread-local variable
        self._current_thread_data.index = thread_index

        my_queue = self._queues[thread_index]

        # Read and process tasks in a loop.
        # Exit immediately after self.running is set to False, it's not a problem if there are any more tasks waiting
        # in the queue - they won't be acknowledged so they will be re-delivered after restart.
        while self.running:
            # Get message from thread's local queue
            try:
                task = my_queue.get(block=True, timeout=1)
            except queue.Empty:
                continue # check self.running again

            # * means fill the rest (to prevent ValueError - too many values to unpack)
            msg_id, etype, eid, attr_updates, *_ = task

            # Acknowledge receipt of the task (regardless of success/failre of its processing)
            self._task_queue_reader.ack(msg_id)

            # Process the task
            start_time = datetime.now()
            try:
                # [1:] because process_task() doesn't need msg_id
                created = self.task_executor.process_task(deepcopy(task[1:]))
            except Exception:
                self.log.error("Error has occurred during processing task: {}".format(task))
                raise
            duration = (datetime.now() - start_time).total_seconds()
            #self.log.debug("Task {} finished in {:.3f} seconds.".format(msg_id, duration))
            if duration > 1.0:
                self.log.debug("Task {} took {} seconds: {}/{} {}{}".format(msg_id, duration, etype, eid, attr_updates, " (new record created)" if created else ""))

    def _watchdog(self):
        """
        Check whether all workers are running and restart them if not.

        Should be called periodically by scheduler.
        Stop whole program after 20 restarts of threads.
        """
        for i, worker in enumerate(self._worker_threads):
            if not worker.is_alive():
                if self._watchdog_restarts < 20:
                    self.log.error("Thread {} is dead, restarting.".format(worker.name))
                    worker.join()
                    new_thread = threading.Thread(target=self._worker_func, args=(i,), name="Worker-{}-{}".format(self.process_index, i))
                    self._worker_threads[i] = new_thread
                    new_thread.start()
                    self._watchdog_restarts += 1
                else:
                    self.log.critical("Thread {} is dead, more than 20 restarts attempted, giving up...".format(worker.name))
                    g.daemon_stop_lock.release()  # Exit program
                    break

    def _dbg_worker_status_print(self):
        """
        Print status of workers and the request queue every 5 seconds.

        Should be run as a separate (daemon) thread.
        Exits when all workers has finished.
        """
        ttl = 10  # Wait for 10 seconds until printing starts
        while True:
            # Check if all workers are dead every second
            time.sleep(1)
            ttl -= 1
            alive_workers = [w for w in self._worker_threads if w.is_alive()]
            if not alive_workers:
                return

            if ttl == 0:
                # Print info and reset counter to 5 seconds
                self.log.info("{} worker threads alive".format(len(alive_workers)))
                ttl = 5
