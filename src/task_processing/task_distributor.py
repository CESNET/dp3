import logging
import threading
import queue
from datetime import datetime
import time

from .task_queue import TaskQueueReader, TaskQueueWriter
import src.g as g

ENTITY_TYPES = ['ip', 'asn', 'bgppref', 'ipblock', 'org']


class TaskDistributor:
    def __init__(self, config, process_index, num_processes):
        assert (isinstance(process_index, int) and isinstance(num_processes, int)), "num_processes and process_index " \
                                                                                    "must be int"
        assert (num_processes >= 1), "number of processed muse be positive number"
        assert (0 <= process_index < num_processes), "process index must be smaller than number of processes"

        self.log = logging.getLogger("TaskDistributor")

        self.process_index = process_index
        self.num_processes = num_processes

        self.rabbit_params = config.get('rabbitmq', {})

        self.running = False

        # TODO handler attribute mapping - check after implementation of register_handler()
        # Mapping of names of attributes to a list of functions that should be
        # called when the attribute is updated
        # (One such mapping for each entity type)
        self._attr2func = {etype: {} for etype in ENTITY_TYPES}
        # Set of attributes that may be updated by a function
        self._func2attr = {etype: {} for etype in ENTITY_TYPES}
        # Mapping of functions to set of attributes the function watches, i.e.
        # is called when the attribute is changed
        self._func_triggers = {etype: {} for etype in ENTITY_TYPES}

        # List of worker threads for processing the update requests
        self._worker_threads = []
        self.num_threads = g.config.get('worker_threads', 8)

        # Internal queues for each worker
        self._queues = [queue.Queue(10) for _ in range(self.num_threads)]

        # Connections to main task queue
        # Reader - reads tasks from a pair of queues (one pair per process) and distributes them to worker threads
        self._task_queue_reader = TaskQueueReader(self._distribute_task, self.process_index, self.rabbit_params)
        # Writer - allows modules to write new tasks
        self._task_queue_writer = TaskQueueWriter(self.num_processes, self.rabbit_params)

        # Object to store thread-local data (e.g. worker-thread index) (each thread sees different object contents)
        self._current_thread_data = threading.local()

        # Number of restarts of threads by watchdog
        self._watchdog_restarts = 0
        # Register watchdog to scheduler
        g.scheduler.register(self._watchdog, second="*/30")

    def register_handler(self):
        # TODO
        pass

    def request_update(self, ekey, update_requests):
        """
        Request an update of one or more attributes of an entity record.

        Put given requests into the main task queue to be processed by some of the workers.
        Requests may request changes of some attribute or they may issue events.

        Arguments:
        ekey -- Entity type and key (2-tuple)
        update_requests -- list of update_request n-tuples (see the comments in the beginning of file)
        """
        # Put task to priority queue, so this can never block due to full queue
        self._task_queue_writer.put_task(ekey[0], ekey[1], update_requests, priority=True)

    def start(self):
        """Run the worker threads and start consuming from TaskQueue."""
        self.log.info("Connecting to RabbitMQ")
        self._task_queue_reader.connect()
        self._task_queue_writer.connect()

        self.log.info("Starting {} worker threads".format(self.num_threads))
        self.running = True
        self._worker_threads = [
            threading.Thread(target=self._worker_func, args=(i,), name="Worker-{}-{}".format(self.process_index, i)) for
            i in range(self.num_threads)]
        for worker in self._worker_threads:
            worker.start()

        self.log.info("Starting consuming tasks from main queue")
        self._task_queue_reader.start()

    def stop(self):
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

    def _distribute_task(self, msg_id, etype, eid, updreq):
        """
        Puts given task into local queue of the corresponding thread.

        Called by TaskQueueReader when a new task is received from the global queue.

        :param msg_id: unique ID of the message, used to acknowledge it
        :param etype: entity type (e.g. 'ip', 'asn')
        :param eid: entity identifier (e.g. '1.2.3.4', 2852)
        :param updreq: list of update requests (n-tuples)
        """
        # Distribute tasks to worker threads by hash of (etype,ekey)
        index = hash((etype, eid)) % self.num_threads
        self._queues[index].put((msg_id, etype, eid, updreq))

    def _worker_func(self, thread_index):
        """
        Main worker function.

        Run as a separate thread. Read its local task queue and calls
        "_process_task" function to process each task.

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

            msg_id, etype, eid, updreq = task

            # Acknowledge receipt of the task (regardless of success/failre of its processing)
            self._task_queue_reader.ack(msg_id)

            # Process the task
            # self.log.debug("Processing task {}: {}/{} {}".format(msg_id, etype, eid, updreq))
            start_time = datetime.now()
            try:
                created = self._process_update_req(etype, eid, updreq.copy())
            except Exception:
                self.log.error("Error has occurred during processing task: {}".format(task))
                raise
            duration = (datetime.now() - start_time).total_seconds()
            #self.log.debug("Task {} finished in {:.3f} seconds.".format(msg_id, duration))
            if duration > 1.0:
                self.log.debug("Task {} took {} seconds: {}/{} {}{}".format(msg_id, duration, etype, eid, updreq, " (new record created)" if created else ""))

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

    def _process_update_req(self, etype, eid, update_requests):
        pass
