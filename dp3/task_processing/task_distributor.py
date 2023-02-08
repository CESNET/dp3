import logging
import queue
import threading
import time
from datetime import datetime

from dp3.common.config import HierarchicalDict, ModelSpec
from dp3.common.task import Task
from dp3.task_processing.task_executor import TaskExecutor

from .. import g
from .task_queue import TaskQueueReader, TaskQueueWriter


class TaskDistributor:
    def __init__(
        self,
        config: HierarchicalDict,
        process_index: int,
        num_processes: int,
        task_executor: TaskExecutor,
        model_spec: ModelSpec,
    ) -> None:
        assert isinstance(process_index, int) and isinstance(num_processes, int), (
            "num_processes and process_index " "must be int"
        )
        assert num_processes >= 1, "number of processed muse be positive number"
        assert (
            0 <= process_index < num_processes
        ), "process index must be smaller than number of processes"

        self.log = logging.getLogger("TaskDistributor")

        self.process_index = process_index
        self.num_processes = num_processes
        self.model_spec = model_spec

        self.rabbit_params = config.get("processing_core.msg_broker", {})

        self.entity_types = list(
            config.get("db_entities").keys()
        )  # List of configured entity types

        self.running = False

        # List of worker threads for processing the update requests
        self._worker_threads = []
        self.num_threads = config.get("processing_core.worker_threads", 8)

        # Internal queues for each worker
        self._queues = [queue.Queue(10) for _ in range(self.num_threads)]

        # Connections to main task queue
        # Reader - reads tasks from a pair of queues (one pair per process)
        # and distributes them to worker threads
        self._task_queue_reader = TaskQueueReader(
            self._distribute_task,
            g.app_name,
            self.model_spec,
            self.process_index,
            self.rabbit_params,
        )
        # Writer - allows modules to write new tasks
        self._task_queue_writer = TaskQueueWriter(
            g.app_name, self.num_processes, self.rabbit_params
        )
        self.task_executor = task_executor
        # Object to store thread-local data (e.g. worker-thread index)
        # (each thread sees different object contents)
        self._current_thread_data = threading.local()

        # Number of restarts of threads by watchdog
        self._watchdog_restarts = 0
        # Register watchdog to scheduler
        g.scheduler.register(self._watchdog, second="*/30")

    def start(self) -> None:
        """Run the worker threads and start consuming from TaskQueue."""
        self.log.info("Connecting to RabbitMQ")
        self._task_queue_reader.connect()
        self._task_queue_reader.check()  # check presence of needed queues
        self._task_queue_writer.connect()
        self._task_queue_writer.check()  # check presence of needed exchanges

        self.log.info(f"Starting {self.num_threads} worker threads")
        self.running = True
        self._worker_threads = [
            threading.Thread(
                target=self._worker_func, args=(i,), name=f"Worker-{self.process_index}-{i}"
            )
            for i in range(self.num_threads)
        ]
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

    def _distribute_task(self, msg_id, task: Task):
        """
        Puts given task into local queue of the corresponding thread.

        Called by TaskQueueReader when a new task is received from the global queue.
        """
        # Distribute tasks to worker threads by hash of (etype,ekey)
        index = hash((task.etype, task.ekey)) % self.num_threads
        self._queues[index].put((msg_id, task))

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
        # Exit immediately after self.running is set to False,
        # it's not a problem if there are any more tasks waiting in the queue
        # - they won't be acknowledged so they will be re-delivered after restart.
        while self.running:
            # Get message from thread's local queue
            try:
                task_tuple = my_queue.get(block=True, timeout=1)
            except queue.Empty:
                continue  # check self.running again

            msg_id, task = task_tuple

            # Acknowledge receipt of the task (regardless of success/failre of its processing)
            self._task_queue_reader.ack(msg_id)

            # Process the task
            start_time = datetime.now()
            try:
                created, new_tasks = self.task_executor.process_task(task)
            except Exception:
                self.log.error(f"Error has occurred during processing task: {task}")
                raise

            duration = (datetime.now() - start_time).total_seconds()
            # self.log.debug("Task {} finished in {:.3f} seconds.".format(msg_id, duration))
            if duration > 1.0:
                self.log.debug(
                    "Task {} took {} seconds: {}/{} {}".format(
                        msg_id,
                        duration,
                        task.etype,
                        task.eid,
                        " (new record created)" if created else "",
                    )
                )

            # Push tasks
            try:
                for task in new_tasks:
                    self._task_queue_writer.put_task(task)
            except Exception as e:
                self.log.error(f"Failed to push tasks created from hooks: {e}")

    def _watchdog(self):
        """
        Check whether all workers are running and restart them if not.

        Should be called periodically by scheduler.
        Stop whole program after 20 restarts of threads.
        """
        for i, worker in enumerate(self._worker_threads):
            if not worker.is_alive():
                if self._watchdog_restarts < 20:
                    self.log.error(f"Thread {worker.name} is dead, restarting.")
                    worker.join()
                    new_thread = threading.Thread(
                        target=self._worker_func, args=(i,), name=f"Worker-{self.process_index}-{i}"
                    )
                    self._worker_threads[i] = new_thread
                    new_thread.start()
                    self._watchdog_restarts += 1
                else:
                    self.log.critical(
                        f"Thread {worker.name} is dead,"
                        " more than 20 restarts attempted, giving up..."
                    )
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
                self.log.info(f"{len(alive_workers)} worker threads alive")
                ttl = 5
