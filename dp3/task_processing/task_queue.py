"""
Functions to work with the main task queue (RabbitMQ)

There are two queues for each worker process:
- "normal" queue for tasks added by other components, this has a limit of 100
  tasks.
- "priority" one for tasks added by workers themselves, this has no limit since
  workers mustn't be stopped by waiting for the queue.

These queues are presented as a single one by this wrapper.
The TaskQueueReader first looks into the "priority" queue and only if there
is no task waiting, it reads the normal one.

Tasks are distributed to worker processes (and threads) by hash of the entity
which is to be modified. The destination queue is decided by the message source,
so each source must know how many worker processes are there.

Exchange and queues must be declared externally!

Related configuration keys and their defaults:
(should be part of global DP3 config files)
```
rabbitmq:
  host: localhost
  port: 5672
  virtual_host: /
  username: guest
  password: guest

worker_processes: 1
```
"""

import collections
import contextlib
import logging
import threading
import time
from typing import Callable, Union

import amqpstorm

from dp3.common.task import Task

# Exchange and queue names
# They must be pre-declared ('direct' exchange type) and binded.
# The names are prefixed by app_name
# Numbers from 0 to number_of_workers-1 are used as routing/binding keys.
DEFAULT_EXCHANGE = "{}-main-task-exchange"
DEFAULT_PRIORITY_EXCHANGE = "{}-priority-task-exchange"
DEFAULT_QUEUE = "{}-worker-{}"
DEFAULT_PRIORITY_QUEUE = "{}-worker-{}-pri"


# When reading, pre-fetch only a limited amount of messages
# (because pre-fetched messages are not counted to queue length limit)
PREFETCH_COUNT = 50

# number of seconds to wait for the i-th attempt to reconnect after error
RECONNECT_DELAYS = [1, 2, 5, 10, 30]


class QueueNotDeclared(RuntimeError):
    def __init__(self, queue_name):
        self.queue_name = queue_name

    def __str__(self):
        return (
            f"Queue '{self.queue_name}' is not declared in RabbitMQ! "
            "Run 'scripts/rmq_reconfigure.sh' to set up "
            "needed exchanges and queues on RabbitMQ server."
        )


class ExchangeNotDeclared(RuntimeError):
    def __init__(self, exchange_name):
        self.exchange_name = exchange_name

    def __str__(self):
        return (
            f"Exchange '{self.exchange_name}' is not declared in RabbitMQ! "
            f"Run 'scripts/rmq_reconfigure.sh' to set up "
            f"needed exchanges and queues on RabbitMQ server."
        )


class RobustAMQPConnection:
    """
    Common TaskQueue wrapper, handles connection to RabbitMQ server with automatic reconnection.
    TaskQueueWriter and TaskQueueReader are derived from this.

    Args:
        rabbit_config: RabbitMQ connection parameters, dict with following keys (all optional):
            host, port, virtual_host, username, password
    """

    def __init__(self, rabbit_config: dict = None) -> None:
        rabbit_config = {} if rabbit_config is None else rabbit_config
        self.log = logging.getLogger("RobustAMQPConnection")
        self.conn_params = {
            "hostname": rabbit_config.get("host", "localhost"),
            "port": int(rabbit_config.get("port", 5672)),
            "virtual_host": rabbit_config.get("virtual_host", "/"),
            "username": rabbit_config.get("username", "guest"),
            "password": rabbit_config.get("password", "guest"),
        }
        self.connection: amqpstorm.Connection = None
        self.channel: amqpstorm.Channel = None
        self._connection_id = 0

    def __del__(self):
        self.disconnect()

    def connect(self) -> None:
        """Create a connection (or reconnect after error).

        If connection can't be established, try it again indefinitely.
        """
        if self.connection:
            self.connection.close()
        self._connection_id += 1

        attempts = 0
        while True:
            attempts += 1
            try:
                self.connection = amqpstorm.Connection(**self.conn_params)
                self.log.debug(
                    "AMQP connection created, server: "
                    "'{hostname}:{port}/{virtual_host}'".format_map(self.conn_params)
                )
                if attempts > 1:
                    # This was a repeated attempt, print success message with ERROR level
                    self.log.error("... it's OK now, we're successfully connected!")

                self.channel = self.connection.channel()
                self.channel.confirm_deliveries()
                self.channel.basic.qos(PREFETCH_COUNT)
                break
            except amqpstorm.AMQPError as e:
                sleep_time = RECONNECT_DELAYS[min(attempts, len(RECONNECT_DELAYS)) - 1]
                self.log.error(
                    f"RabbitMQ connection error (will try to reconnect in {sleep_time}s): {e}"
                )
                time.sleep(sleep_time)
            except KeyboardInterrupt:
                break

    def disconnect(self) -> None:
        if self.connection:
            self.connection.close()
        self.connection = None
        self.channel = None

    def check_queue_existence(self, queue_name: str) -> bool:
        if queue_name is None:
            return True
        assert self.channel is not None, "not connected"
        try:
            self.channel.queue.declare(queue_name, passive=True)
        except amqpstorm.AMQPChannelError:
            return False
        return True

    def check_exchange_existence(self, exchange_name: str) -> bool:
        assert self.channel is not None, "not connected"
        try:
            self.channel.exchange.declare(exchange_name, passive=True)
        except amqpstorm.AMQPChannelError:
            return False
        return True


class TaskQueueWriter(RobustAMQPConnection):
    """
    Writes tasks into main Task Queue

    Args:
        app_name: DP3 application name (used as prefix for RMQ queues and exchanges)
        workers: Number of worker processes in the system
        rabbit_config: RabbitMQ connection parameters, dict with following keys (all optional):
            host, port, virtual_host, username, password
        exchange: Name of the exchange to write tasks to
            (default: `"<app-name>-main-task-exchange"`)
        priority_exchange: Name of the exchange to write priority tasks to
            (default: `"<app-name>-priority-task-exchange"`)
        parent_logger: Logger to inherit prefix from.
    """

    def __init__(
        self,
        app_name: str,
        workers: int = 1,
        rabbit_config: dict = None,
        exchange: str = None,
        priority_exchange: str = None,
        parent_logger: logging.Logger = None,
    ) -> None:
        rabbit_config = {} if rabbit_config is None else rabbit_config
        assert isinstance(workers, int) and workers >= 1, "count of workers must be positive number"
        assert isinstance(exchange, str) or exchange is None, "exchange argument has to be string!"
        assert (
            isinstance(priority_exchange, str) or priority_exchange is None
        ), "priority_exchange has to be string"

        super().__init__(rabbit_config)

        if parent_logger is not None:
            self.log = parent_logger.getChild("TaskQueueWriter")
        else:
            self.log = logging.getLogger("TaskQueueWriter")

        if exchange is None:
            exchange = DEFAULT_EXCHANGE.format(app_name)
        if priority_exchange is None:
            priority_exchange = DEFAULT_PRIORITY_EXCHANGE.format(app_name)

        self.workers = workers
        self.exchange = exchange
        self.exchange_pri = priority_exchange

    def check(self) -> bool:
        """
        Check that needed exchanges are declared, return True or raise RuntimeError.

        If needed exchanges are not declared, reconnect and try again. (max 5 times)
        """
        for attempt, sleep_time in enumerate(RECONNECT_DELAYS):
            if self.check_exchange_existence(self.exchange) and self.check_exchange_existence(
                self.exchange_pri
            ):
                return True
            self.log.warning(
                "RabbitMQ exchange configuration doesn't match (attempt %d of %d, retrying in %ds)",
                attempt + 1,
                len(RECONNECT_DELAYS),
                sleep_time,
            )
            time.sleep(sleep_time)
            self.disconnect()
            self.connect()
        if not self.check_exchange_existence(self.exchange):
            raise ExchangeNotDeclared(self.exchange)
        if not self.check_exchange_existence(self.exchange_pri):
            raise ExchangeNotDeclared(self.exchange_pri)
        return True

    def broadcast_task(self, task: Task, priority: bool = False) -> None:
        """
        Broadcast task to all workers

        Args:
            task: prepared task
            priority: if true, the task is placed into priority queue
                (should only be used internally by workers)
        """
        if not self.channel:
            self.connect()

        task_str = str(task) if len(str(task)) < 500 else f"{str(task)[:500]}...(truncated)"
        self.log.debug(f"Received new broadcast task: {task_str}")

        body = task.as_message()
        exchange = self.exchange_pri if priority else self.exchange

        for routing_key in range(self.workers):
            self._send_message(routing_key, exchange, body)

    def put_task(self, task: Task, priority: bool = False) -> None:
        """
        Put task (update_request) to the queue of corresponding worker

        Args:
            task: prepared task
            priority: if true, the task is placed into priority queue
                (should only be used internally by workers)
        """
        if not self.channel:
            self.connect()

        task_str = str(task) if len(str(task)) < 500 else f"{str(task)[:500]}...(truncated)"
        self.log.debug(f"Received new task: {task_str}")

        # Prepare routing key
        body = task.as_message()
        # index of the worker to send the task to
        routing_key = task.hashed_routing_key() % self.workers

        exchange = self.exchange_pri if priority else self.exchange
        self._send_message(routing_key, exchange, body)

    def _send_message(self, routing_key, exchange, body):
        """Send the message."""

        # ('mandatory' flag means that we want to guarantee it's delivered to someone.
        # If it can't be delivered (no consumer or full queue), wait a while and try again.
        # Always print just one error message for each unsuccessful message to be send.)
        self.log.debug(f"Sending a task with routing_key={routing_key} to exchange '{exchange}'")
        err_printed = 0
        while True:
            try:
                message = amqpstorm.Message.create(self.channel, body)
                success = message.publish(str(routing_key), exchange, mandatory=True)
                if success:  # message ACK'd
                    if err_printed == 1:
                        self.log.debug("... it's OK now, the message was successfully sent")
                    elif err_printed == 2:
                        self.log.warning("... it's OK now, the message was successfully sent")
                    else:
                        self.log.debug("Message successfully sent")
                    break
                else:  # message NACK'd
                    if err_printed != 1:
                        self.log.debug(
                            f"Message rejected (queue of worker {routing_key} is probably full), "
                            "will retry every 100ms"
                        )
                        err_printed = 1
                    time.sleep(0.1)
            except amqpstorm.AMQPChannelError as e:
                if err_printed != 2:
                    self.log.warning(
                        f"Can't deliver a message to worker {routing_key} "
                        f"(will retry every 5 seconds): {e}"
                    )
                    err_printed = 2
                time.sleep(5)
            except amqpstorm.AMQPConnectionError as e:
                self.log.error(f"RabbitMQ connection error (will try to reconnect): {e}")
                self.connect()


class TaskQueueReader(RobustAMQPConnection):
    """
    TaskQueueReader consumes messages from two RabbitMQ queues
    (normal and priority one for given worker)
    and passes them to the given callback function.

    Tasks from the priority queue are passed before the normal ones.

    Each received message must be acknowledged by calling `.ack(msg_tag)`.

    Args:
        callback: Function called when a message is received, prototype: func(tag, Task)
        parse_task: Function called to parse message body into a task, prototype: func(body) -> Task
        app_name: DP3 application name (used as prefix for RMQ queues and exchanges)
        worker_index: index of this worker
            (filled into DEFAULT_QUEUE string using .format() method)
        rabbit_config: RabbitMQ connection parameters, dict with following keys
            (all optional): host, port, virtual_host, username, password
        queue: Name of RabbitMQ queue to read from (default: `"<app-name>-worker-<index>"`)
        priority_queue: Name of RabbitMQ queue to read from (priority messages)
            or `False` to disable. (default: `"<app-name>-worker-<index>-pri"`)
        parent_logger: Logger to inherit prefix from.
    """

    def __init__(
        self,
        callback: Callable,
        parse_task: Callable[[str], Task],
        app_name: str,
        worker_index: int = 0,
        rabbit_config: dict = None,
        queue: str = None,
        priority_queue: Union[str, bool] = None,
        parent_logger: logging.Logger = None,
    ) -> None:
        rabbit_config = {} if rabbit_config is None else rabbit_config
        assert callable(callback), "callback must be callable object"
        assert (
            isinstance(worker_index, int) and worker_index >= 0
        ), "worker_index must be positive number"
        assert isinstance(queue, str) or queue is None, "queue must be string"
        assert (
            isinstance(priority_queue, str) or priority_queue is None or priority_queue is False
        ), "priority_queue must be string or False to disable"

        super().__init__(rabbit_config)

        if parent_logger is not None:
            self.log = parent_logger.getChild("TaskQueueReader")
        else:
            self.log = logging.getLogger("TaskQueueReader")

        self.callback = callback
        self.parse_task = parse_task

        if queue is None:
            queue = DEFAULT_QUEUE.format(app_name, worker_index)
        if priority_queue is None:
            priority_queue = DEFAULT_PRIORITY_QUEUE.format(app_name, worker_index)
        elif priority_queue is False:
            priority_queue = None
        self.queue_name = queue
        self.priority_queue_name = priority_queue
        self.worker_index = worker_index

        self.running = False

        self._consuming_thread = None
        self._processing_thread = None

        # Receive messages into 2 temporary queues
        # (max length should be equal to prefetch_count set in RabbitMQReader)
        self.cache = collections.deque()
        self.cache_pri = collections.deque()
        self.cache_full = threading.Event()  # signalize there's something in the cache

    def __del__(self):  # TODO is this needed?
        # self.log.debug("Destructor called")
        if hasattr(self, "_consuming_thread"):
            self._stop_consuming_thread()
        if hasattr(self, "_processing_thread"):
            self._stop_processing_thread()
        super().__del__()

    def start(self) -> None:
        """Start receiving tasks."""
        if self.running:
            raise RuntimeError("Already running")

        if not self.connection:
            self.connect()

        self.log.info("Starting TaskQueueReader")

        # Start thread for message consuming from server
        self.running = True
        self._consuming_thread = threading.Thread(target=self._consuming_thread_func)
        thread_n = self._consuming_thread.name.split("-")[-1]
        self._consuming_thread.name = f"Consumer-{self.worker_index}-{thread_n}"
        self._consuming_thread.start()

        # Start thread for message processing and passing to user's callback
        self._processing_thread = threading.Thread(target=self._msg_processing_thread_func)
        thread_n = self._processing_thread.name.split("-")[-1]
        self._processing_thread.name = f"Processor-{self.worker_index}-{thread_n}"
        self._processing_thread.start()

    def stop(self) -> None:
        """Stop receiving tasks."""
        if not self.running:
            raise RuntimeError("Not running")

        self.running = False
        self._stop_consuming_thread()
        self._stop_processing_thread()
        self.log.info("TaskQueueReader stopped")

    def reconnect(self) -> None:
        """Clear local message cache and reconnect to RabbitMQ server."""
        self.cache.clear()
        self.cache_pri.clear()

        self.connect()

    def check(self) -> bool:
        """
        Check that needed queues are declared, return True or raise RuntimeError.

        If needed queues are not declared, reconnect and try again. (max 5 times)
        """

        for attempt, sleep_time in enumerate(RECONNECT_DELAYS):
            if self.check_queue_existence(self.queue_name) and self.check_queue_existence(
                self.priority_queue_name
            ):
                return True
            self.log.warning(
                "RabbitMQ queue configuration doesn't match (attempt %d of %d, retrying in %ds)",
                attempt + 1,
                len(RECONNECT_DELAYS),
                sleep_time,
            )
            time.sleep(sleep_time)
            self.disconnect()
            self.connect()
        if not self.check_queue_existence(self.queue_name):
            raise QueueNotDeclared(self.queue_name)
        if not self.check_queue_existence(self.priority_queue_name):
            raise QueueNotDeclared(self.priority_queue_name)
        return True

    def ack(self, msg_tag: tuple[int, int]) -> bool:
        """Acknowledge processing of the message/task

        Will reconnect by itself on channel error.
        Args:
            msg_tag: Message tag received as the first param of the callback function.
        Returns:
            Whether the message was acknowledged successfully and can be processed further.
        """
        conn_id, msg_tag = msg_tag
        if conn_id != self._connection_id:
            return False
        try:
            self.channel.basic.ack(delivery_tag=msg_tag)
        except amqpstorm.AMQPChannelError as why:
            self.log.error("Channel error while acknowledging message: %s", why)
            self.reconnect()
            return False
        return True

    def _consuming_thread_func(self):
        # Register consumers and start consuming loop, reconnect on error
        while self.running:
            try:
                # Register consumers on both queues
                self.channel.basic.consume(self._on_message, self.queue_name, no_ack=False)
                if self.priority_queue_name is not None:
                    self.channel.basic.consume(
                        self._on_message_pri, self.priority_queue_name, no_ack=False
                    )
                # Start consuming (this call blocks until consuming is stopped)
                self.channel.start_consuming()
                self.channel.check_for_errors()
                self.log.info("Exiting thread - Channel was closed.")
                return
            except amqpstorm.AMQPChannelError as e:
                self.log.error("RabbitMQ channel error (will try to reconnect): %s", e)
                self.reconnect()
            except amqpstorm.AMQPConnectionError as e:
                self.log.error(f"RabbitMQ connection error (will try to reconnect): {e}")
                self.reconnect()

    # These two callbacks are called when a new message is received
    # - they only put the message into a local queue
    def _on_message(self, message):
        self.cache.append((self._connection_id, message))
        self.cache_full.set()

    def _on_message_pri(self, message):
        self.cache_pri.append((self._connection_id, message))
        self.cache_full.set()

    def _msg_processing_thread_func(self):
        # Reads local queues and passes tasks to the user callback.
        while self.running:
            # Get task from a local queue (try the priority one first)
            if len(self.cache_pri) > 0:
                conn_id, msg = self.cache_pri.popleft()
                pri = True
            elif len(self.cache) > 0:
                conn_id, msg = self.cache.popleft()
                pri = False
            else:
                self.cache_full.wait()
                self.cache_full.clear()
                continue

            body = msg.body
            tag = (conn_id, msg.delivery_tag)

            self.log.debug(
                "Received {}message: {} (tag: {})".format(
                    "priority " if pri else "",
                    body if len(body) < 500 else f"{body[:500]}...(truncated)",
                    tag,
                )
            )

            # Parse and check validity of received message
            try:
                task = self.parse_task(body)
            except (ValueError, TypeError, KeyError) as e:
                # Print error, acknowledge reception of the message and drop it
                self.log.error(
                    "Erroneous message received from main task queue. "
                    f"Error: {str(e)}, Message: '{body}'"
                )
                self.ack(tag)
                continue

            # Pass message to user's callback function
            try:
                self.callback(tag, task)
            except amqpstorm.AMQPChannelError as e:
                self.log.error("Channel error while processing message: %s", e)
                self.reconnect()
            except Exception as e:
                self.log.exception("Error in user callback function. %s: %s", type(e), str(e))
                self.log.error("Original message: %s", body)

    def watchdog(self):
        """
        Check whether both threads are running and perform a reset if not.

        Register to be called periodically by scheduler.
        """
        proc = self._processing_thread.is_alive()
        cons = self._consuming_thread.is_alive()

        if not proc or not cons:
            self.log.error(
                "Dead threads detected, processing=%s, consuming=%s, restarting TaskQueueReader.",
                "alive" if proc else "dead",
                "alive" if cons else "dead",
            )
            self._stop_consuming_thread()
            self._stop_processing_thread()

            self.channel.close()
            self.channel = None
            self.cache.clear()
            self.cache_pri.clear()

            self.connect()
            self.start()

    def _stop_consuming_thread(self) -> None:
        if self._consuming_thread:
            if self._consuming_thread.is_alive:
                # if not connected, no problem
                with contextlib.suppress(amqpstorm.AMQPError):
                    self.channel.stop_consuming()
            self._consuming_thread.join()
        self._consuming_thread = None

    def _stop_processing_thread(self) -> None:
        if self._processing_thread:
            if self._processing_thread.is_alive:
                self.running = False  # tell processing thread to stop
                self.cache_full.set()  # break potential wait() for data
            self._processing_thread.join()
        self._processing_thread = None


# Set up logging if not part of another program (i.e. when testing/debugging)
if __name__ == "__main__":
    # LOGFORMAT = "%(asctime)-15s,%(threadName)s,%(name)s,[%(levelname)s] %(message)s"
    LOGFORMAT = "[%(levelname)s] %(name)s: %(message)s"
    LOGDATEFORMAT = "%Y-%m-%dT%H:%M:%S"

    logging.basicConfig(level=logging.INFO, format=LOGFORMAT, datefmt=LOGDATEFORMAT)
