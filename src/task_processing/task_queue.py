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
(should be part of global NERD config files)

rabbitmq:
  host: localhost
  port: 5672
  virtual_host: /
  username: guest
  password: guest

parallel:
  processes: 1
"""

import logging
import hashlib
import time
import json
import threading
import collections

import amqpstorm
import requests
from requests.auth import HTTPBasicAuth


from src.common.utils import conv_from_json, conv_to_json
import g

# This sets logging level of all components in this file
LOG_LEVEL = logging.INFO
#LOG_LEVEL = logging.DEBUG

# Exchange and queue names - TODO pp prefix as processing platform, rename later
# They must be pre-declared ('direct' exchange type) and binded.
# Numbers from 0 to number_of_workers-1 are used as routing/binding keys.
DEFAULT_EXCHANGE = 'pp-main-task-exchange'
DEFAULT_PRIORITY_EXCHANGE = 'pp-priority-task-exchange'
DEFAULT_QUEUE = 'pp-worker-{}'
DEFAULT_PRIORITY_QUEUE = 'pp-worker-{}-pri'

# Hash function used to distribute tasks to worker processes. Takes string, returns int.
# (last 4 bytes of MD5)
HASH = lambda x: int(hashlib.md5(x.encode('utf8')).hexdigest()[-4:], 16)

# When reading, pre-fetch only a limited amount of messages
# (because pre-fetched messages are not counted to queue length limit)
PREFETCH_COUNT = 50

# number of seconds to wait for the i-th attempt to reconnect after error
RECONNECT_DELAYS = [1, 2, 5, 10, 30]


class RobustAMQPConnection:
    """
    Common TaskQueue wrapper, handles connection to RabbitMQ server with automatic reconnection.
    TaskQueueWriter and TaskQueueReader are derived from this.
    """
    def __init__(self, rabbit_config={}):
        """
        :param rabbit_config: RabbitMQ connection parameters, dict with following keys (all optional):
            host, port, virtual_host, username, password
        """
        self.log = logging.getLogger('RobustAMQPConnection')
        self.log.setLevel(LOG_LEVEL)
        self.conn_params = {
            'hostname': rabbit_config.get('host', 'localhost'),
            'port': int(rabbit_config.get('port', 5672)),
            'virtual_host': rabbit_config.get('virtual_host', '/'),
            'username': rabbit_config.get('username', 'guest'),
            'password': rabbit_config.get('password', 'guest'),
        }
        self.connection = None
        self.channel = None

        # check if compulsory queues are declared
        try:
            g.config.get('worker_processes')
        except AttributeError:
            # when task_queue is initialized from standalone modules, it has no access to g, so skip queue check
            return
        resp = requests.get("http://localhost:15672/api/queues", auth=HTTPBasicAuth(rabbit_config.get('username', 'guest'),
                                                                                    rabbit_config.get('password', 'guest')))
        if resp.status_code != 200:
            assert False, "could not check RabbitMQ declared queues"
        queues = json.loads(resp.text)
        queues_names = [queue['name'] for queue in queues]
        compulsory_queues = []
        for i in range(0, g.config.get('worker_processes')):
            compulsory_queues.append(DEFAULT_QUEUE.format(i))
            compulsory_queues.append(DEFAULT_PRIORITY_QUEUE.format(i))
        assert all(compulsory_queue in queues_names for compulsory_queue in compulsory_queues), "RabbitMQ server does " \
                                                                                                "not have declared needed queues"

    def __del__(self):
        self.disconnect()

    def connect(self):
        """Create a connection (or reconnect after error).

        If connection can't be established, try it again indefinitely.
        """
        if self.connection:
            self.connection.close()
        attempts = 0
        while True:
            attempts += 1
            try:
                self.connection = amqpstorm.Connection(**self.conn_params)
                self.log.debug("AMQP connection created, server: '{hostname}:{port}/{virtual_host}'".format_map(self.conn_params))
                if attempts > 1:
                    # This was a repeated attempt, print success message with ERROR level
                    self.log.error("... it's OK now, we're successfully connected!")

                self.channel = self.connection.channel()
                self.channel.confirm_deliveries()
                self.channel.basic.qos(PREFETCH_COUNT)
                break
            except amqpstorm.AMQPError as e:
                sleep_time = RECONNECT_DELAYS[min(attempts, len(RECONNECT_DELAYS))-1]
                self.log.error("RabbitMQ connection error (will try to reconnect in {}s): {}".format(sleep_time, e))
                time.sleep(sleep_time)
            except KeyboardInterrupt:
                break

    def disconnect(self):
        if self.connection:
            self.connection.close()
        self.connection = None
        self.channel = None


class TaskQueueWriter(RobustAMQPConnection):
    def __init__(self, workers=1, rabbit_config={}, exchange=DEFAULT_EXCHANGE, priority_exchange=DEFAULT_PRIORITY_EXCHANGE):
        """
        Create an object for writing tasks into the main Task Queue.

        :param workers: Number of worker processes in the system
        :param rabbit_config: RabbitMQ connection parameters, dict with following keys (all optional):
            host, port, virtual_host, username, password
        :param exchange: Name of the exchange to write tasks to
        :param priority_exchange: Name of the exchange to write priority tasks to
        """
        assert isinstance(workers, int) and workers >= 1, "count of workers must be positive number"
        assert isinstance(exchange, str), "exchange argument has to be string!"
        assert isinstance(priority_exchange, str), "priority_exchange has to be string"

        super().__init__(rabbit_config)

        self.log = logging.getLogger('TaskQueueWriter')
        self.log.setLevel(LOG_LEVEL)

        self.workers = workers
        self.exchange = exchange
        self.exchange_pri = priority_exchange

    def put_task(self, etype="", ekey="", attr_updates=None, events=None, data_points=None, create=None, delete=False, src="", tags=None, priority=False):
        """
        Put task (update_request) to the queue of corresponding worker
        :param etype: entity type (eg. 'ip')
        :param ekey: entity key
        :param attr_updates: TODO
        :param events: list of events to issue (just plain strings, as event parameters are not needed, may be added in the future if needed)
        :param data_points: list of attribute data points, which will be saved in the database
        :param create: true = create a new record if it doesn't exist yet; false = don't create a record if it
                    doesn't exist (like "weak" in NERD); not set = use global configuration flag "auto_create_record" of the entity type
        :param delete: delete the record
        :param src: name of source module, mostly for logging
        :param tags: tags for logging (number of tasks per time interval by tag)
        :param priority: if true, the task is places into priority queue
        :return: None
        """
        if not self.channel:
            self.connect()

        # Prepare message
        msg = {
            'etype': etype,
            'ekey': ekey,
            'attr_updates': [] if attr_updates is None else attr_updates,
            'events': [] if events is None else events,
            'data_points': [] if data_points is None else data_points,
            'create': g.config.get('auto_create_record', True) if create is None else create,
            'delete': delete,
            'src': src,
            'tags': [] if tags is None else tags
        }

        self.log.debug(f"Received new task: {msg}")

        # Prepare routing key
        body = json.dumps(msg, default=conv_to_json).encode('utf8')
        key = etype + ':' + str(ekey)
        routing_key = HASH(key) % self.workers  # index of the worker to send the task to

        exchange = self.exchange_pri if priority else self.exchange

        # Send the message
        # ('mandatory' flag means that we want to guarantee it's delivered to someone. If it can't be delivered (no
        #  consumer or full queue), wait a while and try again. Always print just one error message for each
        #  unsuccessful message to be send.)
        self.log.debug("Sending a task with routing_key={} to exchange '{}'".format(routing_key, exchange))
        err_printed = 0
        while True:
            try:
                message = amqpstorm.Message.create(self.channel, body)
                success = message.publish(str(routing_key), exchange, mandatory=True)
                if success: # message ACK'd
                    if err_printed == 1:
                        self.log.debug("... it's OK now, the message was successfully sent")
                    elif err_printed == 2:
                        self.log.warning("... it's OK now, the message was successfully sent")
                    else:
                        self.log.debug("Message successfully sent")
                    break
                else: # message NACK'd
                    if err_printed != 1:
                        self.log.debug("Message rejected (queue of worker {} is probably full), will retry every 100ms".format(routing_key))
                        err_printed = 1
                    time.sleep(0.1)
            except amqpstorm.AMQPChannelError as e:
                if err_printed != 2:
                    self.log.warning("Can't deliver a message to worker {} (will retry every 5 seconds): {}".format(routing_key, e))
                    err_printed = 2
                time.sleep(5)
            except amqpstorm.AMQPConnectionError as e:
                self.log.error("RabbitMQ connection error (will try to reconnect): {}".format(e))
                self.connect()


class TaskQueueReader(RobustAMQPConnection):
    def __init__(self, callback, worker_index=0, rabbit_config={}, queue=DEFAULT_QUEUE, priority_queue=DEFAULT_PRIORITY_QUEUE):
        """
        Create an object for reading tasks from the main Task Queue.

        It consumes messages from two RabbitMQ queues (normal and priority one for given worker) and passes them to
        the given callback function. Tasks from the priority queue are passed before the normal ones.

        Each received message must be acknowledged by calling .ack(msg_tag).

        :param callback: Function called when a message is received, prototype: func(msg_tag, etype, eid, ops)
        :param worker_index: index of this worker (filled into DEFAULT_QUEUE string using .format() method)
        :param rabbit_config: RabbitMQ connection parameters, dict with following keys (all optional):
            host, port, virtual_host, username, password
        :param queue: Name of RabbitMQ queue to read from (should contain "{}" to fill in worker_index)
        :param priority_queue: Name of RabbitMQ queue to read from (priority messages) (should contain "{}" to fill in worker_index)
        """
        assert callable(callback), "callback must be callable object"
        assert isinstance(worker_index, int) and worker_index >= 0, "worker_index must be positive number"
        assert isinstance(queue, str), "queue must be string"
        assert isinstance(priority_queue, str), "priority_queue must be string"

        super().__init__(rabbit_config)

        self.log = logging.getLogger('TaskQueueReader')
        self.log.setLevel(LOG_LEVEL)

        self.callback = callback
        self.queue_name = queue.format(worker_index)
        self.priority_queue_name = priority_queue.format(worker_index)

        self.running = False

        self._consuming_thread = None
        self._processing_thread = None

        # Receive messages into 2 temporary queues (max length should be equal to prefetch_count set in RabbitMQReader)
        self.cache = collections.deque()
        self.cache_pri = collections.deque()
        self.cache_full = threading.Event()  # signalize there's something in the cache

    def __del__(self): #TODO is this needed?
        self.log.debug("Destructor called")
        self._stop_consuming_thread()
        self._stop_processing_thread()
        super().__del__()

    def start(self):
        """Start receiving tasks."""
        if self.running:
            raise RuntimeError("Already running")

        if not self.connection:
            self.connect()

        self.log.info("Starting TaskQueueReader")

        # Start thread for message consuming from server
        self._consuming_thread = threading.Thread(None, self._consuming_thread_func)
        self._consuming_thread.start()

        # Start thread for message processing and passing to user's callback
        self.running = True
        self._processing_thread = threading.Thread(None, self._msg_processing_thread_func)
        self._processing_thread.start()

    def stop(self):
        """Stop receiving tasks."""
        if not self.running:
            raise RuntimeError("Not running")

        self._stop_consuming_thread()
        self._stop_processing_thread()
        self.log.info("TaskQueueReader stopped")

    def ack(self, msg_tag):
        """Acknowledge processing of the message/task

        :param msg_tag: Message tag received as the first param of the callback function.
        """
        self.channel.basic.ack(delivery_tag=msg_tag)

    def _consuming_thread_func(self):
        # Register consumers and start consuming loop, reconnect on error
        while True:
            try:
                # Register consumers on both queues
                self.channel.basic.consume(self._on_message, self.queue_name, no_ack=False)
                self.channel.basic.consume(self._on_message_pri, self.priority_queue_name, no_ack=False)
                # Start consuming (this call blocks until consuming is stopped)
                self.channel.start_consuming()
                return
            except amqpstorm.AMQPConnectionError as e:
                self.log.error("RabbitMQ connection error (will try to reconnect): {}".format(e))
                self.connect()

    # These two callbacks are called when a new message is received - they only put the message into a local queue
    def _on_message(self, message):
        self.cache.append(message)
        self.cache_full.set()

    def _on_message_pri(self, message):
        self.cache_pri.append(message)
        self.cache_full.set()

    def _msg_processing_thread_func(self):
        # Reads local queues and passes tasks to the user callback.
        while self.running:
            # Get task from a local queue (try the priority one first)
            if len(self.cache_pri) > 0:
                msg = self.cache_pri.popleft()
                pri = True
            elif len(self.cache) > 0:
                msg = self.cache.popleft()
                pri = False
            else:
                self.cache_full.wait()
                self.cache_full.clear()
                continue

            body = msg.body
            tag = msg.delivery_tag

            self.log.debug("Received {}message: {} (tag: {})".format("priority " if pri else "", body, tag))

            # Parse and check validity of received message
            try:
                task = json.loads(body, object_hook=conv_from_json)
                etype, ekey, attr_updates = task['etype'], task['ekey'], task['attr_updates']
                events, data_points, create = task['events'], task['data_points'], task['create']
                delete, src, tags = task['delete'], task['src'], task['tags']
            except (ValueError, TypeError, KeyError) as e:
                # Print error, acknowledge reception of the message and drop it
                self.log.error("Erroneous message received from main task queue. Error: {}, Message: '{}'".format(str(e), body))
                self.ack(tag)
                continue

            # Pass message to user's callback function
            self.callback(tag, etype, ekey, attr_updates, events, data_points, create, delete, src, tags)

    def _stop_consuming_thread(self):
        if self._consuming_thread:
            if self._consuming_thread.is_alive:
                try:
                    self.channel.stop_consuming()
                except amqpstorm.AMQPError as e:
                    pass  # not connected or error - no problem here
            self._consuming_thread.join()
        self._consuming_thread = None

    def _stop_processing_thread(self):
        if self._processing_thread:
            if self._processing_thread.is_alive:
                self.running = False  # tell processing thread to stop
                self.cache_full.set()  # break potential wait() for data
            self._processing_thread.join()
        self._processing_thread = None


# Set up logging if not part of another program (i.e. when testing/debugging)
if __name__ == '__main__':
    # LOGFORMAT = "%(asctime)-15s,%(threadName)s,%(name)s,[%(levelname)s] %(message)s"
    LOGFORMAT = "[%(levelname)s] %(name)s: %(message)s"
    LOGDATEFORMAT = "%Y-%m-%dT%H:%M:%S"

    logging.basicConfig(level=logging.INFO, format=LOGFORMAT, datefmt=LOGDATEFORMAT)