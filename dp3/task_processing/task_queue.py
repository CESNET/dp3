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

import hashlib
import json
import logging
import threading
import time
from typing import Any, Callable

import amqpstorm

# Exchange and queue names
# They must be pre-declared ('direct' exchange type) and binded.
# The names are prefixed by app_name
# Numbers from 0 to number_of_workers-1 are used as routing/binding keys.
DEFAULT_EXCHANGE = '{}-main-task-exchange'
DEFAULT_PRIORITY_EXCHANGE = '{}-priority-task-exchange'
DEFAULT_QUEUE = '{}-worker-{}'
DEFAULT_PRIORITY_QUEUE = '{}-worker-{}-pri'

# Hash function used to distribute tasks to worker processes. Takes string, returns int.
# (last 4 bytes of MD5)
HASH = lambda x: int(hashlib.md5(x.encode('utf8')).hexdigest()[-4:], 16)

# When reading, pre-fetch only a limited amount of messages
# (because pre-fetched messages are not counted to queue length limit)
PREFETCH_COUNT = 50

# number of seconds to wait for the i-th attempt to reconnect after error
RECONNECT_DELAYS = [1, 2, 5, 10, 30]

class QueueNotDeclared(RuntimeError):
    def __init__(self, queue_name):
        self.queue_name = queue_name
    def __str__(self):
        return f"Queue '{self.queue_name}' is not declared in RabbitMQ! Run 'scripts/rmq_reconfigure.sh' to set up needed exchanges and queues on RabbitMQ server."

class ExchangeNotDeclared(RuntimeError):
    def __init__(self, exchange_name):
        self.exchange_name = exchange_name
    def __str__(self):
        return f"Exchange '{self.exchange_name}' is not declared in RabbitMQ! Run 'scripts/rmq_reconfigure.sh' to set up needed exchanges and queues on RabbitMQ server."


class RobustAMQPConnection:
    """
    Common TaskQueue wrapper, handles connection to RabbitMQ server with automatic reconnection.
    TaskQueueWriter and TaskQueueReader are derived from this.
    """

    def __init__(self, rabbit_config={}) -> None:
        """
        :param rabbit_config: RabbitMQ connection parameters, dict with following keys (all optional):
            host, port, virtual_host, username, password
        """
        self.log = logging.getLogger('RobustAMQPConnection')
        self.conn_params = {
            'hostname': rabbit_config.get('host', 'localhost'),
            'port': int(rabbit_config.get('port', 5672)),
            'virtual_host': rabbit_config.get('virtual_host', '/'),
            'username': rabbit_config.get('username', 'guest'),
            'password': rabbit_config.get('password', 'guest'),
        }
        self.connection = None
        self.channel = None

    def __del__(self):
        self.disconnect()

    def connect(self) -> None:
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

    def disconnect(self) -> None:
        if self.connection:
            self.connection.close()
        self.connection = None
        self.channel = None

    def check_queue_existence(self, queue_name: str) -> bool:
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
    pass


class TaskQueueReader(RobustAMQPConnection):
    pass
