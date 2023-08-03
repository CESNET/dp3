"""
Module enabling remote control of the platform's internal events.
"""
import logging
from enum import Enum
from typing import Callable

from pydantic import BaseModel

from dp3.common.config import PlatformConfig
from dp3.common.task import Task
from dp3.task_processing.task_queue import TaskQueueReader


class ControlAction(Enum):
    make_snapshots = "make_snapshots"


class ControlConfig(BaseModel):
    allowed_actions: list[ControlAction]


class ControlMessage(Task):
    action: ControlAction

    def routing_key(self):
        return ""

    def as_message(self) -> str:
        return self.json()


class Control:
    """Class enabling remote control of the platform's internal events."""

    def __init__(
        self,
        platform_config: PlatformConfig,
    ) -> None:
        self.log = logging.getLogger("Control")
        self.action_handlers: dict[ControlAction, Callable] = {}
        self.enabled = False

        if platform_config.process_index != 0:
            self.log.debug("Control will be disabled in this worker to avoid race conditions.")
            return

        self.enabled = True
        self.config = ControlConfig.parse_obj(platform_config.config.get("control"))
        self.allowed_actions = set(self.config.allowed_actions)
        self.log.debug("Allowed actions: %s", self.allowed_actions)

        queue = f"{platform_config.app_name}-control"
        self.control_queue = TaskQueueReader(
            callback=self.process_control_task,
            parse_task=ControlMessage.parse_raw,
            app_name=platform_config.app_name,
            worker_index=platform_config.process_index,
            rabbit_config=platform_config.config.get("processing_core.msg_broker", {}),
            queue=queue,
            priority_queue=queue,
            parent_logger=self.log,
        )

    def start(self):
        """Connect to RabbitMQ and start consuming from TaskQueue."""
        if not self.enabled:
            return

        unconfigured_handlers = self.allowed_actions - set(self.action_handlers)
        if unconfigured_handlers:
            raise ValueError(
                f"The following configured actions are missing handlers: {unconfigured_handlers}"
            )

        self.log.info("Connecting to RabbitMQ")
        self.control_queue.connect()
        self.control_queue.check()  # check presence of needed queues
        self.control_queue.start()

        self.log.debug("Configured handlers: %s", self.action_handlers)

    def stop(self):
        """Stop consuming from TaskQueue, disconnect from RabbitMQ."""
        if not self.enabled:
            return

        self.control_queue.stop()
        self.control_queue.disconnect()

    def set_action_handler(self, action: ControlAction, handler: Callable):
        """Sets the handler for the given action"""
        self.log.debug("Setting handler for action %s: %s", action, handler)
        self.action_handlers[action] = handler

    def process_control_task(self, msg_id, task: ControlMessage):
        """
        Acknowledges the received message and executes an action according to the `task`.

        This function should not be called directly, but set as callback for TaskQueueReader.
        """
        self.control_queue.ack(msg_id)
        if task.action in self.allowed_actions:
            self.log.info("Executing action: %s", task.action)
            self.action_handlers[task.action]()
        else:
            self.log.error("Action not allowed: %s", task.action)
