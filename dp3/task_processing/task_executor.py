import logging
from typing import Callable

from event_count_logger import DummyEventGroup, EventCountLogger

from dp3.common.task import Push
from dp3.database.database import DatabaseError, EntityDatabase
from dp3.task_processing.task_hooks import (
    TaskAttrHooksContainer,
    TaskEntityHooksContainer,
    TaskGenericHooksContainer,
)

from ..common.config import PlatformConfig


class TaskExecutor:
    """
    TaskExecutor manages updates of entity records,
    which are being read from task queue (via parent
    [`TaskDistributor`][dp3.task_processing.task_distributor.TaskDistributor])

    Args:
        db: Instance of EntityDatabase
        platform_config: Current platform configuration.
    """

    def __init__(
        self,
        db: EntityDatabase,
        platform_config: PlatformConfig,
    ) -> None:
        # initialize task distribution

        self.log = logging.getLogger("TaskExecutor")

        # Get list of configured entity types
        self.entity_types = list(platform_config.model_spec.entities.keys())
        self.log.debug(f"Configured entity types: {self.entity_types}")

        self.model_spec = platform_config.model_spec
        self.db = db

        # EventCountLogger
        # - count number of events across multiple processes using shared counters in Redis
        ecl = EventCountLogger(
            platform_config.config.get("event_logging.groups"),
            platform_config.config.get("event_logging.redis"),
        )
        self.elog = ecl.get_group("te") or DummyEventGroup()
        # Print warning if some event group is not configured
        not_configured_groups = []
        if isinstance(self.elog, DummyEventGroup):
            not_configured_groups.append("te")
        if not_configured_groups:
            self.log.warning(
                "EventCountLogger: No configuration for event group(s) "
                f"'{','.join(not_configured_groups)}' found, "
                "such events will not be logged (check event_logging.yml)"
            )

        # Hooks
        self._task_generic_hooks = TaskGenericHooksContainer(self.log)
        self._task_entity_hooks = {}
        self._task_attr_hooks = {}

        for entity in self.model_spec.entities:
            self._task_entity_hooks[entity] = TaskEntityHooksContainer(entity, self.log)

        for entity, attr in self.model_spec.attributes:
            attr_type = self.model_spec.attributes[entity, attr].t
            self._task_attr_hooks[entity, attr] = TaskAttrHooksContainer(
                entity, attr, attr_type, self.log
            )

    def register_task_hook(self, hook_type: str, hook: Callable):
        """Registers one of available task hooks

        See: [`TaskGenericHooksContainer`][dp3.task_processing.task_hooks.TaskGenericHooksContainer]
        in `task_hooks.py`
        """
        self._task_generic_hooks.register(hook_type, hook)

    def register_entity_hook(self, hook_type: str, hook: Callable, entity: str):
        """Registers one of available task entity hooks

        See: [`TaskEntityHooksContainer`][dp3.task_processing.task_hooks.TaskEntityHooksContainer]
        in `task_hooks.py`
        """
        self._task_entity_hooks[entity].register(hook_type, hook)

    def register_attr_hook(self, hook_type: str, hook: Callable, entity: str, attr: str):
        """Registers one of available task attribute hooks

        See: [`TaskAttrHooksContainer`][dp3.task_processing.task_hooks.TaskAttrHooksContainer]
        in `task_hooks.py`
        """
        self._task_attr_hooks[entity, attr].register(hook_type, hook)

    def process_task(self, task: Push) -> tuple[bool, list[Push]]:
        """
        Main processing function - push datapoint values, running all registered hooks.

        Args:
            task: Task object to process.
        Returns:
            True if a new record was created, False otherwise,
            and a list of new tasks created by hooks
        """
        self.log.debug(f"Received new task {task.etype}/{task.ekey}, starting processing!")

        new_tasks = []

        # Run on_task_start hook
        self._task_generic_hooks.run_on_start(task)

        # Check existence of etype
        if task.etype not in self.entity_types:
            self.log.error(f"Task {task.etype}/{task.ekey}: Unknown entity type!")
            self.elog.log("task_processing_error")
            return False, new_tasks

        # Check existence of ekey
        try:
            ekey_exists = self.db.ekey_exists(task.etype, task.ekey)
        except DatabaseError as e:
            self.log.error(f"Task {task.etype}/{task.ekey}: DB error: {e}")
            return False, new_tasks

        if not ekey_exists:
            # Run allow_entity_creation hook
            if not self._task_entity_hooks[task.etype].run_allow_creation(task.ekey, task):
                self.log.debug(
                    f"Task {task.etype}/{task.ekey}: hooks decided not to create new ekey record"
                )
                return False, new_tasks

            # Run on_entity_creation hook
            new_tasks += self._task_entity_hooks[task.etype].run_on_creation(task.ekey, task)

        # Insert into database
        try:
            self.db.insert_datapoints(task.etype, task.ekey, task.data_points)
            self.log.debug(f"Task {task.etype}/{task.ekey}: All changes written to DB")
        except DatabaseError as e:
            self.log.error(f"Task {task.etype}/{task.ekey}: DB error: {e}")
            return False, new_tasks

        # Run attribute hooks
        for dp in task.data_points:
            new_tasks += self._task_attr_hooks[dp.etype, dp.attr].run_on_new(dp.eid, dp)

        # Log the processed task
        self.elog.log("task_processed")

        self.log.debug(f"Secondary modules created {len(new_tasks)} new tasks.")

        return True, new_tasks
