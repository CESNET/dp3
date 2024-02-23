"""Core module that executes periodic update callbacks."""

import logging
from collections import defaultdict
from datetime import datetime, timedelta
from typing import Callable, Union

from pydantic import BaseModel, validate_call

from dp3.common.config import CronExpression, PlatformConfig
from dp3.common.scheduler import Scheduler
from dp3.common.task import DataPointTask, task_context
from dp3.common.types import EventGroupType, ParsedTimedelta
from dp3.database.database import EntityDatabase
from dp3.task_processing.task_queue import TaskQueueWriter


class UpdaterConfig(BaseModel):
    """The configuration of the Updater module.

    The periodic update is executed in smaller batches for better robustness.
    The batch size is dynamically adjusted based on the current number of
    processed entities, the total number of entities and the estimated growth rate.
    The minimum batch size can be also specified to avoid excessive overhead.

    Attributes:
        update_batch_cron: A CRON expression for the periodic update.
        update_batch_period: The period of the periodic update.
            Should equal to the period of update_batch_cron.
        est_growth_rate: The estimated growth rate of the number of entities.
        min_batch_size: The minimum batch size of the periodic update.
    """

    update_batch_cron: CronExpression
    update_batch_period: ParsedTimedelta
    est_growth_rate: float = 0.05
    min_batch_size: int = 10


class Updater:
    """Executes periodic update callbacks."""

    def __init__(
        self,
        db: EntityDatabase,
        task_queue_writer: TaskQueueWriter,
        platform_config: PlatformConfig,
        scheduler: Scheduler,
        elog: EventGroupType,
    ):
        self.log = logging.getLogger("Updater")
        self.elog = elog

        self.model_spec = platform_config.model_spec
        self.config = UpdaterConfig.model_validate(platform_config.config.get("updater", {}))
        self.db = db
        self.task_queue_writer = task_queue_writer
        self.scheduler = scheduler

        self.enabled = platform_config.process_index == 0

        if not self.enabled:
            return

        # Get state cache
        self.cache = self.db.get_module_cache("Updater")
        self._setup_cache_indexes()

        self.update_thread_hooks = defaultdict(dict)

    def _setup_cache_indexes(self):
        """Sets up the indexes of the state cache.

        The cache collection has two types of metadata documents, denoted by the "type" field:

        - The state of the update process for each entity type.
        - Notes about entity deletion that happened since the last update.

        State:
            - t_created: datetime
            - t_last_update: datetime
            - t_end: datetime
            - processed: int - The number of currently processed entities.
            - last_processed_ctime: datetime - The time of creation of the last processed entity.
            - total: int - The total number of entities.
            - etype: str
            - period: int
            - eid_only: bool
            - hook_ids: list[str]

        Deletion note:
            - t_created: datetime
            - seen_times: int - How many times the deletion note was seen.
        """
        self.cache.create_index("type", background=True)
        self.cache.create_index("t_created", background=True)

    @validate_call
    def register_record_update_hook(
        self,
        hook: Callable[[str, str, dict], list[DataPointTask]],
        hook_id: str,
        entity_type: str,
        period: ParsedTimedelta,
    ):
        """Registers a hook for periodic update of entities of the specified type.

        The hook receives the entity type, the entity ID and the master record.
        """
        thread_id = (period, entity_type, False)  # false meaning not eid_only
        hooks = self.update_thread_hooks[thread_id]

        if hook_id in hooks:
            raise ValueError(f"Hook ID {hook_id} already registered for {entity_type}.")
        hooks[hook_id] = hook

    @validate_call
    def register_eid_update_hook(
        self,
        hook: Callable[[str, str], list[DataPointTask]],
        hook_id: str,
        entity_type: str,
        period: Union[timedelta, str],
    ):
        """Registers a hook for periodic update of entities of the specified type.

        The hook receives the entity type and the entity ID.
        """
        thread_id = (period, entity_type, True)  # true meaning eid_only
        hooks = self.update_thread_hooks.get(thread_id, {})

        if hook_id in hooks:
            raise ValueError(f"Hook ID {hook_id} already registered for {entity_type}.")
        hooks[hook_id] = hook

    def new_state(self, period: timedelta, entity_type: str, eid_only: bool = False) -> dict:
        now = datetime.now()
        return {
            "t_created": now,
            "t_last_update": now,
            "t_end": now + period,
            "processed": 0,
            "last_processed_ctime": None,
            "period": period,
            "etype": entity_type,
            "eid_only": eid_only,
            "hook_ids": list(self.update_thread_hooks[(period, entity_type, eid_only)].keys()),
        }

    def start(self):
        """
        TODO:
            Resolve configured hooks with cached previous state.
        """
        for (period, entity_type, eid_only), hooks in self.update_thread_hooks.items():
            state = self.new_state(period, entity_type, eid_only)
            self.scheduler.register(
                self._process_update_batch,
                func_args=[entity_type, hooks, state],
                **self.config.update_batch_cron.model_dump(),
            )

    def stop(self):
        """
        Stops the updater.
        """

    def _process_update_batch(self, entity_type: str, hooks: dict, state: dict):
        """Processes a batch of entities of the specified type.

        TODO:
            Add support for entity deletion notes.
            Add support for storing the state in the cache.

        Args:
            entity_type: The entity type.
            hooks: The update hooks.
            state: The state of the update process.
        """
        self.log.debug("Processing update batch for '%s'")
        state["total"] = self.db.get_estimated_entity_count(entity_type)
        batch_size = self._calculate_batch_size(entity_type, state)
        self.log.debug(
            "Current state - total: %s, processed: %s, batch size: %s",
            state["total"],
            state["processed"],
            batch_size,
        )
        records = (
            self.db.get_master_records(entity_type, sort=[("#time_created", 1)])
            .skip(state["processed"])
            .limit(batch_size)
        )

        for record in records:
            self._run_hooks(hooks, entity_type, record)

            state["processed"] += 1
            state["last_processed_ctime"] = record["#time_created"]
            state["t_last_update"] = datetime.now()

        if state["processed"] >= state["total"]:
            self.log.debug(
                "Finished processing '%s' entity with period: %s", entity_type, state["period"]
            )
            state.update(self.new_state(state["period"], entity_type, state["eid_only"]))

    def _process_eid_update_batch(self, entity_type: str, hooks: dict, state: dict):
        """Processes a batch of entities of the specified type, only passing the entity ID.

        Args:
            entity_type: The entity type.
            hooks: The update hooks.
            state: The state of the update process.
        """
        state["total"] = self.db.get_estimated_entity_count(entity_type)
        batch_size = self._calculate_batch_size(entity_type, state)
        records = (
            self.db.get_master_records(
                entity_type, sort={"#time_created": 1}, project={"_id": True}
            )
            .skip(state["processed"])
            .limit(batch_size)
        )

        for record in records:
            self._run_hooks_eid(hooks, entity_type, record["_id"])

            state["processed"] += 1
            state["last_processed_ctime"] = record["#time_created"]
            state["t_last_update"] = datetime.now()

        if state["processed"] >= state["total"]:
            self.log.debug(
                "Finished processing '%s' entity with period: %s", entity_type, state["period"]
            )
            state.update(self.new_state(state["period"], entity_type, state["eid_only"]))

    def _calculate_batch_size(self, entity_type, state):
        processed = state["processed"]
        total = state["total"]

        # Calculate the batch size
        if total <= processed:
            return 0
        batches_remaining = (state["t_end"] - datetime.now()) // self.config.update_batch_period
        batch_size = int((total - processed) / (batches_remaining + 1))
        return batch_size

    def _run_hooks(self, hooks: dict[str, Callable], entity_type: str, record: dict):
        tasks = []
        with task_context(self.model_spec):
            for hook_id, hook in hooks.items():
                self.log.debug("Running hook: '%s'", hook_id)
                try:
                    new_tasks = hook(entity_type, record["_id"], record)
                    tasks.extend(new_tasks)
                except Exception as e:
                    self.elog.log("module_error")
                    self.log.error(f"Error during running hook {hook}: {e}")

        for task in tasks:
            self.task_queue_writer.put_task(task)

    def _run_hooks_eid(self, hooks: dict[str, Callable], entity_type: str, eid: str):
        tasks = []
        with task_context(self.model_spec):
            for hook_id, hook in hooks.items():
                self.log.debug("Running hook: '%s'", hook_id)
                try:
                    new_tasks = hook(entity_type, eid)
                    tasks.extend(new_tasks)
                except Exception as e:
                    self.elog.log("module_error")
                    self.log.error(f"Error during running hook {hook}: {e}")

        for task in tasks:
            self.task_queue_writer.put_task(task)