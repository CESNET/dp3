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
            - total: int - The total number of entities.
            - etype: str
            - period: int
            - eid_only: bool
            - hook_ids: list[str]

        Deletion note:
            - t_created: datetime
            - seen_times: int - How many times was the deletion note seen.
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
        thread_id = (period.total_seconds(), entity_type, False)  # false meaning not eid_only
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
        thread_id = (period.total_seconds(), entity_type, True)  # true meaning eid_only
        hooks = self.update_thread_hooks.get(thread_id, {})

        if hook_id in hooks:
            raise ValueError(f"Hook ID {hook_id} already registered for {entity_type}.")
        hooks[hook_id] = hook

    def new_state(self, period: float, entity_type: str, eid_only: bool = False) -> dict:
        now = datetime.now()
        return {
            "type": "state",
            "t_created": now,
            "t_last_update": now,
            "t_end": now + timedelta(seconds=period),
            "processed": 0,
            "total": 0,
            "iteration": 0,
            "modulo": 0,
            "finished": False,
            "period": period,
            "etype": entity_type,
            "eid_only": eid_only,
            "hook_ids": list(self.update_thread_hooks[(period, entity_type, eid_only)].keys()),
        }

    def start(self):
        """
        Starts the updater.

        Will fetch the state of the updater from the cache and schedule the update threads.
        """
        # Get all unfinished progress states
        states = self.cache.find({"type": "state", "finished": False})
        saved_states = {}
        for state in states:
            period = state["period"]
            entity_type = state["etype"]
            eid_only = state["eid_only"]
            saved_states[(period, entity_type, eid_only)] = state

        # Confirm all saved states have configured hooks, terminate if not
        for (period, entity_type, eid_only), state in saved_states.items():
            if (period, entity_type, eid_only) not in self.update_thread_hooks:
                self.log.warning(
                    "No hooks configured for '%s' entity with period: %s, aborting hooks: %s",
                    entity_type,
                    period,
                    state["hook_ids"],
                )
                self.cache.update_one(
                    {
                        k: v
                        for k, v in state.items()
                        if k in ["type", "period", "etype", "eid_only", "t_created"]
                    },
                    update={"$set": {"finished": True}},
                )

            # Find if any new hooks are added
            configured_hooks = self.update_thread_hooks[(period, entity_type, eid_only)]
            saved_hook_ids = set(state["hook_ids"])
            configured_hook_ids = set(configured_hooks.keys())
            new_hook_ids = configured_hook_ids - saved_hook_ids
            deleted_hook_ids = saved_hook_ids - configured_hook_ids

            # Update the state with new hooks
            if deleted_hook_ids or new_hook_ids:
                if deleted_hook_ids:
                    self.log.warning(
                        "Some hooks are deleted for '%s' entity with period: %s - %s",
                        entity_type,
                        period,
                        deleted_hook_ids,
                    )
                state["hook_ids"] = list(configured_hook_ids)
                self.cache.update_one(
                    {
                        k: v
                        for k, v in state.items()
                        if k in ["type", "period", "etype", "eid_only", "t_created"]
                    },
                    update={
                        "$set": {
                            k: v
                            for k, v in state.items()
                            if k not in ["type", "period", "etype", "eid_only", "t_created"]
                        },
                    },
                )

        # Add newly configured hooks that are not in the saved states
        for thread_id in self.update_thread_hooks:
            if thread_id not in saved_states:
                state = self.new_state(*thread_id)
                self.cache.insert_one(state)
                saved_states[thread_id] = state

        # Schedule the update threads
        for (period, entity_type, eid_only), state in saved_states.items():
            if state["finished"]:
                continue
            hooks = self.update_thread_hooks[(period, entity_type, eid_only)]
            if eid_only:
                processing_func = self._process_eid_update_batch
            else:
                processing_func = self._process_update_batch

            if "_id" in state:
                del state["_id"]

            state["total"] = self.db.get_estimated_entity_count(entity_type)
            state["modulo"] = self._calculate_batch_count(state)

            self.scheduler.register(
                processing_func,
                func_args=[entity_type, hooks, state],
                **self.config.update_batch_cron.model_dump(),
            )

    def stop(self):
        """
        Stops the updater.
        """

    def _process_update_batch(self, entity_type: str, hooks: dict, state: dict):
        """Processes a batch of entities of the specified type.

        Args:
            entity_type: The entity type.
            hooks: The update hooks.
            state: The state of the update process.
        """
        self.log.debug("Processing update batch for '%s'", entity_type)
        batch_cnt = state["modulo"]
        iteration = state["iteration"]

        self.log.debug(
            "Current state - total: %s, processed: %s, iteration: %s",
            state["total"],
            state["processed"],
            iteration,
        )
        records = self.db.get_worker_master_records(
            worker_index=iteration, worker_cnt=batch_cnt, etype=entity_type
        )

        for record in records:
            self._run_hooks(hooks, entity_type, record)

            state["processed"] += 1
            state["t_last_update"] = datetime.now()

        if state["processed"] >= state["total"]:
            state["finished"] = True
        state["iteration"] = iteration + 1

        filter_dict = {
            k: v
            for k, v in state.items()
            if k in ["type", "period", "etype", "eid_only", "t_created", "_id"]
        }
        update_dict = {
            "$set": {
                k: v
                for k, v in state.items()
                if k not in ["type", "period", "etype", "eid_only", "t_created", "_id"]
            }
        }
        self.cache.update_one(filter_dict, update=update_dict, upsert=True)

        if state["finished"]:
            self.log.debug(
                "Finished processing '%s' entity with period: %s", entity_type, state["period"]
            )
            state.update(self.new_state(state["period"], entity_type, state["eid_only"]))
            state["total"] = self.db.get_estimated_entity_count(entity_type)
            state["modulo"] = self._calculate_batch_count(state)

    def _process_eid_update_batch(self, entity_type: str, hooks: dict, state: dict):
        """Processes a batch of entities of the specified type, only passing the entity ID.

        TODO: Finish after finalizing normal update callback

        Args:
            entity_type: The entity type.
            hooks: The update hooks.
            state: The state of the update process.
        """

    def _calculate_batch_count(self, state):
        return state["period"] // self.config.update_batch_period.total_seconds()

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
