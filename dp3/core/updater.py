"""Core module that executes periodic update callbacks."""

import logging
from collections import defaultdict
from collections.abc import Iterator
from datetime import datetime, timedelta
from functools import partial
from typing import Callable, Literal

from pydantic import BaseModel, validate_call
from pymongo.cursor import Cursor
from pymongo.results import UpdateResult

from dp3.common.config import CronExpression, PlatformConfig
from dp3.common.scheduler import Scheduler
from dp3.common.task import DataPointTask, task_context
from dp3.common.types import EventGroupType, ParsedTimedelta
from dp3.database.database import EntityDatabase
from dp3.task_processing.task_queue import TaskQueueWriter


class UpdaterConfig(BaseModel, extra="forbid"):
    """The configuration of the Updater module.

    The periodic update is executed in smaller batches for better robustness.
    The batch size is dynamically adjusted based the total number of entities
    and the estimated growth rate.

    Attributes:
        update_batch_cron: A CRON expression for the periodic update.
        update_batch_period: The period of the periodic update.
            Should equal to the period of `update_batch_cron`.
        cache_management_cron: A CRON expression for the cache management.
        cache_max_entries: The maximum number of finished cache entries per thread_id.
    """

    update_batch_cron: CronExpression
    update_batch_period: ParsedTimedelta

    cache_management_cron: CronExpression = CronExpression(hour=2, minute=0, second=0)
    cache_max_entries: int = 32


class UpdateThreadState(BaseModel, validate_assignment=True):
    """
    A cache item describing a state of one configured update thread.

    Attributes:
        type: "state"
        t_created: Time of creation.
        t_last_update: Time of last update.
        t_end: Time of predicted period end.
        processed: The number of currently processed entities.
        total: The total number of entities.
        iteration: The current iteration.
        total_iterations: Total number of iterations.
        etype: Entity type.
        period: Period length in seconds.
        eid_only: Whether only eids are passed to hooks.
        hook_ids: Hook ids.
        runtime_secs: Total hook runtime in seconds.
    """

    type: Literal["state"] = "state"
    t_created: datetime
    t_last_update: datetime
    t_end: datetime
    processed: int = 0
    total: int = 0
    iteration: int = 0
    total_iterations: int = 0
    finished: bool = False
    period: float
    etype: str
    eid_only: bool
    hook_ids: list[str]
    runtime_secs: float = 0.0

    @classmethod
    def new(cls, hooks: dict, period: float, entity_type: str, eid_only: bool = False):
        """Create a new instance initialized with hooks and thread_id components."""
        now = datetime.now()
        return cls(
            t_created=now,
            t_last_update=now,
            t_end=now + timedelta(seconds=period),
            period=period,
            etype=entity_type,
            eid_only=eid_only,
            hook_ids=hooks.keys(),
        )

    @staticmethod
    def id_attributes():
        """A list of attributes which identify the state in cache."""
        return ["type", "period", "etype", "eid_only", "t_created"]

    @property
    def thread_id(self) -> tuple[float, str, bool]:
        """A tuple of (period, entity_type, eid_only)."""
        return self.period, self.etype, self.eid_only

    def reset(self):
        """Resets counters and timestamps."""
        now = datetime.now()
        self.t_created = now
        self.t_last_update = now
        self.t_end = now + timedelta(seconds=self.period)
        self.iteration = 0
        self.processed = 0
        self.runtime_secs = 0.0
        self.finished = False


class UpdaterCache:
    """
    The cache collection contains the metadata documents with
    the state of the update process for each entity type.
    """

    def __init__(self, cache_collection):
        self._cache = cache_collection
        self._setup_cache_indexes()

    def get_unfinished(self) -> Iterator[UpdateThreadState]:
        """Yields all unfinished cache entries from DB."""
        for state in self._cache.find({"type": "state", "finished": False}):
            yield UpdateThreadState.model_validate(state)

    def upsert(self, state: UpdateThreadState) -> UpdateResult:
        """Update or insert a state entry into DB."""
        state_dict = state.model_dump()
        filter_dict = {k: v for k, v in state_dict.items() if k in state.id_attributes()}
        update_dict = {
            "$set": {k: v for k, v in state_dict.items() if k not in state.id_attributes()}
        }
        return self._cache.update_one(filter_dict, update=update_dict, upsert=True)

    def register_management(
        self, scheduler: Scheduler, trigger: CronExpression, max_entries: int
    ) -> int:
        """Registers the cache management task with the scheduler."""
        return scheduler.register(
            self._manage_cache, func_args=[max_entries], **trigger.model_dump()
        )

    def _manage_cache(self, max_entries: int):
        """Removes the oldest finished cache entries if their count exceeds the limit."""
        counts_per_thread_id = defaultdict(int)
        to_delete = []

        for state in self._cache.find({"type": "state", "finished": True}).sort("t_created", -1):
            state_obj = UpdateThreadState.model_validate(state)
            counts_per_thread_id[state_obj.thread_id] += 1
            if counts_per_thread_id[state_obj.thread_id] > max_entries:
                to_delete.append(state["_id"])

        self._cache.delete_many({"_id": {"$in": to_delete}})

    def _setup_cache_indexes(self):
        """Sets up the indexes of the state cache."""
        self._cache.create_index("t_created", background=True)


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
        self.cache = UpdaterCache(self.db.get_module_cache("Updater"))
        self.cache.register_management(
            scheduler, self.config.cache_management_cron, self.config.cache_max_entries
        )

        self.update_thread_hooks = defaultdict(dict)

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
        self._register_hook(hook, hook_id, entity_type, period.total_seconds(), eid_only=False)

    @validate_call
    def register_eid_update_hook(
        self,
        hook: Callable[[str, str], list[DataPointTask]],
        hook_id: str,
        entity_type: str,
        period: ParsedTimedelta,
    ):
        """Registers a hook for periodic update of entities of the specified type.

        The hook receives the entity type and the entity ID.
        """
        self._register_hook(hook, hook_id, entity_type, period.total_seconds(), eid_only=True)

    def _register_hook(self, hook, hook_id: str, entity_type: str, period: float, eid_only: bool):
        if not self.enabled:
            return

        update_period_secs = self.config.update_batch_period.total_seconds()
        if period < update_period_secs:
            raise ValueError(
                f"The total period {period}s is must be greater or equal than "
                f"the update batch period {update_period_secs}s."
            )

        thread_id = (period, entity_type, eid_only)
        hooks = self.update_thread_hooks[thread_id]
        if hook_id in hooks:
            raise ValueError(f"Hook ID {hook_id} already registered for {entity_type}.")
        self.log.info(
            "Registered hook '%s' to thread processing entity '%s' over %.1fs, eid_only = %s",
            hook_id,
            entity_type,
            period,
            eid_only,
        )
        hooks[hook_id] = hook

    def start(self):
        """
        Starts the updater.

        Will fetch the state of the updater from the cache and schedule the update threads.
        """
        if not self.enabled:
            return

        # Get all unfinished progress states
        saved_states = {}
        for state in self.cache.get_unfinished():
            saved_states[state.thread_id] = state

        # Confirm all saved states have configured hooks, terminate if not
        for thread_id, state in saved_states.items():
            if thread_id not in self.update_thread_hooks:
                self.log.warning(
                    "Previously configured hooks %s for '%s' entity with period: %ss "
                    "match no current configuration, aborting update thread.",
                    state.hook_ids,
                    state.etype,
                    state.period,
                )
                state.finished = True
                self.cache.upsert(state)
                continue

            # Find if any new hooks are added
            configured_hooks = self.update_thread_hooks[thread_id]
            saved_hook_ids = set(state.hook_ids)
            configured_hook_ids = set(configured_hooks.keys())
            new_hook_ids = configured_hook_ids - saved_hook_ids
            deleted_hook_ids = saved_hook_ids - configured_hook_ids

            # Update the state with new hooks
            if deleted_hook_ids or new_hook_ids:
                if deleted_hook_ids:
                    self.log.warning(
                        "Previously configured hooks %s were deleted for entity '%s', period: %ss",
                        deleted_hook_ids,
                        state.etype,
                        state.period,
                    )
                state.hook_ids = list(configured_hook_ids)
                self.cache.upsert(state)

        # Add newly configured hooks that are not in the saved states
        for thread_id, hooks in self.update_thread_hooks.items():
            if thread_id not in saved_states:
                state = UpdateThreadState.new(hooks, *thread_id)
                saved_states[thread_id] = state

        # Schedule the update threads
        for (period, entity_type, eid_only), state in saved_states.items():
            if state.finished:
                continue
            hooks = self.update_thread_hooks[(period, entity_type, eid_only)]
            if eid_only:
                processing_func = self._process_eid_update_batch
            else:
                processing_func = self._process_update_batch

            state.total = self.db.get_estimated_entity_count(entity_type)
            try:
                total_iterations = self._calculate_iteration_count(state.period)
                if state.iteration != 0 and total_iterations != state.total_iterations:
                    self.log.info("The update period was changed, resetting iteration number")
                    state.iteration = 0
                state.total_iterations = total_iterations
            except ValueError:
                self.log.error(
                    "Invalid period configuration for thread: %s, "
                    "the update batch period must be smaller than the total period ",
                    state,
                )
                raise

            self.scheduler.register(
                processing_func,
                func_args=[entity_type, hooks, state],
                **self.config.update_batch_cron.model_dump(),
            )

        # Connect the queue writer
        self.task_queue_writer.connect()
        self.task_queue_writer.check()  # check presence of needed exchanges

    def stop(self):
        """Stops the updater."""
        if not self.enabled:
            return

        self.task_queue_writer.disconnect()

    def _process_update_batch(self, entity_type: str, hooks: dict, state: UpdateThreadState):
        """Processes a batch of entities of the specified type.

        Args:
            entity_type: The entity type.
            hooks: The update hooks.
            state: The state of the update process.
        """
        self._process_batch(
            entity_type,
            hooks,
            state,
            record_getter=self.db.get_worker_master_records,
            hook_runner=self._run_hooks,
        )

    def _process_eid_update_batch(self, entity_type: str, hooks: dict, state: UpdateThreadState):
        """Processes a batch of entities of the specified type, only passing the entity ID.

        Args:
            entity_type: The entity type.
            hooks: The update hooks.
            state: The state of the update process.
        """
        projection = {"_id": True}
        self._process_batch(
            entity_type,
            hooks,
            state,
            record_getter=partial(self.db.get_worker_master_records, projection=projection),
            hook_runner=self._run_hooks_eid,
        )

    def _process_batch(
        self,
        entity_type: str,
        hooks: dict,
        state: UpdateThreadState,
        record_getter: Callable[[int, int, str], Cursor],
        hook_runner: Callable[[dict[str, Callable], str, dict], None],
    ):
        """Processes a batch of entities of the specified type using the specified `value_getter`.

        Args:
            entity_type: The entity type.
            hooks: The update hooks.
            state: The state of the update process.
            record_getter: Callable taking the (iteration, total_iterations, entity_type)
                to access the required record values.
            hook_runner: Callable taking the (hooks, etype, record) and running the hooks.
        """
        self.log.debug(
            "Processing update batch for '%s' over %.1fs, eid_only = %s",
            entity_type,
            state.period,
            state.eid_only,
        )
        start = datetime.now()
        iteration_cnt = state.total_iterations
        iteration = state.iteration

        self.log.debug(
            "Current state - processed entities: %s/%s, iteration: %s/%s, "
            "avg runtime per entity: %.3fs",
            state.processed,
            state.total,
            iteration,
            state.total_iterations,
            state.runtime_secs / state.processed if state.processed > 0 else 0,
        )
        records = record_getter(iteration, iteration_cnt, entity_type)

        for record in records:
            hook_runner(hooks, entity_type, record)

            state.processed += 1
        state.t_last_update = datetime.now()

        duration = state.t_last_update - start
        state.runtime_secs += duration.total_seconds()

        if state.runtime_secs > state.period:
            self.log.warning(
                "%s hooks: %s, The total hook runtime %.2fs exceeds the desired period %.2fs. "
                "(Current average is %.3fs per entity when limit is %.3fs) "
                "Consider optimizing the hooks or extending the update period.",
                entity_type,
                state.hook_ids,
                state.runtime_secs,
                state.period,
                state.runtime_secs / state.processed,
                state.period / state.total,
            )

        state.iteration = iteration + 1
        if state.iteration >= iteration_cnt:
            state.finished = True

        self.cache.upsert(state)

        if state.finished:
            self.log.debug(
                "Finished processing '%s' entity with period: %s", entity_type, state.period
            )
            state.reset()
            state.total = self.db.get_estimated_entity_count(entity_type)

    def _calculate_iteration_count(self, period_seconds: float) -> int:
        total_iterations = int(period_seconds // self.config.update_batch_period.total_seconds())
        if total_iterations == 0:
            raise ValueError("The total period is shorter than the update batch period.")
        return total_iterations

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

    def _run_hooks_eid(self, hooks: dict[str, Callable], entity_type: str, record: dict):
        tasks = []
        with task_context(self.model_spec):
            for hook_id, hook in hooks.items():
                self.log.debug("Running hook: '%s'", hook_id)
                try:
                    new_tasks = hook(entity_type, record["_id"])
                    tasks.extend(new_tasks)
                except Exception as e:
                    self.elog.log("module_error")
                    self.log.error(f"Error during running hook {hook}: {e}")

        for task in tasks:
            self.task_queue_writer.put_task(task)
