"""Core module that executes periodic update callbacks."""

import logging
from collections import defaultdict
from collections.abc import Callable, Iterator
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from functools import partial
from typing import Literal

from event_count_logger import DummyEventGroup
from pydantic import BaseModel, Field, validate_call
from pymongo.cursor import Cursor
from pymongo.results import UpdateResult

from dp3.common.config import CronExpression, PlatformConfig
from dp3.common.hook_telemetry import HookTelemetry
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
        batch: Whether hooks are called once per batch instead of once per record.
        hook_ids: Hook ids.
        runtime_secs: Total hook runtime in seconds.
        slowest_batch_secs: Runtime of the slowest batch in seconds.
        metrics: Metrics returned by batch hooks, summed over the period, by hook id.
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
    batch: bool = False
    hook_ids: list[str]
    runtime_secs: float = 0.0
    slowest_batch_secs: float = 0.0
    metrics: dict[str, dict[str, float]] = Field(default_factory=dict)

    @classmethod
    def new(
        cls,
        hooks: dict,
        period: float,
        entity_type: str,
        eid_only: bool = False,
        batch: bool = False,
    ):
        """Create a new instance initialized with hooks and thread_id components."""
        now = _now_ms()
        return cls(
            t_created=now,
            t_last_update=now,
            t_end=now + timedelta(seconds=period),
            period=period,
            etype=entity_type,
            eid_only=eid_only,
            batch=batch,
            hook_ids=hooks.keys(),
        )

    @staticmethod
    def id_attributes():
        """A list of attributes which identify the state in cache."""
        return ["type", "period", "etype", "eid_only", "batch", "t_created"]

    @property
    def thread_id(self) -> "UpdateThreadId":
        """A tuple of (period, entity_type, eid_only, batch)."""
        return self.period, self.etype, self.eid_only, self.batch

    def reset(self):
        """Resets counters and timestamps."""
        now = _now_ms()
        self.t_created = now
        self.t_last_update = now
        self.t_end = now + timedelta(seconds=self.period)
        self.iteration = 0
        self.processed = 0
        self.runtime_secs = 0.0
        self.slowest_batch_secs = 0.0
        self.metrics = {}
        self.finished = False


def _now_ms() -> datetime:
    """Current time truncated to milliseconds, the precision of datetimes stored in MongoDB.

    Thread states keep the same `t_created` before and after being saved and loaded again.
    """
    now = datetime.now(UTC)
    return now.replace(microsecond=now.microsecond // 1000 * 1000)


@dataclass(frozen=True)
class UpdateBatch:
    """A batch of entities passed to an internal batch update hook.

    Attributes:
        query_filter: Filter selecting the master records of this batch.
        iteration: Index of this batch within the current period.
        total_iterations: Number of batches in a period.
        period_start: Start of the current period, identifies it across restarts.
    """

    query_filter: dict
    iteration: int
    total_iterations: int
    period_start: datetime


@dataclass(frozen=True)
class UpdatePeriod:
    """A finished period of a batch update thread, passed to period end callbacks.

    Attributes:
        period_start: Start of the period.
        period_end: End of the last batch of the period.
        runtime_secs: Total runtime of the batches in seconds.
        slowest_batch_secs: Runtime of the slowest batch in seconds.
        metrics: Metrics returned by the hook, summed over the batches of the period.
    """

    period_start: datetime
    period_end: datetime
    runtime_secs: float
    slowest_batch_secs: float
    metrics: dict[str, float]


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
        if not state.batch:
            # States saved before batch threads were introduced have no `batch` field
            filter_dict["batch"] = {"$ne": True}
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


UpdateThreadId = tuple[float, str, bool, bool]


def validate_update_period(period: float, update_batch_period: float) -> None:
    """Validate that an update hook period can be processed by updater batches."""
    if period < update_batch_period:
        raise ValueError(
            f"The total period ({period}s) must be greater than or equal to "
            f"the update batch period ({update_batch_period}s)."
        )


def get_update_thread_hooks(
    update_thread_hooks: dict[UpdateThreadId, dict], hook_id: str, thread_id: UpdateThreadId
) -> dict:
    """Return the updater thread hook mapping after checking hook ID uniqueness."""
    hooks = update_thread_hooks[thread_id]
    _period, entity_type, _eid_only, _batch = thread_id
    if hook_id in hooks:
        raise ValueError(f"Hook ID {hook_id} already registered for {entity_type}.")
    return hooks


class Updater:
    """Executes periodic update callbacks."""

    def __init__(
        self,
        db: EntityDatabase,
        task_queue_writer: TaskQueueWriter,
        platform_config: PlatformConfig,
        scheduler: Scheduler,
        elog: EventGroupType,
        hook_elog: EventGroupType | None = None,
    ):
        self.log = logging.getLogger("Updater")
        self.elog = elog
        self.hook_telemetry = HookTelemetry(
            hook_elog if hook_elog is not None else DummyEventGroup()
        )

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
        self._period_end_callbacks: dict[UpdateThreadId, dict[str, Callable]] = defaultdict(dict)

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

    def register_batch_update_hook(
        self,
        hook: Callable[[str, UpdateBatch], dict[str, float]],
        hook_id: str,
        entity_type: str,
        period: timedelta,
        on_period_end: Callable[[str, UpdatePeriod], None] | None = None,
    ):
        """Registers an internal hook called once per batch of entities of the specified type.

        Over the `period`, the hook is called for every batch with a filter selecting its master
        records, so that the hook can process them all by a single database operation.

        The hook returns metrics of the batch, which are summed over the period and saved
        with the progress of the thread. After the last batch of a period, `on_period_end`
        is called with the totals. If the worker stops before the progress is saved,
        the last batch is repeated and `on_period_end` is called again with the same totals.

        Intended for DP3 maintenance tasks, which have to modify the database directly.
        Modules should register record or eid update hooks returning datapoints instead.
        """
        if not self.enabled:
            return

        period_secs = period.total_seconds()
        validate_update_period(period_secs, self.config.update_batch_period.total_seconds())

        thread_id: UpdateThreadId = (period_secs, entity_type, False, True)
        hooks = get_update_thread_hooks(self.update_thread_hooks, hook_id, thread_id)
        self.log.info(
            "Registered batch hook '%s' to thread processing entity '%s' over %.1fs",
            hook_id,
            entity_type,
            period_secs,
        )
        hooks[hook_id] = hook
        if on_period_end is not None:
            self._period_end_callbacks[thread_id][hook_id] = on_period_end

    def _register_hook(self, hook, hook_id: str, entity_type: str, period: float, eid_only: bool):
        if not self.enabled:
            return

        update_period_secs = self.config.update_batch_period.total_seconds()
        validate_update_period(period, update_period_secs)

        thread_id: UpdateThreadId = (period, entity_type, eid_only, False)
        hooks = get_update_thread_hooks(self.update_thread_hooks, hook_id, thread_id)
        self.log.info(
            "Registered hook '%s' to thread processing entity '%s' over %.1fs, eid_only = %s",
            hook_id,
            entity_type,
            period,
            eid_only,
        )
        hook_type = "periodic_eid_update" if eid_only else "periodic_update"
        hooks[hook_id] = self.hook_telemetry.wrap(
            hook_type,
            hook,
            entity_type,
            hook_id,
            f"period={period:g}s",
        )

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
        for thread_id, state in saved_states.items():
            if state.finished:
                continue
            hooks = self.update_thread_hooks[thread_id]
            _period, entity_type, eid_only, batch = thread_id
            if batch:
                processing_func = self._process_batch_hooks
            elif eid_only:
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

    def _process_batch_hooks(self, entity_type: str, hooks: dict, state: UpdateThreadState):
        """Calls batch hooks once for a batch of entities of the specified type.

        Args:
            entity_type: The entity type.
            hooks: The batch hooks.
            state: The state of the update process.
        """
        if self._retry_finished_save(entity_type, state):
            return
        start = datetime.now(UTC)
        batch = UpdateBatch(
            query_filter={"#hash": {"$mod": [state.total_iterations, state.iteration]}},
            iteration=state.iteration,
            total_iterations=state.total_iterations,
            period_start=state.t_created,
        )
        for hook_id, hook in hooks.items():
            try:
                batch_metrics = hook(entity_type, batch)
            except Exception as e:
                self.elog.log("module_error")
                self.log.exception("Error during running batch hook '%s': %s", hook_id, e)
                continue
            hook_metrics = state.metrics.setdefault(hook_id, {})
            for name, value in batch_metrics.items():
                hook_metrics[name] = hook_metrics.get(name, 0) + value
        self._finish_batch(entity_type, state, start, on_finished=self._end_batch_period)

    def _end_batch_period(self, entity_type: str, state: UpdateThreadState):
        """Calls period end callbacks of a batch thread with the totals of the finished period."""
        for hook_id, callback in self._period_end_callbacks.get(state.thread_id, {}).items():
            period = UpdatePeriod(
                period_start=state.t_created,
                period_end=state.t_last_update,
                runtime_secs=state.runtime_secs,
                slowest_batch_secs=state.slowest_batch_secs,
                metrics=dict(state.metrics.get(hook_id, {})),
            )
            try:
                callback(entity_type, period)
            except Exception as e:
                self.elog.log("module_error")
                self.log.exception("Error during period end of batch hook '%s': %s", hook_id, e)

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
        if self._retry_finished_save(entity_type, state):
            return
        self.log.debug(
            "Processing update batch for '%s' over %.1fs, eid_only = %s",
            entity_type,
            state.period,
            state.eid_only,
        )
        start = datetime.now(UTC)
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
        self._finish_batch(entity_type, state, start)

    def _finish_batch(
        self,
        entity_type: str,
        state: UpdateThreadState,
        start: datetime,
        on_finished: Callable[[str, UpdateThreadState], None] | None = None,
    ):
        """Records the batch runtime and advances the state to the next batch.

        `on_finished` is called after the last batch of the period, before the state is saved.
        """
        state.t_last_update = datetime.now(UTC)

        duration = (state.t_last_update - start).total_seconds()
        state.runtime_secs += duration
        state.slowest_batch_secs = max(state.slowest_batch_secs, duration)

        if state.runtime_secs > state.period:
            if state.total > 0:
                self.log.warning(
                    "%s hooks: %s, The total hook runtime %.2fs exceeds the desired period %.2fs. "
                    "(Current average is %.3fs per entity when limit is %.3fs) "
                    "Consider optimizing the hooks or extending the update period.",
                    entity_type,
                    state.hook_ids,
                    state.runtime_secs,
                    state.period,
                    state.runtime_secs / state.processed if state.processed > 0 else 0.0,
                    state.period / state.total,
                )
            else:
                # `state.total` is an estimate (MongoDB estimated_document_count)
                # and may be 0 for an empty or newly created collection, even when
                # records are present. Without it the per-entity timing budget is
                # undefined, so report the runtime against the processed count.
                self.log.warning(
                    "%s hooks: %s, The total hook runtime %.2fs exceeds the desired "
                    "period %.2fs. The estimated entity count is 0 (processed %s so far), "
                    "so a per-entity timing limit cannot be determined.",
                    entity_type,
                    state.hook_ids,
                    state.runtime_secs,
                    state.period,
                    state.processed,
                )

        state.iteration += 1
        if state.iteration >= state.total_iterations:
            state.finished = True
            if on_finished is not None:
                on_finished(entity_type, state)

        self._save_state(entity_type, state)

    def _save_state(self, entity_type: str, state: UpdateThreadState):
        """Saves the state, a finished period is reset to start the next one once it is saved."""
        self.cache.upsert(state)

        if state.finished:
            self.log.debug(
                "Finished processing '%s' entity with period: %s", entity_type, state.period
            )
            state.reset()
            state.total = self.db.get_estimated_entity_count(entity_type)

    def _retry_finished_save(self, entity_type: str, state: UpdateThreadState) -> bool:
        """Retries saving a finished period whose save has failed, returns True if it was needed.

        No batch is processed until then, so that the finished period is not processed again.
        """
        if not state.finished:
            return False
        self.log.warning("Retrying the failed save of the finished period of '%s'", entity_type)
        self._save_state(entity_type, state)
        return True

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
                    if isinstance(new_tasks, list):
                        hook.log("created_tasks", len(new_tasks))
                except Exception as e:
                    self.elog.log("module_error")
                    self.log.error(f"Error during running hook {hook.callback}: {e}")

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
                    if isinstance(new_tasks, list):
                        hook.log("created_tasks", len(new_tasks))
                except Exception as e:
                    self.elog.log("module_error")
                    self.log.error(f"Error during running hook {hook.callback}: {e}")

        for task in tasks:
            self.task_queue_writer.put_task(task)
