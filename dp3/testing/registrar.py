"""Test callback registrar for DP3 secondary modules."""

import copy
import logging
import warnings
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import timedelta
from functools import partial
from typing import Any, Callable, Optional, Union

from apscheduler.triggers.cron import CronTrigger
from event_count_logger import DummyEventGroup

from dp3.common.callback_registrar import (
    _drop_master,
    on_attr_change_in_snapshots,
    on_entity_creation_in_snapshots,
    unset_flag,
)
from dp3.common.config import ModelSpec
from dp3.common.datapoint import DataPointBase
from dp3.common.hook_types import ATTR_TYPE_TO_ON_NEW_HOOK
from dp3.common.task import DataPointTask, task_context
from dp3.common.utils import get_func_name, parse_time_duration
from dp3.core.updater import (
    UpdateThreadId,
    get_update_thread_hooks,
    validate_update_period,
)
from dp3.snapshots.snapshot_hooks import (
    SnapshotCorrelationHookContainer,
    SnapshotTimeseriesHookContainer,
)


@dataclass
class HookRegistration:
    """Captured callback registration made by a secondary module."""

    kind: str
    hook: Callable
    entity: Optional[str] = None
    attr: Optional[str] = None
    hook_type: Optional[str] = None
    hook_id: Optional[str] = None
    entity_type: Optional[str] = None
    attr_type: Optional[str] = None
    depends_on: list[list[str]] = field(default_factory=list)
    may_change: list[list[str]] = field(default_factory=list)
    refresh: Any = None
    period: Any = None
    deprecated: bool = False
    extra: dict[str, Any] = field(default_factory=dict)


class TestCallbackRegistrar:
    """Callback registrar implementation for module unit tests."""

    def __init__(
        self,
        model_spec: ModelSpec,
        log: Optional[logging.Logger] = None,
        update_batch_period: Any = None,
    ):
        self.model_spec = model_spec
        self.log = log or logging.getLogger(self.__class__.__name__)
        self.update_batch_period = update_batch_period
        self.registrations: list[HookRegistration] = []
        self._task_hooks: defaultdict[str, list[HookRegistration]] = defaultdict(list)
        self._allow_creation_hooks: defaultdict[str, list[HookRegistration]] = defaultdict(list)
        self._on_creation_hooks: defaultdict[str, list[HookRegistration]] = defaultdict(list)
        self._attr_hooks: defaultdict[tuple[str, str], list[HookRegistration]] = defaultdict(list)
        self._snapshot_init_hooks: list[HookRegistration] = []
        self._snapshot_finalize_hooks: list[HookRegistration] = []
        self._periodic_record_hooks: defaultdict[
            tuple[float, str, bool], dict[str, HookRegistration]
        ] = defaultdict(dict)
        self._periodic_eid_hooks: defaultdict[
            tuple[float, str, bool], dict[str, HookRegistration]
        ] = defaultdict(dict)
        self._scheduler_jobs: list[HookRegistration] = []

        self._correlation_hooks = SnapshotCorrelationHookContainer(
            self.log, model_spec, DummyEventGroup()
        )
        self._timeseries_hooks = SnapshotTimeseriesHookContainer(
            self.log, model_spec, DummyEventGroup()
        )

    def scheduler_register(  # noqa: PLR0913
        self,
        func: Callable,
        *,
        func_args: Union[list, tuple] = None,
        func_kwargs: dict = None,
        year: Union[int, str] = None,
        month: Union[int, str] = None,
        day: Union[int, str] = None,
        week: Union[int, str] = None,
        day_of_week: Union[int, str] = None,
        hour: Union[int, str] = None,
        minute: Union[int, str] = None,
        second: Union[int, str] = None,
        timezone: str = "UTC",
        misfire_grace_time: int = 1,
    ) -> int:
        CronTrigger(year, month, day, week, day_of_week, hour, minute, second, timezone=timezone)
        job_id = len(self._scheduler_jobs) + 1
        schedule = {
            "year": year,
            "month": month,
            "day": day,
            "week": week,
            "day_of_week": day_of_week,
            "hour": hour,
            "minute": minute,
            "second": second,
            "timezone": timezone,
            "misfire_grace_time": misfire_grace_time,
        }
        reg = HookRegistration(
            kind="scheduler",
            hook=func,
            extra={
                "func_args": list(func_args or []),
                "func_kwargs": dict(func_kwargs or {}),
                "schedule": schedule,
                "job_id": job_id,
            },
        )
        self._record(reg)
        self._scheduler_jobs.append(reg)
        return job_id

    def register_task_hook(self, hook_type: str, hook: Callable):
        if hook_type != "on_task_start":
            raise ValueError(f"Hook type '{hook_type}' doesn't exist.")
        reg = HookRegistration(kind="task", hook_type=hook_type, hook=hook)
        self._record(reg)
        self._task_hooks[hook_type].append(reg)

    def register_allow_entity_creation_hook(self, hook: Callable, entity: str):
        self._validate_entity(entity)
        reg = HookRegistration(kind="allow_entity_creation", entity=entity, hook=hook)
        self._record(reg)
        self._allow_creation_hooks[entity].append(reg)

    def register_on_entity_creation_hook(
        self,
        hook: Callable,
        entity: str,
        refresh: Any = None,
        may_change: list[list[str]] = None,
    ):
        self._validate_entity(entity)
        if refresh is not None and may_change is None:
            raise ValueError("'may_change' must be specified if 'refresh' is specified")
        reg = HookRegistration(
            kind="on_entity_creation",
            entity=entity,
            hook=hook,
            refresh=refresh,
            may_change=copy.deepcopy(may_change or []),
        )
        self._record(reg)
        self._on_creation_hooks[entity].append(reg)
        if refresh is not None:
            self._register_refreshed_entity_creation_hook(hook, entity, refresh, may_change)

    def register_on_new_attr_hook(
        self,
        hook: Callable,
        entity: str,
        attr: str,
        refresh: Any = None,
        may_change: list[list[str]] = None,
    ):
        hook_type = self._hook_type_for_attr(entity, attr)
        if refresh is not None and may_change is None:
            raise ValueError("'may_change' must be specified if 'refresh' is specified")
        reg = HookRegistration(
            kind="on_new_attr",
            hook_type=hook_type,
            entity=entity,
            attr=attr,
            hook=hook,
            refresh=refresh,
            may_change=copy.deepcopy(may_change or []),
        )
        self._record(reg)
        self._attr_hooks[entity, attr].append(reg)
        if refresh is not None:
            self._register_refreshed_attr_hook(hook, entity, attr, refresh, may_change)

    def register_entity_hook(self, hook_type: str, hook: Callable, entity: str):
        warnings.warn(
            "register_entity_hook() is deprecated; use "
            "register_allow_entity_creation_hook() or register_on_entity_creation_hook().",
            DeprecationWarning,
            stacklevel=2,
        )
        self._validate_entity(entity)
        if hook_type == "allow_entity_creation":
            reg = HookRegistration(
                kind="allow_entity_creation", entity=entity, hook=hook, deprecated=True
            )
            self._record(reg)
            self._allow_creation_hooks[entity].append(reg)
            return
        if hook_type == "on_entity_creation":
            reg = HookRegistration(
                kind="on_entity_creation", entity=entity, hook=hook, deprecated=True
            )
            self._record(reg)
            self._on_creation_hooks[entity].append(reg)
            return
        raise ValueError(f"Hook type '{hook_type}' doesn't exist.")

    def register_attr_hook(self, hook_type: str, hook: Callable, entity: str, attr: str):
        warnings.warn(
            "register_attr_hook() is deprecated; use register_on_new_attr_hook().",
            DeprecationWarning,
            stacklevel=2,
        )
        expected_hook_type = self._hook_type_for_attr(entity, attr)
        if hook_type != expected_hook_type:
            raise ValueError(f"Hook type '{hook_type}' doesn't exist for {entity}/{attr}.")
        reg = HookRegistration(
            kind="on_new_attr",
            hook_type=hook_type,
            entity=entity,
            attr=attr,
            hook=hook,
            deprecated=True,
        )
        self._record(reg)
        self._attr_hooks[entity, attr].append(reg)

    def register_timeseries_hook(self, hook: Callable, entity_type: str, attr_type: str):
        self._timeseries_hooks.register(hook, entity_type, attr_type)
        reg = HookRegistration(
            kind="timeseries", hook=hook, entity_type=entity_type, attr_type=attr_type
        )
        self._record(reg)

    def register_correlation_hook(
        self,
        hook: Callable,
        entity_type: str,
        depends_on: list[list[str]],
        may_change: list[list[str]],
    ):
        wrapped = _drop_master(hook)
        hook_id = self._correlation_hooks.register(wrapped, entity_type, depends_on, may_change)
        reg = HookRegistration(
            kind="correlation",
            hook=hook,
            hook_id=hook_id,
            entity_type=entity_type,
            depends_on=copy.deepcopy(depends_on),
            may_change=copy.deepcopy(may_change),
        )
        self._record(reg)

    def register_correlation_hook_with_master_record(
        self,
        hook: Callable,
        entity_type: str,
        depends_on: list[list[str]],
        may_change: list[list[str]],
    ):
        hook_id = self._correlation_hooks.register(hook, entity_type, depends_on, may_change)
        reg = HookRegistration(
            kind="correlation_with_master_record",
            hook=hook,
            hook_id=hook_id,
            entity_type=entity_type,
            depends_on=copy.deepcopy(depends_on),
            may_change=copy.deepcopy(may_change),
        )
        self._record(reg)

    def register_snapshot_init_hook(self, hook: Callable):
        reg = HookRegistration(kind="snapshot_init", hook=hook)
        self._record(reg)
        self._snapshot_init_hooks.append(reg)

    def register_snapshot_finalize_hook(self, hook: Callable):
        reg = HookRegistration(kind="snapshot_finalize", hook=hook)
        self._record(reg)
        self._snapshot_finalize_hooks.append(reg)

    def register_periodic_update_hook(
        self, hook: Callable, hook_id: str, entity_type: str, period: Any
    ):
        self._validate_entity(entity_type)
        period_seconds = self._period_seconds(period)
        hooks = self._validate_periodic_hook(
            self._periodic_record_hooks, entity_type, hook_id, period_seconds, eid_only=False
        )
        reg = HookRegistration(
            kind="periodic_update",
            hook=hook,
            hook_id=hook_id,
            entity_type=entity_type,
            period=period,
        )
        self._record(reg)
        hooks[hook_id] = reg

    def register_periodic_eid_update_hook(
        self, hook: Callable, hook_id: str, entity_type: str, period: Any
    ):
        self._validate_entity(entity_type)
        period_seconds = self._period_seconds(period)
        hooks = self._validate_periodic_hook(
            self._periodic_eid_hooks, entity_type, hook_id, period_seconds, eid_only=True
        )
        reg = HookRegistration(
            kind="periodic_eid_update",
            hook=hook,
            hook_id=hook_id,
            entity_type=entity_type,
            period=period,
        )
        self._record(reg)
        hooks[hook_id] = reg

    def run_task_hooks(self, hook_type: str, task: DataPointTask) -> None:
        for reg in self._task_hooks[hook_type]:
            reg.hook(task)

    def run_allow_entity_creation(self, entity: str, eid: Any, task: DataPointTask) -> bool:
        return all(reg.hook(eid, task) for reg in self._allow_creation_hooks[entity])

    def run_on_entity_creation(
        self, entity: str, eid: Any, task: DataPointTask
    ) -> list[DataPointTask]:
        tasks: list[DataPointTask] = []
        with task_context(self.model_spec):
            for reg in self._on_creation_hooks[entity]:
                hook_tasks = reg.hook(eid, task)
                if isinstance(hook_tasks, list):
                    tasks.extend(hook_tasks)
        return tasks

    def run_on_new_attr(self, entity: str, attr: str, eid: Any, dp: DataPointBase):
        tasks: list[DataPointTask] = []
        with task_context(self.model_spec):
            for reg in self._attr_hooks[entity, attr]:
                hook_tasks = reg.hook(eid, dp)
                if isinstance(hook_tasks, list):
                    tasks.extend(hook_tasks)
        return tasks

    def run_timeseries_hook(
        self, entity_type: str, attr_type: str, attr_history: list[dict]
    ) -> list[DataPointTask]:
        tasks: list[DataPointTask] = []
        with task_context(self.model_spec):
            for hook in self._timeseries_hooks._hooks[entity_type, attr_type]:
                hook_tasks = hook(entity_type, attr_type, attr_history)
                tasks.extend(hook_tasks)
        return tasks

    def run_correlation_hooks(
        self,
        entity_type: str,
        record: dict,
        master_record: Optional[dict] = None,
    ) -> list[DataPointTask]:
        eid = self._assert_record_eid(record)
        return self.run_correlation_hooks_for_entities(
            {(entity_type, eid): record}, {(entity_type, eid): master_record or {}}
        )

    def run_correlation_hooks_for_entities(
        self, entities: dict[tuple[str, Any], dict], master_records: Optional[dict] = None
    ) -> list[DataPointTask]:
        master_records = master_records or {}
        for entity_type, _ in entities:
            self._validate_entity(entity_type)
        for record in entities.values():
            self._assert_record_eid(record)

        entity_types = {etype for etype, _ in entities}
        hook_subset = [
            (hook_id, hook, etype)
            for etype in entity_types
            for hook_id, hook in self._correlation_hooks._hooks[etype]
        ]
        topological_order = self._correlation_hooks._dependency_graph.topological_order
        hook_subset.sort(key=lambda item: topological_order.index(item[0]))

        tasks: list[DataPointTask] = []
        with task_context(self.model_spec):
            for _, hook, etype in hook_subset:
                for entity_key, record in entities.items():
                    if entity_key[0] != etype:
                        continue
                    hook_tasks = hook(etype, record, master_records.get(entity_key, {}))
                    if hook_tasks is not None and hook_tasks:
                        tasks.extend(hook_tasks)
        return tasks

    def run_snapshot_init_hooks(self) -> list[DataPointTask]:
        return self._run_no_arg_hooks(self._snapshot_init_hooks)

    def run_snapshot_finalize_hooks(self) -> list[DataPointTask]:
        return self._run_no_arg_hooks(self._snapshot_finalize_hooks)

    def run_periodic_update(
        self,
        entity_type: str,
        eid: Any,
        master_record: dict,
        hook_id: Optional[str] = None,
    ) -> list[DataPointTask]:
        hooks = self._matching_update_hooks(self._periodic_record_hooks, entity_type, hook_id)
        tasks: list[DataPointTask] = []
        with task_context(self.model_spec):
            for reg in hooks:
                hook_tasks = reg.hook(entity_type, eid, master_record)
                tasks.extend(hook_tasks)
        return tasks

    def run_periodic_eid_update(
        self, entity_type: str, eid: Any, hook_id: Optional[str] = None
    ) -> list[DataPointTask]:
        hooks = self._matching_update_hooks(self._periodic_eid_hooks, entity_type, hook_id)
        tasks: list[DataPointTask] = []
        with task_context(self.model_spec):
            for reg in hooks:
                hook_tasks = reg.hook(entity_type, eid)
                tasks.extend(hook_tasks)
        return tasks

    def get_scheduler_job(
        self, job: Union[int, str, Callable, HookRegistration]
    ) -> HookRegistration:
        """Return a registered scheduler job by id, callable, or callable name."""
        if isinstance(job, int):
            for reg in self._scheduler_jobs:
                if reg.extra["job_id"] == job:
                    return reg
            raise ValueError(f"No scheduler job has id {job!r}.")
        if isinstance(job, HookRegistration):
            if job.kind != "scheduler":
                raise ValueError(f"Registration kind '{job.kind}' is not a scheduler job.")
            return job

        matches = [reg for reg in self._scheduler_jobs if _callable_matches(reg.hook, job)]
        if not matches:
            raise ValueError(f"No scheduler job matches {job!r}.")
        if len(matches) > 1:
            raise ValueError(f"Multiple scheduler jobs match {job!r}.")
        return matches[0]

    def run_scheduler_job(self, job: Union[int, str, Callable, HookRegistration]):
        reg = self.get_scheduler_job(job)
        return reg.hook(*reg.extra["func_args"], **reg.extra["func_kwargs"])

    def _register_refreshed_entity_creation_hook(
        self, hook: Callable, entity: str, refresh: Any, may_change: list[list[str]]
    ) -> None:
        correlation_hook = partial(on_entity_creation_in_snapshots, self.model_spec, refresh, hook)
        self._register_refresh_correlation_hook(correlation_hook, entity, [], may_change)
        self._register_refresh_finalize_hook(refresh)

    def _register_refreshed_attr_hook(
        self, hook: Callable, entity: str, attr: str, refresh: Any, may_change: list[list[str]]
    ) -> None:
        correlation_hook = partial(on_attr_change_in_snapshots, self.model_spec, refresh, hook)
        self._register_refresh_correlation_hook(correlation_hook, entity, [[attr]], may_change)
        self._register_refresh_finalize_hook(refresh)

    def _register_refresh_correlation_hook(
        self,
        hook: Callable,
        entity_type: str,
        depends_on: list[list[str]],
        may_change: list[list[str]],
    ) -> None:
        wrapped = _drop_master(hook)
        hook_id = self._correlation_hooks.register(wrapped, entity_type, depends_on, may_change)
        self._record(
            HookRegistration(
                kind="correlation",
                hook=hook,
                hook_id=hook_id,
                entity_type=entity_type,
                depends_on=copy.deepcopy(depends_on),
                may_change=copy.deepcopy(may_change),
            )
        )

    def _register_refresh_finalize_hook(self, refresh: Any) -> None:
        hook = partial(unset_flag, refresh)
        reg = HookRegistration(kind="snapshot_finalize", hook=hook)
        self._record(reg)
        self._snapshot_finalize_hooks.append(reg)

    def _record(self, registration: HookRegistration) -> None:
        self.registrations.append(registration)

    def _validate_entity(self, entity: str) -> None:
        if entity not in self.model_spec.entities:
            raise ValueError(f"Entity '{entity}' does not exist.")

    def _hook_type_for_attr(self, entity: str, attr: str) -> str:
        try:
            return ATTR_TYPE_TO_ON_NEW_HOOK[self.model_spec.attributes[entity, attr].t]
        except KeyError as e:
            raise ValueError(
                f"Cannot register hook for attribute {entity}/{attr}, are you sure it exists?"
            ) from e

    @staticmethod
    def _assert_record_eid(record: dict) -> Any:
        if "eid" not in record:
            raise ValueError("Correlation hook records must contain an 'eid' field.")
        return record["eid"]

    def _validate_periodic_hook(
        self,
        update_thread_hooks: dict,
        entity_type: str,
        hook_id: str,
        period_seconds: float,
        eid_only: bool,
    ) -> dict:
        update_period_seconds = self._update_batch_period_seconds()
        if update_period_seconds is not None:
            validate_update_period(period_seconds, update_period_seconds)
        thread_id: UpdateThreadId = (period_seconds, entity_type, eid_only)
        return get_update_thread_hooks(update_thread_hooks, hook_id, thread_id)

    def _update_batch_period_seconds(self) -> Optional[float]:
        if self.update_batch_period is None:
            return None
        return self._period_seconds(self.update_batch_period)

    @staticmethod
    def _period_seconds(period: Any) -> float:
        if isinstance(period, timedelta):
            return period.total_seconds()
        return parse_time_duration(period).total_seconds()

    def _run_no_arg_hooks(self, hooks: list[HookRegistration]) -> list[DataPointTask]:
        tasks: list[DataPointTask] = []
        with task_context(self.model_spec):
            for reg in hooks:
                hook_tasks = reg.hook()
                tasks.extend(hook_tasks)
        return tasks

    @staticmethod
    def _matching_update_hooks(hooks: dict, entity_type: str, hook_id: Optional[str]):
        matches = []
        for (_, etype, _), thread_hooks in hooks.items():
            if etype != entity_type:
                continue
            if hook_id is not None:
                if hook_id in thread_hooks:
                    matches.append(thread_hooks[hook_id])
            else:
                matches.extend(thread_hooks.values())
        return matches


def _callable_matches(func: Callable, expected: Union[str, Callable]) -> bool:
    if callable(expected):
        return func == expected
    func_name = get_func_name(func)
    return func_name == expected or func_name.endswith(f".{expected}")
