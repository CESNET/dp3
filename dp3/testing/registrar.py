"""Test callback registrar for DP3 secondary modules."""

import copy
import logging
import warnings
from collections import defaultdict
from dataclasses import dataclass, field
from functools import wraps
from typing import Any, Callable, Optional, Union

from event_count_logger import DummyEventGroup

from dp3.common.attrspec import AttrType
from dp3.common.config import ModelSpec
from dp3.common.datapoint import DataPointBase
from dp3.common.task import DataPointTask, task_context
from dp3.common.utils import get_func_name
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


def _drop_master_for_test(hook: Callable[[str, dict], Any]) -> Callable[[str, dict, dict], Any]:
    @wraps(hook)
    def wrapped(entity_type: str, record: dict, _master_record: dict):
        return hook(entity_type, record)

    return wrapped


class TestCallbackRegistrar:
    """Callback registrar implementation for module unit tests."""

    attr_spec_t_to_on_attr = {
        AttrType.PLAIN: "on_new_plain",
        AttrType.OBSERVATIONS: "on_new_observation",
        AttrType.TIMESERIES: "on_new_ts_chunk",
    }

    def __init__(self, model_spec: ModelSpec, log: Optional[logging.Logger] = None):
        self.model_spec = model_spec
        self.log = log or logging.getLogger(self.__class__.__name__)
        self.registrations: list[HookRegistration] = []
        self._task_hooks: defaultdict[str, list[HookRegistration]] = defaultdict(list)
        self._allow_creation_hooks: defaultdict[str, list[HookRegistration]] = defaultdict(list)
        self._on_creation_hooks: defaultdict[str, list[HookRegistration]] = defaultdict(list)
        self._attr_hooks: defaultdict[tuple[str, str], list[HookRegistration]] = defaultdict(list)
        self._snapshot_init_hooks: list[HookRegistration] = []
        self._snapshot_finalize_hooks: list[HookRegistration] = []
        self._periodic_record_hooks: defaultdict[tuple[str, str], list[HookRegistration]] = (
            defaultdict(list)
        )
        self._periodic_eid_hooks: defaultdict[tuple[str, str], list[HookRegistration]] = (
            defaultdict(list)
        )
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
            },
        )
        self._record(reg)
        self._scheduler_jobs.append(reg)
        return len(self._scheduler_jobs) - 1

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
        wrapped = _drop_master_for_test(hook)
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
        reg = HookRegistration(
            kind="periodic_update",
            hook=hook,
            hook_id=hook_id,
            entity_type=entity_type,
            period=period,
        )
        self._record(reg)
        self._periodic_record_hooks[entity_type, hook_id].append(reg)

    def register_periodic_eid_update_hook(
        self, hook: Callable, hook_id: str, entity_type: str, period: Any
    ):
        self._validate_entity(entity_type)
        reg = HookRegistration(
            kind="periodic_eid_update",
            hook=hook,
            hook_id=hook_id,
            entity_type=entity_type,
            period=period,
        )
        self._record(reg)
        self._periodic_eid_hooks[entity_type, hook_id].append(reg)

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
                if isinstance(hook_tasks, list):
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
                    if isinstance(hook_tasks, list):
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
                if isinstance(hook_tasks, list):
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
                if isinstance(hook_tasks, list):
                    tasks.extend(hook_tasks)
        return tasks

    def get_scheduler_job(
        self, job: Union[int, str, Callable, HookRegistration]
    ) -> HookRegistration:
        """Return a registered scheduler job by index, callable, or callable name."""
        if isinstance(job, int):
            return self._scheduler_jobs[job]
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

    def _record(self, registration: HookRegistration) -> None:
        self.registrations.append(registration)

    def _validate_entity(self, entity: str) -> None:
        if entity not in self.model_spec.entities:
            raise ValueError(f"Entity '{entity}' does not exist.")

    def _hook_type_for_attr(self, entity: str, attr: str) -> str:
        try:
            return self.attr_spec_t_to_on_attr[self.model_spec.attributes[entity, attr].t]
        except KeyError as e:
            raise ValueError(
                f"Cannot register hook for attribute {entity}/{attr}, are you sure it exists?"
            ) from e

    @staticmethod
    def _assert_record_eid(record: dict) -> Any:
        if "eid" not in record:
            raise ValueError("Correlation hook records must contain an 'eid' field.")
        return record["eid"]

    def _run_no_arg_hooks(self, hooks: list[HookRegistration]) -> list[DataPointTask]:
        tasks: list[DataPointTask] = []
        with task_context(self.model_spec):
            for reg in hooks:
                hook_tasks = reg.hook()
                if isinstance(hook_tasks, list):
                    tasks.extend(hook_tasks)
        return tasks

    @staticmethod
    def _matching_update_hooks(hooks: dict, entity_type: str, hook_id: Optional[str]):
        if hook_id is not None:
            return list(hooks[entity_type, hook_id])
        return [reg for (etype, _), regs in hooks.items() if etype == entity_type for reg in regs]


def _callable_matches(func: Callable, expected: Union[str, Callable]) -> bool:
    if callable(expected):
        return func == expected
    func_name = get_func_name(func)
    return func_name == expected or func_name.endswith(f".{expected}")
