import logging
from collections.abc import Callable
from typing import Any

from event_count_logger import DummyEventGroup

from dp3.common.attrspec import AttrType
from dp3.common.config import ModelSpec
from dp3.common.datapoint import DataPointBase
from dp3.common.datatype import AnyEidT
from dp3.common.hook_telemetry import HookTelemetry, TrackedHook
from dp3.common.hook_types import ATTR_TYPE_TO_ON_NEW_HOOK
from dp3.common.task import DataPointTask, task_context
from dp3.common.types import EventGroupType
from dp3.common.utils import get_func_name

TaskStartHook = TrackedHook[[DataPointTask], Any]
AllowEntityCreationHook = TrackedHook[[AnyEidT, DataPointTask], bool]
OnEntityCreationHook = TrackedHook[[AnyEidT, DataPointTask], list[DataPointTask]]
OnNewAttributeHook = TrackedHook[
    [AnyEidT, DataPointBase],
    list[DataPointTask] | None,
]


class TaskGenericHooksContainer:
    """Container for generic hooks

    Possible hooks:

    - `on_task_start`: receives Task, no return value requirements
    """

    def __init__(
        self,
        log: logging.Logger,
        elog: EventGroupType,
        hook_elog: EventGroupType | None = None,
    ):
        self.log = log.getChild("genericHooks")
        self.elog = elog
        self.telemetry = HookTelemetry(hook_elog if hook_elog is not None else DummyEventGroup())

        self._on_start: list[TaskStartHook] = []

    def register(self, hook_type: str, hook: Callable):
        if hook_type == "on_task_start":
            if any(registered.callback == hook for registered in self._on_start):
                raise ValueError(f"Hook '{get_func_name(hook)}' is already registered.")
            self._on_start.append(self.telemetry.wrap(hook_type, hook))
        else:
            raise ValueError(f"Hook type '{hook_type}' doesn't exist.")

        self.log.debug(f"Added '{hook_type}' hook: {get_func_name(hook)}")

    def run_on_start(self, task: DataPointTask):
        for hook in self._on_start:
            try:
                hook(task)
            except Exception as e:
                self.elog.log("module_error")
                self.log.error(f"Error during running hook {hook.callback}: {e}")


class TaskEntityHooksContainer:
    """Container for entity hooks

    Possible hooks:

    - `allow_entity_creation`: receives eid and Task, may prevent entity record creation (by
          returning False)
    - `on_entity_creation`: receives eid and Task, may return list of DataPointTasks
    """

    def __init__(
        self,
        entity: str,
        model_spec: ModelSpec,
        log: logging.Logger,
        elog: EventGroupType,
        hook_elog: EventGroupType | None = None,
    ):
        self.entity = entity
        self.log = log.getChild(f"entityHooks.{entity}")
        self.elog = elog
        self.telemetry = HookTelemetry(hook_elog if hook_elog is not None else DummyEventGroup())
        self.model_spec = model_spec

        self._allow_creation: list[AllowEntityCreationHook] = []
        self._on_creation: list[OnEntityCreationHook] = []

    def register(self, hook_type: str, hook: Callable):
        if hook_type == "allow_entity_creation":
            hooks = self._allow_creation
        elif hook_type == "on_entity_creation":
            hooks = self._on_creation
        else:
            raise ValueError(f"Hook type '{hook_type}' doesn't exist.")

        if any(registered.callback == hook for registered in hooks):
            raise ValueError(
                f"Hook '{get_func_name(hook)}' is already registered for entity '{self.entity}'."
            )
        hooks.append(self.telemetry.wrap(hook_type, hook, self.entity))

        self.log.debug(f"Added '{hook_type}' hook: {get_func_name(hook)}")

    def run_allow_creation(self, eid: AnyEidT, task: DataPointTask):
        for hook in self._allow_creation:
            try:
                if hook(eid, task):
                    hook.log("decisions_allowed")
                else:
                    hook.log("decisions_denied")
                    self.log.debug(
                        "Creation of eid '%s' prevented because hook '%s' returned False.",
                        eid,
                        get_func_name(hook.callback),
                    )
                    return False
            except Exception as e:
                self.elog.log("module_error")
                self.log.error(f"Error during running hook {get_func_name(hook.callback)}: {e}")

        return True

    def run_on_creation(self, eid: AnyEidT, task: DataPointTask):
        new_tasks = []

        with task_context(self.model_spec):
            for hook in self._on_creation:
                try:
                    hook_new_tasks = hook(eid, task)
                    if isinstance(hook_new_tasks, list):
                        hook.log("created_tasks", len(hook_new_tasks))
                        new_tasks += hook_new_tasks
                except Exception as e:
                    self.elog.log("module_error")
                    self.log.error(f"Error during running hook {hook.callback}: {e}")

        return new_tasks


class TaskAttrHooksContainer:
    """Container for attribute hooks

    Possible hooks:

    - `on_new_plain`, `on_new_observation`, `on_new_ts_chunk`:
        receives eid and DataPointBase, may return a list of DataPointTasks
    """

    def __init__(
        self,
        entity: str,
        attr: str,
        attr_type: AttrType,
        model_spec: ModelSpec,
        log: logging.Logger,
        elog: EventGroupType,
        hook_elog: EventGroupType | None = None,
    ):
        self.entity = entity
        self.attr = attr
        self.log = log.getChild(f"attributeHooks.{entity}.{attr}")
        self.elog = elog
        self.telemetry = HookTelemetry(hook_elog if hook_elog is not None else DummyEventGroup())
        self.model_spec = model_spec

        try:
            self.on_new_hook_type = ATTR_TYPE_TO_ON_NEW_HOOK[attr_type]
        except KeyError as e:
            raise ValueError(f"Invalid attribute type '{attr_type}'") from e

        self._on_new: list[OnNewAttributeHook] = []

    def register(self, hook_type: str, hook: Callable):
        if hook_type != self.on_new_hook_type:
            raise ValueError(
                f"Hook type '{hook_type}' doesn't exist for {self.entity}/{self.attr}."
            )
        if any(registered.callback == hook for registered in self._on_new):
            raise ValueError(
                f"Hook '{get_func_name(hook)}' is already registered for "
                f"attribute '{self.entity}/{self.attr}'."
            )
        self._on_new.append(self.telemetry.wrap(hook_type, hook, self.entity, self.attr))

        self.log.debug(f"Added '{hook_type}' hook: {get_func_name(hook)}")

    def run_on_new(self, eid: AnyEidT, dp: DataPointBase):
        new_tasks = []

        with task_context(self.model_spec):
            for hook in self._on_new:
                try:
                    hook_new_tasks = hook(eid, dp)
                    if isinstance(hook_new_tasks, list):
                        hook.log("created_tasks", len(hook_new_tasks))
                        new_tasks += hook_new_tasks
                except Exception as e:
                    self.elog.log("module_error")
                    self.log.error(f"Error during running hook {hook.callback}: {e}")

        return new_tasks
