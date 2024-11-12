import logging
from typing import Callable

from dp3.common.attrspec import AttrType
from dp3.common.config import ModelSpec
from dp3.common.datapoint import DataPointBase
from dp3.common.datatype import AnyEidT
from dp3.common.task import DataPointTask, task_context
from dp3.common.types import EventGroupType
from dp3.common.utils import get_func_name


class TaskGenericHooksContainer:
    """Container for generic hooks

    Possible hooks:

    - `on_task_start`: receives Task, no return value requirements
    """

    def __init__(self, log: logging.Logger, elog: EventGroupType):
        self.log = log.getChild("genericHooks")
        self.elog = elog

        self._on_start = []

    def register(self, hook_type: str, hook: Callable):
        if hook_type == "on_task_start":
            self._on_start.append(hook)
        else:
            raise ValueError(f"Hook type '{hook_type}' doesn't exist.")

        self.log.debug(f"Added '{hook_type}' hook: {get_func_name(hook)}")

    def run_on_start(self, task: DataPointTask):
        for hook in self._on_start:
            # Run hook
            try:
                hook(task)
            except Exception as e:
                self.elog.log("module_error")
                self.log.error(f"Error during running hook {hook}: {e}")


class TaskEntityHooksContainer:
    """Container for entity hooks

    Possible hooks:

    - `allow_entity_creation`: receives eid and Task, may prevent entity record creation (by
          returning False)
    - `on_entity_creation`: receives eid and Task, may return list of DataPointTasks
    """

    def __init__(
        self, entity: str, model_spec: ModelSpec, log: logging.Logger, elog: EventGroupType
    ):
        self.entity = entity
        self.log = log.getChild(f"entityHooks.{entity}")
        self.elog = elog
        self.model_spec = model_spec

        self._allow_creation = []
        self._on_creation = []

    def register(self, hook_type: str, hook: Callable):
        if hook_type == "allow_entity_creation":
            self._allow_creation.append(hook)
        elif hook_type == "on_entity_creation":
            self._on_creation.append(hook)
        else:
            raise ValueError(f"Hook type '{hook_type}' doesn't exist.")

        self.log.debug(f"Added '{hook_type}' hook: {get_func_name(hook)}")

    def run_allow_creation(self, eid: AnyEidT, task: DataPointTask):
        for hook in self._allow_creation:
            try:
                if not hook(eid, task):
                    self.log.debug(
                        f"Creation of eid '{eid}' prevented because hook '{hook}' returned False."
                    )
                    return False
            except Exception as e:
                self.elog.log("module_error")
                self.log.error(f"Error during running hook {get_func_name(hook)}: {e}")

        return True

    def run_on_creation(self, eid: AnyEidT, task: DataPointTask):
        new_tasks = []

        with task_context(self.model_spec):
            for hook in self._on_creation:
                try:
                    # Run hook
                    hook_new_tasks = hook(eid, task)

                    # Append new tasks to process
                    if isinstance(hook_new_tasks, list):
                        new_tasks += hook_new_tasks
                except Exception as e:
                    self.elog.log("module_error")
                    self.log.error(f"Error during running hook {hook}: {e}")

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
    ):
        self.entity = entity
        self.attr = attr
        self.log = log.getChild(f"attributeHooks.{entity}.{attr}")
        self.elog = elog
        self.model_spec = model_spec

        if attr_type == AttrType.PLAIN:
            self.on_new_hook_type = "on_new_plain"
        elif attr_type == AttrType.OBSERVATIONS:
            self.on_new_hook_type = "on_new_observation"
        elif attr_type == AttrType.TIMESERIES:
            self.on_new_hook_type = "on_new_ts_chunk"
        else:
            raise ValueError(f"Invalid attribute type '{attr_type}'")

        self._on_new = []

    def register(self, hook_type: str, hook: Callable):
        if hook_type == self.on_new_hook_type:
            self._on_new.append(hook)
        else:
            raise ValueError(
                f"Hook type '{hook_type}' doesn't exist for {self.entity}/{self.attr}."
            )

        self.log.debug(f"Added '{hook_type}' hook: {get_func_name(hook)}")

    def run_on_new(self, eid: AnyEidT, dp: DataPointBase):
        new_tasks = []

        with task_context(self.model_spec):
            for hook in self._on_new:
                try:
                    # Run hook
                    hook_new_tasks = hook(eid, dp)

                    # Append new tasks to process
                    if isinstance(hook_new_tasks, list):
                        new_tasks += hook_new_tasks
                except Exception as e:
                    self.elog.log("module_error")
                    self.log.error(f"Error during running hook {hook}: {e}")

        return new_tasks
