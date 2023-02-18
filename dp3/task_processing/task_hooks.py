import logging
from typing import Callable

from dp3.common.attrspec import AttrType
from dp3.common.datapoint import DataPointBase
from dp3.common.task import Task


class TaskGenericHooksContainer:
    """Container for generic hooks

    Possible hooks:

    - `on_task_start`: receives Task, no return value requirements
    """

    def __init__(self, log: logging.Logger):
        self.log = log.getChild("genericHooks")

        self._on_start = []

    def register(self, hook_type: str, hook: Callable):
        if hook_type == "on_task_start":
            self._on_start.append(hook)
        else:
            raise ValueError(f"Hook type '{hook_type}' doesn't exist.")

        self.log.debug(f"Added '{hook_type}' hook")

    def run_on_start(self, task: Task):
        for hook in self._on_start:
            # Run hook
            try:
                hook(task)
            except Exception as e:
                self.log.error(f"Error during running hook {hook}: {e}")


class TaskEntityHooksContainer:
    """Container for entity hooks

    Possible hooks:

    - `allow_entity_creation`: receives ekey and Task, may prevent entity record creation (by
          returning False)
    - `on_entity_creation`: receives ekey and Task, may return new Tasks (including new
          DataPoints)
    """

    def __init__(self, entity: str, log: logging.Logger):
        self.entity = entity
        self.log = log.getChild(f"entityHooks.{entity}")

        self._allow_creation = []
        self._on_creation = []

    def register(self, hook_type: str, hook: Callable):
        if hook_type == "allow_entity_creation":
            self._allow_creation.append(hook)
        elif hook_type == "on_entity_creation":
            self._on_creation.append(hook)
        else:
            raise ValueError(f"Hook type '{hook_type}' doesn't exist.")

        self.log.debug(f"Added '{hook_type}' hook")

    def run_allow_creation(self, ekey: str, task: Task):
        for hook in self._allow_creation:
            try:
                if not hook(ekey, task):
                    self.log.debug(
                        f"Creation of ekey '{ekey}' prevented because hook '{hook}' returned False."
                    )
                    return False
            except Exception as e:
                self.log.error(f"Error during running hook {hook}: {e}")

        return True

    def run_on_creation(self, ekey: str, task: Task):
        new_tasks = []

        for hook in self._on_creation:
            try:
                # Run hook
                hook_new_tasks = hook(ekey, task)

                # Append new tasks to process
                if type(hook_new_tasks) is list:
                    new_tasks += hook_new_tasks
            except Exception as e:
                self.log.error(f"Error during running hook {hook}: {e}")

        return new_tasks


class TaskAttrHooksContainer:
    """Container for attribute hooks

    Possible hooks:

    - `on_new_plain`, `on_new_observation`, `on_new_ts_chunk`:
        receives ekey and Task, may return new Tasks (including new DataPoints)
    """

    def __init__(self, entity: str, attr: str, attr_type: AttrType, log: logging.Logger):
        self.entity = entity
        self.attr = attr
        self.log = log.getChild(f"attributeHooks.{entity}.{attr}")

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

        self.log.debug(f"Added '{hook_type}' hook")

    def run_on_new(self, ekey: str, dp: DataPointBase):
        new_tasks = []

        for hook in self._on_new:
            try:
                # Run hook
                hook_new_tasks = hook(ekey, dp)

                # Append new tasks to process
                if type(hook_new_tasks) is list:
                    new_tasks += hook_new_tasks
            except Exception as e:
                self.log.error(f"Error during running hook {hook}: {e}")

        return new_tasks
