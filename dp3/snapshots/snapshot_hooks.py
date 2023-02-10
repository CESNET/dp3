import logging
from collections import defaultdict
from typing import Callable

from dp3.common.attrspec import AttrType
from dp3.common.config import ModelSpec


class SnapshotTimeseriesHookContainer:
    """Container for timeseries analysis hooks"""

    def __init__(self, log: logging.Logger, model_spec: ModelSpec):
        self.log = log.getChild("timeseriesHooks")
        self.model_spec = model_spec

        self._hooks = defaultdict(list)

    def register(self, entity_type: str, attr_type: str, hook: Callable):
        if (entity_type, attr_type) not in self.model_spec.attributes:
            raise ValueError(f"Attribute '{attr_type}' of entity '{entity_type}' does not exist.")
        spec = self.model_spec.attributes[entity_type, attr_type]
        if spec.t != AttrType.TIMESERIES:
            raise ValueError(f"'{entity_type}.{attr_type}' is not a timeseries, but '{spec.t}'")
        self._hooks[entity_type, attr_type].append(hook)
        self.log.debug(f"Added hook:  '{hook.__qualname__}'")

    def run(self, entity_id: str, attr_id: str, attr_history: dict):
        tasks = []
        for hook in self._hooks[entity_id, attr_id]:
            try:
                new_tasks = hook(entity_id, attr_id, attr_history)
                tasks.extend(new_tasks)
            except Exception as e:
                self.log.error(f"Error during running hook {hook}: {e}")
        return tasks
