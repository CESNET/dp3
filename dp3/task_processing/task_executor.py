import inspect
import logging
from collections import deque
from copy import deepcopy
from datetime import datetime
from typing import Any, Callable, Union
from collections.abc import Iterable

from event_count_logger import EventCountLogger, DummyEventGroup

from dp3.common.attrspec import AttrSpec
from dp3.common.entityspec import EntitySpec
from dp3.common.task import Task
from dp3.database.database import DatabaseError, EntityDatabase
from .. import g
from ..common.utils import get_func_name, parse_rfc_time


class TaskExecutor:
    """
    TaskExecutor manages updates of entity records, which are being read from task queue (via parent TaskDistributor)

    A hooked function receives a list of updates that triggered its call,
    i.e. a list of 2-tuples (attr_name, new_value) or (event_name, param)
    (if more than one update triggers the same function, it's called only once).

    If there are multiple matching events, only the first one is used.

    :param db: Instance of EntityDatabase
    :param attr_spec: Configuration of entity types and attributes (dict entity_name->entity_spec)
    """

    def __init__(
        self,
        db: EntityDatabase,
        attr_spec: dict[str, dict[str, Union[EntitySpec, dict[str, AttrSpec]]]],
    ) -> None:
        # initialize task distribution

        self.log = logging.getLogger("TaskExecutor")

        # Get list of configured entity types
        self.entity_types = list(attr_spec.keys())
        self.log.debug(f"Configured entity types: {self.entity_types}")

        # Mapping of names of attributes to a list of functions that should be
        # called when the attribute is updated
        # (One such mapping for each entity type)
        self._attr2func = {etype: {} for etype in self.entity_types}
        # Set of attributes that may be updated by a function
        self._func2attr = {etype: {} for etype in self.entity_types}
        # Mapping of functions to set of attributes the function watches, i.e.
        # is called when the attribute is changed
        self._func_triggers = {etype: {} for etype in self.entity_types}
        # cache for may_change set calculation - is cleared when register_handler() is called
        self._may_change_cache = self._init_may_change_cache()

        self.attr_spec = attr_spec
        self.db = db

        # EventCountLogger - count number of events across multiple processes using shared counters in Redis
        ecl = EventCountLogger(
            g.config.get("event_logging.groups"), g.config.get("event_logging.redis")
        )
        self.elog = ecl.get_group("te") or DummyEventGroup()
        # Print warning if some event group is not configured
        not_configured_groups = []
        if isinstance(self.elog, DummyEventGroup):
            not_configured_groups.append("te")
        if not_configured_groups:
            self.log.warning(
                f"EventCountLogger: No configuration for event group(s) '{','.join(not_configured_groups)}' found, such events will not be logged (check event_logging.yml)"
            )

    def _init_may_change_cache(self) -> dict[str, dict[Any, Any]]:
        """
        Initializes _may_change_cache with all supported Entity types
        :return: None
        """
        may_change_cache = {}
        for etype in self.entity_types:
            may_change_cache[etype] = {}
        return may_change_cache

    def register_handler(
        self, func: Callable, etype: str, triggers: Iterable[str], changes: Iterable[str]
    ) -> None:
        """
        Hook a function (or bound method) to specified attribute changes/events. Each function must be registered only
        once! Type check is already done in TaskDistributor.
        :param func: function or bound method (callback)
        :param etype: entity type (only changes of attributes of this etype trigger the func)
        :param triggers: set/list/tuple of attributes whose update trigger the call of the method (update of any one of the attributes will do)
        :param changes: set/list/tuple of attributes the method call may update (may be None)
        :return: None
        """
        # need to clear calculations in set, because may be wrong now
        self._may_change_cache = self._init_may_change_cache()
        # _func2attr[etype]: function -> list of attrs it may change
        # _func_triggers[etype]: function -> list of attrs that trigger it
        # _attr2func[etype]: attribute -> list of functions its change triggers
        # There are separate mappings for each entity type.
        self._func2attr[etype][func] = tuple(changes) if changes is not None else ()
        self._func_triggers[etype][func] = set(triggers)
        for attr in triggers:
            if attr in self._attr2func[etype]:
                self._attr2func[etype][attr].append(func)
            else:
                self._attr2func[etype][attr] = [func]

    def process_task(self, task: Task):
        """
        Main processing function - update attributes or trigger an event.

        :param: task
        :return: True if a new record was created, False otherwise.
        """
        self.log.debug(f"Received new task {task.etype}/{task.ekey}, starting processing!")

        # Check existence of etype
        if task.etype not in self.attr_spec:
            self.log.error(f"Task {task.etype}/{task.ekey}: Unknown entity type!")
            self.elog.log("task_processing_error")
            return False

        # *** Now we have the record, process the requested updates ***
        created = False
        try:
            self.db.insert_datapoints(task.etype, task.ekey, task.data_points)
            self.log.debug(
                f"Task {task.etype}/{task.ekey}: All changes written to DB, processing finished."
            )
            created = True
        except DatabaseError as e:
            self.log.error(f"Task {task.etype}/{task.ekey}: DB error: {e}")

        # Log the processed task
        self.elog.log("task_processed")

        return created
