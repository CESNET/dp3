import logging
from functools import partial
from logging import Logger
from typing import Callable, Union

from pydantic import BaseModel

from dp3.common.attrspec import AttrType
from dp3.common.config import ModelSpec, PlatformConfig, read_config_dir
from dp3.common.datapoint import DataPointType
from dp3.common.datatype import AnyEidT
from dp3.common.scheduler import Scheduler
from dp3.common.state import SharedFlag
from dp3.common.task import DataPointTask
from dp3.common.types import ParsedTimedelta
from dp3.core.updater import Updater
from dp3.snapshots.snapshooter import SnapShooter
from dp3.task_processing.task_executor import TaskExecutor


def write_datapoints_into_record(model_spec: ModelSpec, tasks: list[DataPointTask], record: dict):
    eid = record["eid"]
    for task in tasks:
        if task.eid != eid:
            continue
        for dp in task.data_points:
            attr_spec = model_spec.attributes[dp.etype, dp.attr]
            if attr_spec.t == AttrType.PLAIN:
                if attr_spec.t in AttrType.PLAIN | AttrType.OBSERVATIONS and attr_spec.is_iterable:
                    v = [
                        elem.model_dump() if isinstance(elem, BaseModel) else elem for elem in dp.v
                    ]
                else:
                    v = dp.v.model_dump() if isinstance(dp.v, BaseModel) else dp.v

                record[dp.attr] = v


def on_entity_creation_in_snapshots(
    model_spec: ModelSpec,
    run_flag: SharedFlag,
    original_hook: Callable[[AnyEidT, DataPointTask], list[DataPointTask]],
    etype: str,
    record: dict,
) -> list[DataPointTask]:
    """Wrapper for on_entity_creation hooks to enable running as a snapshot callback."""
    if not run_flag.isset():
        return []
    eid = record["eid"]
    mock_task = DataPointTask(etype=etype, eid=eid, data_points=[])
    tasks = original_hook(eid, mock_task)
    write_datapoints_into_record(model_spec, tasks, record)
    return tasks


def on_attr_change_in_snapshots(
    model_spec: ModelSpec,
    run_flag: SharedFlag,
    original_hook: Callable[[AnyEidT, DataPointTask], Union[list[DataPointTask], None]],
    etype: str,
    record: dict,
) -> list[DataPointTask]:
    """Wrapper for on_entity_creation hooks to enable running as a snapshot callback."""
    if not run_flag.isset():
        return []
    eid = record["eid"]
    mock_task = DataPointTask(etype=etype, eid=eid, data_points=[])
    tasks = original_hook(eid, mock_task)
    if isinstance(tasks, list):
        write_datapoints_into_record(model_spec, tasks, record)
    return tasks


def unset_flag(flag: SharedFlag) -> list:
    flag.unset()
    return []


def reload_module_config(
    log: Logger, platform_config: PlatformConfig, modules: dict, module: str
) -> None:
    """
    Reloads configuration of a module.

    Args:
        log: log to write messages to
        platform_config: Platform configuration
        modules: Dictionary of loaded modules by their names
        module: Name of the module to reload

    Returns:
        Module's configuration
    """
    log.debug(f"Reloading config of module '{module}'")

    if module not in modules:
        log.warning(f"Could not find module '{module}', cannot reload config. Is it loaded?")
        return

    config = read_config_dir(platform_config.config_base_path, recursive=True)
    module_config = config.get(f"modules.{module}", {})
    modules[module].load_config(platform_config, module_config)
    modules[module].refresh.set()
    log.info(f"Config of module '{module}' reloaded successfully.")


class CallbackRegistrar:
    """Interface for callback registration."""

    def __init__(
        self,
        scheduler: Scheduler,
        task_executor: TaskExecutor,
        snap_shooter: SnapShooter,
        updater: Updater,
    ):
        self._scheduler = scheduler
        self._task_executor = task_executor
        self._snap_shooter = snap_shooter
        self._updater = updater

        self.log = logging.getLogger(self.__class__.__name__)
        self.model_spec = task_executor.model_spec
        self.attr_spec_t_to_on_attr = {
            AttrType.PLAIN: "on_new_plain",
            AttrType.OBSERVATIONS: "on_new_observation",
            AttrType.TIMESERIES: "on_new_ts_chunk",
        }

    def scheduler_register(
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
        """
        Register a function to be run at specified times.

        Pass cron-like specification of when the function should be called,
        see [docs](https://apscheduler.readthedocs.io/en/latest/modules/triggers/cron.html)
        of apscheduler.triggers.cron for details.

        Args:
            func: function or method to be called
            func_args: list of positional arguments to call func with
            func_kwargs: dict of keyword arguments to call func with
            year: 4-digit year
            month: month (1-12)
            day: day of month (1-31)
            week: ISO week (1-53)
            day_of_week: number or name of weekday (0-6 or mon,tue,wed,thu,fri,sat,sun)
            hour: hour (0-23)
            minute: minute (0-59)
            second: second (0-59)
            timezone: Timezone for time specification (default is UTC).
            misfire_grace_time: seconds after the designated run time
                that the job is still allowed to be run (default is 1)
        Returns:
             job ID
        """
        return self._scheduler.register(
            func,
            func_args=func_args,
            func_kwargs=func_kwargs,
            year=year,
            month=month,
            day=day,
            week=week,
            day_of_week=day_of_week,
            hour=hour,
            minute=minute,
            second=second,
            timezone=timezone,
            misfire_grace_time=misfire_grace_time,
        )

    def register_task_hook(self, hook_type: str, hook: Callable):
        """Registers one of available task hooks

        See: [`TaskGenericHooksContainer`][dp3.task_processing.task_hooks.TaskGenericHooksContainer]
        in `task_hooks.py`
        """
        self._task_executor.register_task_hook(hook_type, hook)

    def register_allow_entity_creation_hook(
        self,
        hook: Callable[[AnyEidT, DataPointTask], bool],
        entity: str,
    ):
        """
        Registers passed hook to allow entity creation.

        Binds hook to specified entity (though same hook can be bound multiple times).

        Args:
            hook: `hook` callable should expect eid and Task as arguments and return a bool.
            entity: specifies entity type
        """
        self._task_executor.register_entity_hook("allow_entity_creation", hook, entity)

    def register_on_entity_creation_hook(
        self,
        hook: Callable[[AnyEidT, DataPointTask], list[DataPointTask]],
        entity: str,
        refresh: SharedFlag = None,
        may_change: list[list[str]] = None,
    ):
        """
        Registers passed hook to be called on entity creation.

        Binds hook to specified entity (though same hook can be bound multiple times).

        Allows registration of refreshing on configuration changes, if `refresh` is specified.
        In that case, `may_change` must be specified.

        Args:
            hook: `hook` callable should expect eid and Task as arguments and
                return a list of DataPointTask objects to perform.
            entity: specifies entity type
            refresh: If specified, registered hook will be called on configuration changes.
                Pass `self.refresh` from `BaseModule` subclasses.
            may_change: each item should specify an attribute that `hook` may change,
                for specification format see `register_correlation_hook`
        """
        self._task_executor.register_entity_hook("on_entity_creation", hook, entity)
        if refresh is not None:
            if may_change is None:
                raise ValueError("'may_change' must be specified if 'refresh' is specified")
            self._snap_shooter.register_correlation_hook(
                partial(on_entity_creation_in_snapshots, self.model_spec, refresh, hook),
                entity,
                [],
                may_change,
            )
            self._snap_shooter.register_run_finalize_hook(partial(unset_flag, refresh))

    def register_entity_hook(self, hook_type: str, hook: Callable, entity: str):
        """Registers one of available task entity hooks

        !!! warning "Deprecated"

            This method is deprecated, use `register_on_entity_creation_hook`
            or `register_allow_entity_creation_hook` instead.

        See: [`TaskEntityHooksContainer`][dp3.task_processing.task_hooks.TaskEntityHooksContainer]
        in `task_hooks.py`
        """
        self._task_executor.register_entity_hook(hook_type, hook, entity)

    def register_on_new_attr_hook(
        self,
        hook: Callable[[AnyEidT, DataPointType], Union[None, list[DataPointTask]]],
        entity: str,
        attr: str,
        refresh: SharedFlag = None,
        may_change: list[list[str]] = None,
    ):
        """
        Registers passed hook to be called on new attribute datapoint.

        Args:
            hook: `hook` callable should expect eid and a datapoint as arguments.
                Can optionally return a list of DataPointTasks to perform.
            entity: specifies entity type
            attr: specifies attribute name
            refresh: If specified, registered hook will be called on configuration changes.
                Pass `self.refresh` from `BaseModule` subclasses.
            may_change: each item should specify an attribute that `hook` may change,
                for specification format see `register_correlation_hook`

        Raises:
            ValueError: If entity and attr do not specify a valid attribute, a ValueError is raised.
        """
        try:
            hook_type = self.attr_spec_t_to_on_attr[self.model_spec.attributes[entity, attr].t]
        except KeyError as e:
            raise ValueError(
                f"Cannot register hook for attribute {entity}/{attr}, are you sure it exists?"
            ) from e
        self._task_executor.register_attr_hook(hook_type, hook, entity, attr)

        if refresh is None:
            return
        if may_change is None:
            raise ValueError("'may_change' must be specified if 'refresh' is specified")

        self._snap_shooter.register_correlation_hook(
            partial(on_attr_change_in_snapshots, self.model_spec, refresh, hook),
            entity,
            [[attr]],
            may_change,
        )
        self._snap_shooter.register_run_finalize_hook(partial(unset_flag, refresh))

    def register_attr_hook(self, hook_type: str, hook: Callable, entity: str, attr: str):
        """
        Registers one of available task attribute hooks

        !!! warning "Deprecated"

            This method is deprecated, use `register_on_new_attr_hook` instead.

        See: [`TaskAttrHooksContainer`][dp3.task_processing.task_hooks.TaskAttrHooksContainer]
        in `task_hooks.py`
        """
        self._task_executor.register_attr_hook(hook_type, hook, entity, attr)

    def register_timeseries_hook(
        self,
        hook: Callable[[str, str, list[dict]], list[DataPointTask]],
        entity_type: str,
        attr_type: str,
    ):
        """
        Registers passed timeseries hook to be called during snapshot creation.

        Binds hook to specified `entity_type` and `attr_type` (though same hook can be bound
        multiple times).

        Args:
            hook: `hook` callable should expect entity_type, attr_type and attribute
                history as arguments and return a list of `DataPointTask` objects.
            entity_type: specifies entity type
            attr_type: specifies attribute type

        Raises:
            ValueError: If entity_type and attr_type do not specify a valid timeseries attribute,
                a ValueError is raised.
        """
        self._snap_shooter.register_timeseries_hook(hook, entity_type, attr_type)

    def register_correlation_hook(
        self,
        hook: Callable[[str, dict], Union[None, list[DataPointTask]]],
        entity_type: str,
        depends_on: list[list[str]],
        may_change: list[list[str]],
    ):
        """
        Registers passed hook to be called during snapshot creation.

        Binds hook to specified entity_type (though same hook can be bound multiple times).

        `entity_type` and attribute specifications are validated, `ValueError` is raised on failure.

        Args:
            hook: `hook` callable should expect entity type as str
                and its current values, including linked entities, as dict.
                Can optionally return a list of DataPointTask objects to perform.
            entity_type: specifies entity type
            depends_on: each item should specify an attribute that is depended on
                in the form of a path from the specified entity_type to individual attributes
                (even on linked entities).
            may_change: each item should specify an attribute that `hook` may change.
                specification format is identical to `depends_on`.

        Raises:
            ValueError: On failure of specification validation.
        """
        self._snap_shooter.register_correlation_hook(hook, entity_type, depends_on, may_change)

    def register_snapshot_init_hook(self, hook: Callable[[], list[DataPointTask]]):
        """
        Registers passed hook to be called before a run of snapshot creation begins.

        Args:
            hook: `hook` callable should expect no arguments and
                return a list of DataPointTask objects to perform.
        """
        self._snap_shooter.register_run_init_hook(hook)

    def register_snapshot_finalize_hook(self, hook: Callable[[], list[DataPointTask]]):
        """
        Registers passed hook to be called after a run of snapshot creation ends.

        Args:
            hook: `hook` callable should expect no arguments and
                return a list of DataPointTask objects to perform.
        """
        self._snap_shooter.register_run_finalize_hook(hook)

    def register_periodic_update_hook(
        self,
        hook: Callable[[str, AnyEidT, dict], list[DataPointTask]],
        hook_id: str,
        entity_type: str,
        period: ParsedTimedelta,
    ):
        """
        Registers a callback for periodic update of entities of the specified type.

        The callback receives the entity type, the entity ID and the master record.

        Args:
            hook: `hook` callable should expect entity type, entity ID and master record
                as arguments and return a list of DataPointTask objects to perform.
            hook_id: specifies hook ID
            entity_type: specifies entity type
            period: specifies period of the callback
        """
        self._updater.register_record_update_hook(hook, hook_id, entity_type, period)

    def register_periodic_eid_update_hook(
        self,
        hook: Callable[[str, AnyEidT], list[DataPointTask]],
        hook_id: str,
        entity_type: str,
        period: ParsedTimedelta,
    ):
        """
        Registers a callback for periodic update of entities of the specified type.

        The callback receives the entity type and the entity ID.

        Args:
            hook: `hook` callable should expect entity type and entity ID as arguments
                and return a list of DataPointTask objects to perform.
            hook_id: specifies hook ID
            entity_type: specifies entity type
            period: specifies period of the callback
        """
        self._updater.register_eid_update_hook(hook, hook_id, entity_type, period)
