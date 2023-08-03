from typing import Callable, Union

from dp3.common.scheduler import Scheduler
from dp3.common.task import DataPointTask
from dp3.snapshots.snapshooter import SnapShooter
from dp3.task_processing.task_executor import TaskExecutor


class CallbackRegistrar:
    """Interface for callback registration."""

    def __init__(
        self, scheduler: Scheduler, task_executor: TaskExecutor, snap_shooter: SnapShooter
    ):
        self._scheduler = scheduler
        self._task_executor = task_executor
        self._snap_shooter = snap_shooter

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
    ) -> int:
        """
        Register a function to be run at specified times.

        Pass cron-like specification of when the function should be called,
        see [docs](https://apscheduler.readthedocs.io/en/latest/modules/triggers/cron.html)
        of apscheduler.triggers.cron for details.
        `
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
        )

    def register_task_hook(self, hook_type: str, hook: Callable):
        """Registers one of available task hooks

        See: [`TaskGenericHooksContainer`][dp3.task_processing.task_hooks.TaskGenericHooksContainer]
        in `task_hooks.py`
        """
        self._task_executor.register_task_hook(hook_type, hook)

    def register_entity_hook(self, hook_type: str, hook: Callable, entity: str):
        """Registers one of available task entity hooks

        See: [`TaskEntityHooksContainer`][dp3.task_processing.task_hooks.TaskEntityHooksContainer]
        in `task_hooks.py`
        """
        self._task_executor.register_entity_hook(hook_type, hook, entity)

    def register_attr_hook(self, hook_type: str, hook: Callable, entity: str, attr: str):
        """Registers one of available task attribute hooks

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
        hook: Callable[[str, dict], None],
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
                and its current values, including linked entities, as dict
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
