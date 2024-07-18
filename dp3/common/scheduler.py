"""
Allows modules to register functions (callables) to be run at
specified times or intervals (like cron does).

Based on APScheduler package
"""

import logging
from typing import Callable, Union

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

from dp3.common.utils import get_func_name


class Scheduler:
    """
    Allows modules to register functions (callables) to be run
    at specified times or intervals (like cron does).
    """

    def __init__(self) -> None:
        self.log = logging.getLogger("Scheduler")
        # self.log.setLevel("DEBUG")
        logging.getLogger("apscheduler.scheduler").setLevel("WARNING")
        logging.getLogger("apscheduler.executors.default").setLevel("WARNING")
        self.sched = BackgroundScheduler(timezone="UTC")
        self.last_job_id = 0

    def start(self) -> None:
        self.log.debug("Scheduler start")
        self.sched.start()

    def stop(self) -> None:
        self.log.debug("Scheduler stop")
        self.sched.shutdown()

    def register(
        self,
        func: Callable,
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
        self.last_job_id += 1
        trigger = CronTrigger(
            year, month, day, week, day_of_week, hour, minute, second, timezone=timezone
        )
        self.sched.add_job(
            func,
            trigger,
            func_args,
            func_kwargs,
            coalesce=True,
            max_instances=1,
            misfire_grace_time=misfire_grace_time,
            id=str(self.last_job_id),
        )
        self.log.debug(f"Registered function {get_func_name(func)} to be called at {trigger}")
        return self.last_job_id

    def pause_job(self, id):
        """Pause job with given ID"""
        self.sched.pause_job(str(id))

    def resume_job(self, id):
        """Resume previously paused job with given ID"""
        self.sched.resume_job(str(id))
