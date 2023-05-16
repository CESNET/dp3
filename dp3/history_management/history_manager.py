import gzip
import json
import logging
from datetime import datetime, timedelta
from json import JSONEncoder
from pathlib import Path
from typing import Any, Optional

from dp3.common.attrspec import AttrType
from dp3.common.callback_registrar import CallbackRegistrar
from dp3.common.config import PlatformConfig
from dp3.database.database import DatabaseError, EntityDatabase


class DatetimeEncoder(JSONEncoder):
    """JSONEncoder to encode datetime using the standard ADiCT format string."""

    def default(self, o: Any) -> Any:
        if isinstance(o, datetime):
            return o.strftime("%Y-%m-%dT%H:%M:%S.%fZ")[:-4]
        return super().default(o)


class HistoryManager:
    def __init__(
        self, db: EntityDatabase, platform_config: PlatformConfig, registrar: CallbackRegistrar
    ) -> None:
        self.log = logging.getLogger("HistoryManager")

        self.db = db
        self.model_spec = platform_config.model_spec
        self.worker_index = platform_config.process_index
        self.num_workers = platform_config.num_processes
        self.config = platform_config.config.get("history_manager")

        if platform_config.process_index != 0:
            self.log.debug(
                "History management will be disabled in this worker to avoid race conditions."
            )
            return

        # Schedule datapoints cleaning
        datapoint_cleaning_period = self.config["datapoint_cleaning"]["tick_rate"]
        registrar.scheduler_register(self.delete_old_dps, minute=f"*/{datapoint_cleaning_period}")

        # Schedule datapoint archivation
        self.keep_timedelta = timedelta(days=self.config["datapoint_archivation"]["days_to_keep"])
        self.log_dir = self._ensure_log_dir(self.config["datapoint_archivation"]["archive_dir"])
        registrar.scheduler_register(self.archive_old_dps, minute=0, hour=2)  # Every day at 2 AM

    def delete_old_dps(self):
        """Deletes old data points from master collection."""
        self.log.debug("Deleting old records ...")

        for etype_attr, attr_conf in self.model_spec.attributes.items():
            etype, attr_name = etype_attr
            max_age = None

            if attr_conf.t == AttrType.OBSERVATIONS:
                max_age = attr_conf.history_params.max_age
            elif attr_conf.t == AttrType.TIMESERIES:
                max_age = attr_conf.timeseries_params.max_age

            if not max_age:
                continue

            t_old = datetime.utcnow() - max_age

            try:
                self.db.delete_old_dps(etype, attr_name, t_old)
            except DatabaseError as e:
                self.log.error(e)

    def archive_old_dps(self):
        """
        Archives old data points from raw collection.

        Updates already saved archive files, if present.
        """

        t_old = datetime.utcnow() - self.keep_timedelta
        t_old = t_old.replace(hour=0, minute=0, second=0, microsecond=0)
        self.log.debug("Archiving all records before %s ...", t_old)

        max_date, min_date, total_dps = self._get_raw_dps_summary(t_old)
        if total_dps == 0:
            self.log.debug("Found no datapoints to archive.")
            return
        self.log.debug(
            "Found %s datapoints to archive in the range %s - %s", total_dps, min_date, max_date
        )

        n_days = (max_date - min_date).days + 1
        for date, next_date in [
            (min_date + timedelta(days=n), min_date + timedelta(days=n + 1)) for n in range(n_days)
        ]:
            date_string = date.strftime("%Y%m%d")
            day_datapoints = []
            for etype in self.model_spec.entities:
                result_cursor = self.db.get_raw(etype, after=date, before=next_date)
                for dp in result_cursor:
                    day_datapoints.append(self._reformat_dp(dp))
            self.log.debug("%s: %s datapoints to archive", date_string, len(day_datapoints))
            if not day_datapoints:
                continue

            date_logfile = self.log_dir / f"dp-log-{date_string}.json.gz"
            if date_logfile.exists():
                with gzip.open(date_logfile, "rt", encoding="utf-8") as archive:
                    saved = json.load(archive)
                self.log.debug("%s: %s already saved on disk", date_string, len(saved))
                saved.extend(day_datapoints)
                day_datapoints = saved
                self.log.debug(
                    "%s: Archiving %s datapoints in total", date_string, len(day_datapoints)
                )

            with gzip.open(date_logfile, "wt", encoding="utf-8") as archive:
                json.dump(day_datapoints, archive, cls=DatetimeEncoder)
            self.log.debug("%s: written to %s", date_string, date_logfile)

            deleted_count = 0
            for etype in self.model_spec.entities:
                deleted_res = self.db.delete_old_raw_dps(etype, next_date)
                deleted_count += deleted_res.deleted_count
            self.log.debug("Deleted %s datapoints", deleted_count)

    @staticmethod
    def _reformat_dp(dp):
        del dp["_id"]
        dp["id"] = dp["eid"]
        dp["type"] = dp["etype"]
        del dp["eid"]
        del dp["etype"]
        return dp

    def _get_raw_dps_summary(
        self, before: datetime
    ) -> tuple[Optional[datetime], Optional[datetime], int]:
        date_ranges = []
        for etype in self.model_spec.entities:
            result_cursor = self.db.get_raw_summary(etype, before=before)
            for range_summary in result_cursor:
                date_ranges.append(range_summary)
        if not date_ranges:
            return None, None, 0
        min_date = min(x["earliest"] for x in date_ranges).replace(
            hour=0, minute=0, second=0, microsecond=0
        )
        max_date = max(x["latest"] for x in date_ranges).replace(
            hour=0, minute=0, second=0, microsecond=0
        )
        total_dps = sum(x["count"] for x in date_ranges)
        return max_date, min_date, total_dps

    @staticmethod
    def _ensure_log_dir(log_dir_path: str):
        log_dir = Path(log_dir_path)
        log_dir.mkdir(parents=True, exist_ok=True)
        return log_dir
