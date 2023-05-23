import gzip
import json
import logging
import os
from datetime import datetime, timedelta
from json import JSONEncoder
from pathlib import Path
from typing import Any, Optional

from dp3.common.attrspec import AttrSpecType, AttrType, ObservationsHistoryParams
from dp3.common.callback_registrar import CallbackRegistrar
from dp3.common.config import PlatformConfig
from dp3.database.database import DatabaseError, EntityDatabase

DB_SEND_CHUNK = 100


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

        # Schedule master document aggregation
        registrar.scheduler_register(self.aggregate_master_docs, minute="*/10")

        if platform_config.process_index != 0:
            self.log.debug(
                "History management will be disabled in this worker to avoid race conditions."
            )
            return

        # Schedule datapoints cleaning
        datapoint_cleaning_period = self.config["datapoint_cleaning"]["tick_rate"]
        registrar.scheduler_register(self.delete_old_dps, minute=f"*/{datapoint_cleaning_period}")

        snapshot_cleaning_cron = self.config["snapshot_cleaning"]["cron_schedule"]
        self.keep_snapshot_delta = timedelta(days=self.config["snapshot_cleaning"]["days_to_keep"])
        registrar.scheduler_register(self.delete_old_snapshots, **snapshot_cleaning_cron)

        # Schedule datapoint archivation
        self.keep_raw_delta = timedelta(days=self.config["datapoint_archivation"]["days_to_keep"])
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

    def delete_old_snapshots(self):
        """Deletes old snapshots."""
        t_old = datetime.now() - self.keep_snapshot_delta
        self.log.debug("Deleting all snapshots before %s", t_old)

        deleted_total = 0
        for etype in self.model_spec.entities:
            try:
                result = self.db.delete_old_snapshots(etype, t_old)
                deleted_total += result.deleted_count
            except DatabaseError as e:
                self.log.exception(e)
        self.log.debug("Deleted %s snapshots in total.", deleted_total)

    def archive_old_dps(self):
        """
        Archives old data points from raw collection.

        Updates already saved archive files, if present.
        """

        t_old = datetime.utcnow() - self.keep_raw_delta
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
            day_datapoints = 0
            date_logfile = self.log_dir / f"dp-log-{date_string}.json"

            with open(date_logfile, "w", encoding="utf-8") as logfile:
                first = True
                for etype in self.model_spec.entities:
                    result_cursor = self.db.get_raw(etype, after=date, before=next_date)
                    for dp in result_cursor:
                        if first:
                            logfile.write(
                                f"[\n{json.dumps(self._reformat_dp(dp), cls=DatetimeEncoder)}"
                            )
                            first = False
                        else:
                            logfile.write(
                                f",\n{json.dumps(self._reformat_dp(dp), cls=DatetimeEncoder)}"
                            )
                        day_datapoints += 1
                logfile.write("\n]")
            self.log.debug(
                "%s: Archived %s datapoints to %s", date_string, day_datapoints, date_logfile
            )
            compress_file(date_logfile)
            os.remove(date_logfile)
            self.log.debug("%s: Saved archive was compressed", date_string)

            if not day_datapoints:
                continue

            deleted_count = 0
            for etype in self.model_spec.entities:
                deleted_res = self.db.delete_old_raw_dps(etype, next_date)
                deleted_count += deleted_res.deleted_count
            self.log.debug("%s: Deleted %s datapoints", date_string, deleted_count)

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

    def aggregate_master_docs(self):
        self.log.debug("Starting master documents aggregation.")
        start = datetime.now()
        entities = 0
        if self.worker_index == 0:
            self.db.save_metadata(start, {"entities": 0, "aggregation_start": start})

        for entity in self.model_spec.entities:
            entity_attr_specs = self.model_spec.entity_attributes[entity]
            eids = []
            aggregated_records = []
            records_cursor = self.db.get_worker_master_records(
                self.worker_index, self.num_workers, entity, no_cursor_timeout=True
            )
            try:
                for master_document in records_cursor:
                    entities += 1
                    self.aggregate_master_doc(entity_attr_specs, master_document)
                    eids.append(master_document["_id"])
                    aggregated_records.append(master_document)

                    if len(aggregated_records) > DB_SEND_CHUNK:
                        self.db.update_master_records(entity, eids, aggregated_records)
                        eids.clear()
                        aggregated_records.clear()

                if aggregated_records:
                    self.db.update_master_records(entity, eids, aggregated_records)
                    eids.clear()
                    aggregated_records.clear()
            finally:
                records_cursor.close()

        self.db.update_metadata(
            start, metadata={"aggregation_end": datetime.now()}, increase={"entities": entities}
        )
        self.log.debug("Master documents aggregation end.")

    @staticmethod
    def aggregate_master_doc(attr_specs: dict[str, AttrSpecType], master_document: dict):
        for attr, history in master_document.items():
            if attr not in attr_specs:
                continue
            spec = attr_specs[attr]

            if spec.t != AttrType.OBSERVATIONS or not spec.history_params.aggregate:
                continue

            master_document[attr] = aggregate_dp_history_on_equal(history, spec.history_params)


def aggregate_dp_history_on_equal(history: list[dict], spec: ObservationsHistoryParams):
    """
    Merge datapoints in the history with equal values and overlapping time validity.

    Avergages the confidence.
    """
    history = sorted(history, key=lambda x: x["t1"])
    aggregated_history = []
    current_dp = None
    merged_cnt = 0
    pre = spec.pre_validity
    post = spec.post_validity

    for dp in history:
        if not current_dp:
            current_dp = dp
            merged_cnt += 1
            continue

        if current_dp["v"] == dp["v"] and current_dp["t2"] + post >= dp["t1"] - pre:
            current_dp["t2"] = max(dp["t2"], current_dp["t2"])
            current_dp["c"] += dp["c"]
            merged_cnt += 1
        else:
            aggregated_history.append(current_dp)
            current_dp["c"] /= merged_cnt

            merged_cnt = 1
            current_dp = dp
    if current_dp:
        current_dp["c"] /= merged_cnt
        aggregated_history.append(current_dp)
    return aggregated_history


def compress_file(original: Path, compressed: Path = None):
    if compressed is None:
        compressed = original.parent / (original.name + ".gz")

    with open(original, encoding="utf-8") as in_fp, gzip.open(
        compressed, "wt", encoding="utf-8"
    ) as out_fp:
        out_fp.writelines(in_fp)
