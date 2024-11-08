import gzip
import json
import logging
import os
from datetime import datetime
from pathlib import Path
from typing import Optional

from pydantic import BaseModel, Extra

from dp3.common.attrspec import (
    AttrSpecObservations,
    AttrSpecType,
    AttrType,
    ObservationsHistoryParams,
)
from dp3.common.callback_registrar import CallbackRegistrar
from dp3.common.config import CronExpression, PlatformConfig
from dp3.common.types import DP3Encoder, ParsedTimedelta
from dp3.common.utils import entity_expired
from dp3.database.database import DatabaseError, EntityDatabase

DB_SEND_CHUNK = 100


class SnapshotCleaningConfig(BaseModel):
    """Configuration for snapshot cleaning.

    Attributes:
        schedule: Schedule for snapshot cleaning.
        older_than: Snapshots older than this will be deleted.
    """

    schedule: CronExpression
    older_than: ParsedTimedelta


class DPArchivationConfig(BaseModel):
    """Configuration for datapoint archivation.

    Attributes:
        schedule: Schedule for datapoint archivation.
        older_than: Datapoints older than this will be archived.
        archive_dir: Directory where to archive datapoints. Can be `None` to only delete them.
    """

    schedule: CronExpression
    older_than: ParsedTimedelta
    archive_dir: Optional[str] = None


class HistoryManagerConfig(BaseModel, extra=Extra.forbid):
    """Configuration for history manager.

    Attributes:
        aggregation_schedule: Schedule for master document aggregation.
        datapoint_cleaning_schedule: Schedule for datapoint cleaning.
        mark_datapoints_schedule: Schedule for marking datapoints in master docs.
        snapshot_cleaning: Configuration for snapshot cleaning.
        datapoint_archivation: Configuration for datapoint archivation.
    """

    aggregation_schedule: CronExpression
    datapoint_cleaning_schedule: CronExpression
    mark_datapoints_schedule: CronExpression
    snapshot_cleaning: SnapshotCleaningConfig
    datapoint_archivation: DPArchivationConfig


class HistoryManager:
    def __init__(
        self, db: EntityDatabase, platform_config: PlatformConfig, registrar: CallbackRegistrar
    ) -> None:
        self.log = logging.getLogger("HistoryManager")

        self.db = db
        self.model_spec = platform_config.model_spec
        self.worker_index = platform_config.process_index
        self.num_workers = platform_config.num_processes
        self.config = HistoryManagerConfig.model_validate(
            platform_config.config.get("history_manager")
        )

        # Schedule master document aggregation
        registrar.scheduler_register(
            self.aggregate_master_docs, **self.config.aggregation_schedule.model_dump()
        )

        if platform_config.process_index != 0:
            self.log.debug(
                "History management will be disabled in this worker to avoid race conditions."
            )
            return

        # Schedule datapoints cleaning
        datapoint_marking_schedule = self.config.mark_datapoints_schedule
        registrar.scheduler_register(
            self.mark_datapoints_in_master_docs, **datapoint_marking_schedule.model_dump()
        )

        datapoint_cleaning_schedule = self.config.datapoint_cleaning_schedule
        registrar.scheduler_register(
            self.delete_old_dps, **datapoint_cleaning_schedule.model_dump()
        )

        snapshot_cleaning_schedule = self.config.snapshot_cleaning.schedule
        self.keep_snapshot_delta = self.config.snapshot_cleaning.older_than
        registrar.scheduler_register(
            self.delete_old_snapshots, **snapshot_cleaning_schedule.model_dump()
        )

        # Schedule datapoint archivation
        archive_config = self.config.datapoint_archivation
        self.keep_raw_delta = archive_config.older_than
        if archive_config.archive_dir is not None:
            self.log_dir = self._ensure_log_dir(archive_config.archive_dir)
        else:
            self.log_dir = None
        registrar.scheduler_register(self.archive_old_dps, **archive_config.schedule.model_dump())

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

    def mark_datapoints_in_master_docs(self):
        """Marks the timestamps of all datapoints in master documents."""
        self.log.debug("Marking the datapoint timestamps for all entity records ...")

        for entity, attr_conf in self.model_spec.entity_attributes.items():
            attrs_to_mark = []
            for attr, conf in attr_conf.items():
                if conf.t in AttrType.OBSERVATIONS | AttrType.TIMESERIES:
                    attrs_to_mark.append(attr)

            if not attrs_to_mark:
                continue
            try:
                res = self.db.mark_all_entity_dps_t2(entity, attrs_to_mark)
                self.log.debug("Marked %s records of %s", res.modified_count, entity)
            except DatabaseError as e:
                self.log.error(e)

    def delete_old_snapshots(self):
        """Deletes old snapshots."""
        t_old = datetime.now() - self.keep_snapshot_delta
        self.log.debug("Deleting all snapshots before %s", t_old)

        deleted_total = 0
        try:
            deleted_total = self.db.snapshots.delete_old(t_old)
        except DatabaseError as e:
            self.log.exception(e)
        self.log.debug("Deleted %s snapshots in total.", deleted_total)

    def archive_old_dps(self):
        """
        Archives old data points from raw collection.

        Updates already saved archive files, if present.
        """

        t_old = datetime.utcnow() - self.keep_raw_delta
        self.log.debug("Archiving all records before %s ...", t_old)

        for etype in self.model_spec.entities:
            res = self.db.move_raw_to_archive(etype)
            if res:
                self.log.info("Current %s raw collection was moved to archive: %s", etype, res)

        max_date, min_date, total_dps = self._get_raw_dps_summary(t_old)
        if total_dps == 0:
            self.log.debug("Found no datapoints to archive.")
            return
        self.log.debug(
            "Found %s datapoints to archive in the range %s - %s", total_dps, min_date, max_date
        )

        if self.log_dir is None:
            self.log.debug("No archive directory specified, skipping archivation.")
        else:
            min_date_string = min_date.strftime("%Y%m%dT%H%M%S")
            max_date_string = max_date.strftime("%Y%m%dT%H%M%S")
            date_logfile = self.log_dir / f"dp-log-{min_date_string}--{max_date_string}.jsonl"
            datapoints = 0

            with open(date_logfile, "w", encoding="utf-8") as logfile:
                for etype in self.model_spec.entities:
                    for result_cursor in self.db.get_archive(etype, after=min_date, before=t_old):
                        for dp in result_cursor:
                            logfile.write(f"{json.dumps(self._reformat_dp(dp), cls=DP3Encoder)}\n")
                            datapoints += 1

            self.log.info("Archived %s datapoints to %s", datapoints, date_logfile)
            compress_file(date_logfile)
            os.remove(date_logfile)
            self.log.debug("Saved archive was compressed")

        deleted_count = 0
        for etype in self.model_spec.entities:
            for deleted_res in self.db.delete_old_archived_dps(etype, before=t_old):
                deleted_count += deleted_res.deleted_count
        self.log.info("Deleted %s datapoints", deleted_count)

        dropped_count = 0
        for etype in self.model_spec.entities:
            dropped_count += self.db.drop_empty_archives(etype)
        if dropped_count:
            self.log.info("Dropped %s empty archive collection(s)", dropped_count)

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
            summary = self.db.get_archive_summary(etype, before=before)
            if summary:
                date_ranges.append(summary)
        if not date_ranges:
            return None, None, 0
        min_date = min(x["earliest"] for x in date_ranges)
        max_date = max(x["latest"] for x in date_ranges)
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
        utcnow = datetime.utcnow()
        entities = 0
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
                    if entity_expired(utcnow, master_document):
                        continue  # Avoid expired entities to avoid conflict with garbage collector

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

            if spec.multi_value:
                master_document[attr] = aggregate_multivalue_dp_history_on_equal(history, spec)
            else:
                master_document[attr] = aggregate_dp_history_on_equal(history, spec.history_params)


def aggregate_multivalue_dp_history_on_equal(history: list[dict], spec: AttrSpecObservations):
    """
    Merge multivalue datapoints in the history with equal values and overlapping time validity.

    Avergages the confidence.
    Will keep a pool of "active" datapoints and merge them with the next datapoint
    if they have the same value and overlapping time validity.

    FIXME:
      The average calculation only works for the current iteration,
      but for the next call of the algorithm, the count of aggregated datapoints is lost.
    """
    history = sorted(history, key=lambda x: x["t1"])
    aggregated_history = []
    pre = spec.history_params.pre_validity
    post = spec.history_params.post_validity

    if spec.data_type.hashable:
        current_dps = {}

        for dp in history:
            v = dp["v"]
            if v in current_dps:
                current_dp = current_dps[v]
                if current_dp["t2"] + post >= dp["t1"] - pre:  # Merge with current_dp
                    current_dp["t2"] = max(dp["t2"], current_dp["t2"])
                    current_dp["c"] += dp["c"]
                    current_dp["cnt"] += 1
                else:  # No overlap, finalize current_dp and reset
                    current_dp["c"] /= current_dp["cnt"]
                    del current_dp["cnt"]
                    aggregated_history.append(current_dp)
                    current_dps[v] = dp
                    current_dps[v]["cnt"] = 1
            else:  # New value, finalize initialize current_dp
                current_dps[v] = dp
                current_dps[v]["cnt"] = 1

        for _v, current_dp in current_dps.items():  # Finalize remaining dps
            current_dp["c"] /= current_dp["cnt"]
            del current_dp["cnt"]
            aggregated_history.append(current_dp)
        return aggregated_history
    else:
        current_dps = []

        for dp in history:
            v = dp["v"]
            for i, current_dp in enumerate(current_dps):
                if current_dp["v"] != v:
                    continue

                if current_dp["t2"] + post >= dp["t1"] - pre:  # Merge with current_dp
                    current_dp["t2"] = max(dp["t2"], current_dp["t2"])
                    current_dp["c"] += dp["c"]
                    current_dp["cnt"] += 1
                else:  # No overlap, finalize current_dp and reset
                    current_dp["c"] /= current_dp["cnt"]
                    del current_dp["cnt"]
                    aggregated_history.append(current_dp)
                    dp["cnt"] = 1
                    current_dps[i] = dp
                break
            else:  # New value, finalize initialize current_dp
                dp["cnt"] = 1
                current_dps.append(dp)

        for current_dp in current_dps:  # Finalize remaining dps
            current_dp["c"] /= current_dp["cnt"]
            del current_dp["cnt"]
            aggregated_history.append(current_dp)
        return aggregated_history


def aggregate_dp_history_on_equal(history: list[dict], spec: ObservationsHistoryParams):
    """
    Merge datapoints in the history with equal values and overlapping time validity.

    Avergages the confidence.

    FIXME:
      The average calculation only works for the current iteration,
      but for the next call of the algorithm, the count of aggregated datapoints is lost.
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

    with (
        open(original, encoding="utf-8") as in_fp,
        gzip.open(compressed, "wt", encoding="utf-8") as out_fp,
    ):
        out_fp.writelines(in_fp)
