import gzip
import json
import logging
import os
import time
from collections.abc import Callable
from contextlib import nullcontext
from datetime import UTC, datetime, timedelta
from functools import partial
from itertools import chain
from pathlib import Path
from typing import TypeVar

from pydantic import BaseModel, ConfigDict

from dp3.common.attrspec import (
    AttrSpecObservations,
    AttrSpecType,
    AttrType,
    ObservationsHistoryParams,
)
from dp3.common.callback_registrar import CallbackRegistrar
from dp3.common.config import CronExpression, PlatformConfig
from dp3.common.types import DP3Encoder, ParsedTimedelta
from dp3.common.utils import entity_expired, error_summary
from dp3.core.updater import UpdateBatch, UpdatePeriod, Updater
from dp3.database.database import (
    MASTER_REVISION_FIELD,
    DatabaseError,
    EntityDatabase,
    MasterRecordUpdate,
    run_with_db_retries,
)

DB_SEND_CHUNK = 100
AGGREGATION_MAX_RETRIES = 3
# Archive collections created up to this long after the archivation cutoff are processed
ARCHIVE_TIME_TOLERANCE = timedelta(hours=1)
ARCHIVE_EXPORT_BATCH_SIZE = 1000
ARCHIVE_FILE_TIME_FORMAT = "%Y%m%dT%H%M%S"

T = TypeVar("T")


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
        older_than: Raw datapoints received longer ago than this will be archived.
        archive_dir: Directory where to archive datapoints. Can be `None` to only delete them.
    """

    schedule: CronExpression
    older_than: ParsedTimedelta
    archive_dir: str | None = None


class DatapointCleaningConfig(BaseModel):
    """Configuration of datapoint cleaning.

    Attributes:
        period: Every master record is cleaned once per this period. The records are cleaned
            continuously in small batches by the Updater, see `updater.update_batch_period`.
        chunk_size: Deprecated and ignored.
        chunk_pause: Deprecated and ignored.
    """

    model_config = ConfigDict(extra="forbid")

    period: ParsedTimedelta = timedelta(days=1)
    chunk_size: int | None = None
    chunk_pause: ParsedTimedelta | None = None


class HistoryManagerConfig(BaseModel):
    """Configuration for history manager.

    Attributes:
        aggregation_schedule: Schedule for master document aggregation.
        datapoint_cleaning_schedule: Deprecated and ignored, see `datapoint_cleaning.period`.
        datapoint_cleaning: Configuration of datapoint cleaning.
        mark_datapoints_schedule: Deprecated schedule for marking datapoints in master docs.
        snapshot_cleaning: Configuration for snapshot cleaning.
        datapoint_archivation: Configuration for datapoint archivation.
    """

    model_config = ConfigDict(extra="forbid")

    aggregation_schedule: CronExpression
    datapoint_cleaning_schedule: CronExpression | None = None
    datapoint_cleaning: DatapointCleaningConfig = DatapointCleaningConfig()
    mark_datapoints_schedule: CronExpression | None = None
    snapshot_cleaning: SnapshotCleaningConfig
    datapoint_archivation: DPArchivationConfig


class HistoryManager:
    def __init__(
        self,
        db: EntityDatabase,
        platform_config: PlatformConfig,
        registrar: CallbackRegistrar,
        updater: Updater,
    ) -> None:
        self.log = logging.getLogger("HistoryManager")

        self.db = db
        self.model_spec = platform_config.model_spec
        self.worker_index = platform_config.process_index
        self.num_workers = platform_config.num_processes
        self.config = HistoryManagerConfig.model_validate(
            platform_config.config.get("history_manager")
        )
        if self.config.mark_datapoints_schedule is not None:
            self.log.warning(
                "The history_manager.mark_datapoints_schedule option is deprecated and ignored."
            )
        for option, value in [
            ("datapoint_cleaning_schedule", self.config.datapoint_cleaning_schedule),
            ("datapoint_cleaning.chunk_size", self.config.datapoint_cleaning.chunk_size),
            ("datapoint_cleaning.chunk_pause", self.config.datapoint_cleaning.chunk_pause),
        ]:
            if value is not None:
                self.log.warning(
                    "The history_manager.%s option is deprecated and ignored, "
                    "datapoints are cleaned continuously over datapoint_cleaning.period.",
                    option,
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

        # Clean datapoints continuously, a batch of master records at a time
        cleaning_period = self.config.datapoint_cleaning.period
        for etype in self.model_spec.entities:
            max_ages = self._history_max_ages(etype)
            if not max_ages:
                continue
            updater.register_batch_update_hook(
                partial(self._delete_old_dps_batch, max_ages=max_ages),
                hook_id="history_manager_datapoint_cleaning",
                entity_type=etype,
                period=cleaning_period,
                on_period_end=self._end_cleaning_period,
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

    def _history_max_ages(self, etype: str) -> dict[str, timedelta]:
        """Get `max_age` of all history attributes of `etype` which have it configured."""
        max_ages = {}
        for attr_name, attr_conf in self.model_spec.entity_attributes[etype].items():
            max_age = None
            if attr_conf.t == AttrType.OBSERVATIONS:
                max_age = attr_conf.history_params.max_age
            elif attr_conf.t == AttrType.TIMESERIES:
                max_age = attr_conf.timeseries_params.max_age
            if max_age:
                max_ages[attr_name] = max_age
        return max_ages

    def _delete_old_dps_batch(
        self, etype: str, batch: UpdateBatch, max_ages: dict[str, timedelta]
    ) -> dict[str, int]:
        """Delete expired datapoints from a batch of master records of `etype`.

        Called by the Updater for every batch, so that all master records are cleaned once
        per `datapoint_cleaning.period`. Each record is rewritten once for all of its
        expired histories. Returns metrics of the batch, summed over the period by the Updater.
        """
        now = datetime.now(UTC)
        t_olds = {attr_name: now - max_age for attr_name, max_age in max_ages.items()}
        failed = False
        matched = modified = 0
        try:
            res = self.db.delete_old_dps(etype, t_olds, batch.query_filter)
            matched, modified = res.matched_count, res.modified_count
        except DatabaseError as e:
            failed = True
            self.log.error("Deleting old datapoints of %s failed: %s", etype, e)

        self.log.debug(
            "Deleted old datapoints of %s in batch %s/%s: %s/%s master records modified",
            etype,
            batch.iteration + 1,
            batch.total_iterations,
            modified,
            matched,
        )
        return {
            "batches": 1,
            "failed_batches": int(failed),
            "records": matched,
            "modified": modified,
        }

    def _end_cleaning_period(self, etype: str, period: UpdatePeriod):
        """Save metrics of a finished datapoint cleaning period of `etype`.

        Can be called again for the same period after a worker restart, with the same totals.
        """
        metrics = period.metrics
        self.log.info(
            "Finished cleaning period of %s: %s/%s master records modified in %s batches "
            "(%s failed), %.1fs updating, slowest batch %.1fs",
            etype,
            metrics.get("modified", 0),
            metrics.get("records", 0),
            metrics.get("batches", 0),
            metrics.get("failed_batches", 0),
            period.runtime_secs,
            period.slowest_batch_secs,
        )
        try:
            self.db.metadata.update(
                period.period_start,
                metadata={
                    "#module": self.__class__.__qualname__,
                    "#time_created": period.period_start,
                    "cleaning_start": period.period_start,
                    "cleaning_end": period.period_end,
                    "entity": etype,
                    "cleaning_period": self.config.datapoint_cleaning.period.total_seconds(),
                    "cleaning_metrics": metrics
                    | {
                        "update_time": period.runtime_secs,
                        "slowest_batch": period.slowest_batch_secs,
                    },
                },
                # Periods of entity types may start at the same time
                key=etype,
            )
        except DatabaseError:
            self.log.exception("Saving cleaning metadata failed")

    def delete_old_snapshots(self):
        """Deletes old snapshots, one entity type after another.

        A failure of one entity type is logged and recorded, the others are still cleaned.
        """
        start = datetime.now(UTC)
        t_old = start - self.keep_snapshot_delta
        self.log.debug("Deleting all snapshots before %s", t_old)

        stats = {"buckets": 0, "oversized": 0, "failures": 0}
        # The snapshots are cleaned also when the run can't be recorded
        run_end = nullcontext({})
        try:
            self.db.metadata.save(
                start, {"snapshot_cleaning_start": start, "deleted_before": t_old} | stats
            )
            run_end = self.db.metadata.record_run_end(start, "snapshot_cleaning_end", stats)
        except DatabaseError:
            self.log.exception("Saving snapshot cleaning metadata failed")

        entity_types = []
        with run_end as end:
            end["snapshot_cleaning_metrics"] = {"entity_types": entity_types}
            for etype in self.model_spec.entities:
                metrics = {"entity": etype}
                entity_types.append(metrics)
                etype_start = time.monotonic()
                try:
                    deleted = self.db.snapshots.delete_old(etype, t_old)
                    metrics.update(deleted)
                    for name, count in deleted.items():
                        stats[name] += count
                except DatabaseError as e:
                    stats["failures"] += 1
                    metrics["error"] = error_summary(e)
                    self.log.exception("Deleting old snapshots of %s failed", etype)
                metrics["duration"] = time.monotonic() - etype_start

        self.log.info(
            "Snapshot cleaning done: %s snapshot bucket(s) and %s oversized snapshot(s) deleted, "
            "%s failure(s)",
            stats["buckets"],
            stats["oversized"],
            stats["failures"],
        )

    def archive_old_dps(self):
        """Archives old datapoints from raw collections.

        1. Current raw collections are renamed to new archive collections, each holding the raw
           datapoints received between two runs.
        2. Archive collections created more than `older_than` ago are selected, based on
           the creation time in their name.
        3. All their datapoints are exported to a compressed file in `archive_dir`
           (skipped if `archive_dir` is not set), and they are dropped.
        4. Any other empty archive collections are dropped.

        Transient database errors (e.g. a lost connection) are retried. The export is all or
        nothing: if it fails, no selected archive collection is dropped and the export is repeated
        by the next run. Renaming, selection and dropping are done per entity type and per
        archive collection, so a failure there does not stop the others.
        """
        start = datetime.now(UTC)
        t_old = start - self.keep_raw_delta
        stats = {
            "archives": 0,
            "exported": 0,
            "future_dated": 0,
            "dropped": 0,
            "export_failed": False,
            "failures": 0,
            "file": None,
        }
        self.log.debug("Archiving raw datapoints received before %s ...", t_old)
        try:
            self.db.metadata.save(start, {"archivation_start": start})
        except DatabaseError:
            self.log.exception("Saving archivation metadata failed")

        # Runs a database step with retries, a failed step is logged and counted in `stats`
        step = partial(self._archivation_step, stats)

        archives = {}
        for etype in self.model_spec.entities:
            if res := step(f"Move of {etype} raw collection", self.db.move_raw_to_archive, etype):
                self.log.info("Current %s raw collection was moved to archive: %s", etype, res)
            archives[etype] = step(
                f"Listing of {etype} archive collections",
                self._get_archives_to_process,
                etype,
                t_old,
                start,
                default=[],
            )
            stats["archives"] += len(archives[etype])

        exported = True
        if self.log_dir is None:
            self.log.debug("No archive directory specified, skipping export.")
        elif stats["archives"]:
            try:
                stats["exported"], stats["future_dated"], stats["file"] = self._export_archives(
                    archives, start
                )
            except Exception as e:
                exported = False
                stats["export_failed"] = True
                self.log.exception(
                    "Export of archived datapoints failed, archive collections are kept "
                    "for the next run: %s",
                    e,
                )

        if exported:
            for archive_col_name in chain.from_iterable(archives.values()):
                if step(f"Drop of {archive_col_name}", self.db.drop_archive, archive_col_name):
                    stats["dropped"] += 1

        for etype in self.model_spec.entities:
            stats["dropped"] += step(
                f"Drop of empty {etype} archive collections",
                self.db.drop_empty_archives,
                etype,
                default=0,
            )

        self.log.info(
            "Archivation done: %s archive collection(s) processed, %s datapoints exported "
            "(%s future-dated), %s archive collection(s) dropped, %s failure(s)%s",
            stats["archives"],
            stats["exported"],
            stats["future_dated"],
            stats["dropped"],
            stats["failures"],
            ", export failed" if stats["export_failed"] else "",
        )
        try:
            self.db.metadata.update(
                start,
                metadata={
                    "archivation_start": start,
                    "archivation_end": datetime.now(UTC),
                    "archivation_metrics": stats,
                },
            )
        except DatabaseError:
            self.log.exception("Saving archivation metadata failed")

    def _archivation_step(
        self, stats: dict, description: str, operation: Callable[..., T], *args, default=None
    ) -> T:
        """Run a database step of archivation, retrying it after transient errors.

        A failed step is logged and counted in `stats`, and `default` is returned,
        so that a failure does not stop the remaining steps.
        """
        try:
            return run_with_db_retries(partial(operation, *args), description, self.log)
        except Exception as e:
            stats["failures"] += 1
            self.log.exception("%s failed: %s", description, e)
            return default

    def _get_archives_to_process(self, etype: str, t_old: datetime, start: datetime) -> list[str]:
        """Select archive collections of `etype` old enough to be exported and dropped.

        An archive collection holds datapoints received before its creation time,
        so it is processed once it was created before `t_old`. A tolerance allows
        for scheduling jitter and schedule changes, so that the archive collection created
        by the previous run is not left for one more run.
        Archive collections created by the current run (started at `start`) are never
        processed, even with a zero `older_than`.
        Archive collections with unrecognized names are always processed.
        """
        tolerance = min(ARCHIVE_TIME_TOLERANCE, self.keep_raw_delta / 2)
        # Archive names have a second precision
        run_start = start.replace(microsecond=0)
        selected = []
        for archive_col_name in self.db.get_archive_col_names(etype):
            created = self.db.get_archive_col_time(etype, archive_col_name)
            if created is None or (created <= t_old + tolerance and created < run_start):
                selected.append(archive_col_name)
        return selected

    def _export_archives(
        self, archives: dict[str, list[str]], start: datetime
    ) -> tuple[int, int, str | None]:
        """Export all datapoints of `archives` to a single compressed file in `archive_dir`.

        The file is named by the range of all exported `t1` values, or by the run start
        if no exported datapoint has a `t1`. Each archive collection is
        exported with retries; a retried collection is exported again from its beginning.
        Both the export and the compression are written to temporary files, the compressed file
        gets its final name only once complete. Temporary files are removed on failure.

        Returns the number of exported and future-dated datapoints, and the name of the file,
        if any.
        """
        partial_file = (
            self.log_dir / f"dp-log-partial-{start.strftime(ARCHIVE_FILE_TIME_FORMAT)}.jsonl"
        )
        partial_compressed = Path(f"{partial_file}.gz")
        datapoints, future_dated, t_min, t_max = 0, 0, None, None
        try:
            with open(partial_file, "wb") as logfile:
                for etype, archive_col_names in archives.items():
                    for archive_col_name in archive_col_names:
                        position = logfile.tell()
                        count, col_future, col_min, col_max = run_with_db_retries(
                            partial(
                                self._export_archive, logfile, position, etype, archive_col_name
                            ),
                            f"Export of {archive_col_name}",
                            self.log,
                        )
                        datapoints += count
                        future_dated += col_future
                        t_min = min(filter(None, (t_min, col_min)), default=None)
                        t_max = max(filter(None, (t_max, col_max)), default=None)

            if datapoints == 0:
                partial_file.unlink()
                self.log.debug("Found no datapoints to export.")
                return 0, 0, None

            compress_file(partial_file, partial_compressed)
            compressed = self._export_file_path(t_min or start, t_max or start)
            os.replace(partial_compressed, compressed)
        except BaseException:
            partial_compressed.unlink(missing_ok=True)
            raise
        finally:
            partial_file.unlink(missing_ok=True)

        self.log.info("Archived %s datapoints to %s", datapoints, compressed)
        return datapoints, future_dated, str(compressed)

    def _export_archive(
        self, logfile, position: int, etype: str, archive_col_name: str
    ) -> tuple[int, int, datetime | None, datetime | None]:
        """Export all datapoints of one archive collection to `logfile`, starting at `position`.

        Anything after `position` (left by a failed previous attempt) is overwritten.
        Returns the number of exported datapoints, the number of future-dated ones
        (`t1` later than the creation of the archive collection), and the range of their
        `t1` values.
        """
        logfile.seek(position)
        logfile.truncate()
        count, future_dated, t_min, t_max = 0, 0, None, None
        created = self.db.get_archive_col_time(etype, archive_col_name)
        cursor = self.db.get_archived_dps(archive_col_name, batch_size=ARCHIVE_EXPORT_BATCH_SIZE)
        try:
            for dp in cursor:
                t1 = dp.get("t1")
                if isinstance(t1, datetime):
                    t_min = t1 if t_min is None else min(t_min, t1)
                    t_max = t1 if t_max is None else max(t_max, t1)
                    if created is not None and t1 > created:
                        future_dated += 1
                line = json.dumps(self._reformat_dp(dp), cls=DP3Encoder) + "\n"
                logfile.write(line.encode("utf-8"))
                count += 1
        finally:
            cursor.close()
        if future_dated:
            self.log.warning(
                "%s datapoints of %s have t1 later than the creation of the archive "
                "collection (%s), at most %s",
                future_dated,
                archive_col_name,
                created,
                t_max,
            )
        return count, future_dated, t_min, t_max

    def _export_file_path(self, t_min: datetime, t_max: datetime) -> Path:
        """Returns a path of a new compressed export file for the `t_min` - `t_max` range.

        Never returns a path of an existing export file, compressed or not.
        """
        base = (
            f"dp-log-{t_min.strftime(ARCHIVE_FILE_TIME_FORMAT)}"
            f"--{t_max.strftime(ARCHIVE_FILE_TIME_FORMAT)}"
        )
        name = base
        suffix = 1
        while (self.log_dir / f"{name}.jsonl").exists() or (
            self.log_dir / f"{name}.jsonl.gz"
        ).exists():
            name = f"{base}-{suffix}"
            suffix += 1
        return self.log_dir / f"{name}.jsonl.gz"

    @staticmethod
    def _reformat_dp(dp):
        del dp["_id"]
        dp["id"] = dp["eid"]
        dp["type"] = dp["etype"]
        del dp["eid"]
        del dp["etype"]
        return dp

    @staticmethod
    def _ensure_log_dir(log_dir_path: str):
        log_dir = Path(log_dir_path)
        log_dir.mkdir(parents=True, exist_ok=True)
        return log_dir

    def aggregate_master_docs(self):
        self.log.debug("Starting master documents aggregation.")
        ts = datetime.now(UTC)
        stats = {
            "entities": 0,
            "updated": 0,
            "revision_conflicts": 0,
            "retries": 0,
            "retry_exhausted": 0,
        }
        self.db.metadata.save(
            ts,
            {"aggregation_start": ts} | stats,
        )

        datapoint_counts = []
        with self.db.metadata.record_run_end(ts, "aggregation_end", increase=stats) as end:
            end["aggregation_metrics"] = {"entity_types": datapoint_counts}
            for entity, attr_specs in self._aggregated_attr_specs().items():
                counts = {attr: {"attribute": attr, "before": 0, "after": 0} for attr in attr_specs}
                datapoint_counts.append({"entity": entity, "datapoints": list(counts.values())})
                self._aggregate_entity(entity, attr_specs, ts, counts, stats)
        self.log.debug("Master documents aggregation end: %s", stats)

    def _aggregated_attr_specs(self) -> dict[str, dict]:
        """Returns the specs of attributes with aggregated history, by entity type."""
        specs = {
            entity: {
                attr: spec
                for attr, spec in self.model_spec.entity_attributes[entity].items()
                if spec.t == AttrType.OBSERVATIONS and spec.history_params.aggregate
            }
            for entity in self.model_spec.entities
        }
        return {entity: attr_specs for entity, attr_specs in specs.items() if attr_specs}

    def _aggregate_entity(
        self, entity: str, attr_specs: dict, ts: datetime, counts: dict, stats: dict
    ):
        """Aggregates the datapoints of `attr_specs` in the worker's master records of `entity`."""
        projection = {
            **dict.fromkeys(attr_specs, True),
            "#ttl": True,
            "#time_created": True,
            MASTER_REVISION_FIELD: True,
        }
        aggregated_records = []
        with self.db.get_worker_master_records(
            self.worker_index,
            self.num_workers,
            entity,
            projection=projection,
            no_cursor_timeout=True,
        ) as records_cursor:
            for master_document in records_cursor:
                if entity_expired(ts, master_document):
                    continue  # Avoid expired entities to avoid conflict with garbage collector

                stats["entities"] += 1
                if update := self._aggregate_document(attr_specs, master_document, counts):
                    aggregated_records.append(update)
                if len(aggregated_records) >= DB_SEND_CHUNK:
                    self._update_master_records(
                        entity, attr_specs, projection, ts, aggregated_records, stats
                    )
                    aggregated_records.clear()

            if aggregated_records:
                self._update_master_records(
                    entity, attr_specs, projection, ts, aggregated_records, stats
                )

    def _aggregate_document(
        self, attr_specs: dict, master_document: dict, counts: dict
    ) -> MasterRecordUpdate | None:
        """Aggregates the datapoints of one master record, returns its update if any changed."""
        before = {attr: len(master_document.get(attr, [])) for attr in attr_specs}
        fields = self.aggregate_master_doc(attr_specs, master_document)
        # Computed initial-pass counts, not confirmed persisted changes.
        for attr, count in counts.items():
            count["before"] += before[attr]
            count["after"] += len(fields.get(attr, master_document.get(attr, [])))
        if not fields:
            return None
        return (
            master_document["_id"],
            master_document.get(MASTER_REVISION_FIELD),
            master_document.get("#time_created"),
            fields,
        )

    def _update_master_records(
        self,
        entity: str,
        attr_specs: dict[str, AttrSpecType],
        projection: dict,
        aggregation_time: datetime,
        records: list[MasterRecordUpdate],
        stats: dict[str, int],
    ) -> None:
        pending = records
        for retry in range(AGGREGATION_MAX_RETRIES + 1):
            submitted = pending
            matched = self.db.update_master_records(entity, submitted)
            stats["updated"] += matched
            conflicts = len(submitted) - matched
            if not conflicts:
                return

            stats["revision_conflicts"] += conflicts
            if retry == AGGREGATION_MAX_RETRIES:
                stats["retry_exhausted"] += conflicts
                self.log.warning(
                    "Aggregation of %s master records of %s exhausted revision retries.",
                    conflicts,
                    entity,
                )
                return

            stats["retries"] += 1
            pending = []
            records_cursor = self.db.get_master_records_by_eids(
                entity,
                [record[0] for record in submitted],
                projection=projection,
            )
            try:
                for master_document in records_cursor:
                    if entity_expired(aggregation_time, master_document):
                        continue
                    fields = self.aggregate_master_doc(attr_specs, master_document)
                    if fields:
                        pending.append(
                            (
                                master_document["_id"],
                                master_document.get(MASTER_REVISION_FIELD),
                                master_document.get("#time_created"),
                                fields,
                            )
                        )
            finally:
                records_cursor.close()

            if not pending:
                return

    @staticmethod
    def aggregate_master_doc(
        attr_specs: dict[str, AttrSpecType], master_document: dict
    ) -> dict[str, list[dict]]:
        aggregated_fields = {}
        for attr, spec in attr_specs.items():
            if attr not in master_document:
                continue
            history = master_document[attr]

            if spec.multi_value:
                aggregated_history, changed = aggregate_multivalue_dp_history_on_equal(
                    history, spec
                )
            else:
                aggregated_history, changed = aggregate_dp_history_on_equal(
                    history, spec.history_params
                )

            if changed:
                aggregated_fields[attr] = aggregated_history

        return aggregated_fields


def _restore_stored_order(aggregated: list[dict], original: list[dict]):
    """Order `aggregated` by `t1`, keeping the stored relative order of equal `t1` datapoints.

    Aggregated datapoints are the original datapoint objects (modified in place),
    so their original position serves as the tie-breaker. Unlike a `t2` tie-breaker,
    the position does not change when a merge extends a datapoint's `t2`.
    """
    stored_position = {id(dp): i for i, dp in enumerate(original)}
    aggregated.sort(key=lambda dp: (dp["t1"], stored_position[id(dp)]))


def aggregate_multivalue_dp_history_on_equal(
    history: list[dict], spec: AttrSpecObservations
) -> tuple[list[dict], bool]:
    """
    Merge multivalue datapoints in the history with equal values and overlapping time validity.

    Avergages the confidence.
    Will keep a pool of "active" datapoints and merge them with the next datapoint
    if they have the same value and overlapping time validity.

    The result is ordered by `t1`, datapoints with equal `t1` keep their stored relative order.
    Already aggregated datapoints therefore keep their positions between consecutive runs;
    when newly appended datapoints merge into stored ones, only the merged positions
    and the array's tail change. Removing or inserting datapoints mid-history shifts the rest.

    FIXME:
      The average calculation only works for the current iteration,
      but for the next call of the algorithm, the count of aggregated datapoints is lost.
    """
    original_history = history
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
        _restore_stored_order(aggregated_history, original_history)
        return aggregated_history, history_was_aggregated(original_history, aggregated_history)
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
        _restore_stored_order(aggregated_history, original_history)
        return aggregated_history, history_was_aggregated(original_history, aggregated_history)


def aggregate_dp_history_on_equal(
    history: list[dict], spec: ObservationsHistoryParams
) -> tuple[list[dict], bool]:
    """
    Merge datapoints in the history with equal values and overlapping time validity.

    Avergages the confidence.

    FIXME:
      The average calculation only works for the current iteration,
      but for the next call of the algorithm, the count of aggregated datapoints is lost.
    """
    original_history = history
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
    return aggregated_history, history_was_aggregated(original_history, aggregated_history)


def history_was_aggregated(original: list[dict], aggregated: list[dict]) -> bool:
    """Return whether aggregation removed or reordered any datapoints."""
    return len(original) != len(aggregated) or any(
        before is not after for before, after in zip(original, aggregated, strict=True)
    )


def compress_file(original: Path, compressed: Path = None):
    if compressed is None:
        compressed = original.parent / (original.name + ".gz")

    with (
        open(original, encoding="utf-8") as in_fp,
        gzip.open(compressed, "wt", encoding="utf-8") as out_fp,
    ):
        out_fp.writelines(in_fp)
