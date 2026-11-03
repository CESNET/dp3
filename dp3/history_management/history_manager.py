import gzip
import json
import logging
import os
import time
from collections.abc import Callable
from dataclasses import asdict, dataclass
from datetime import UTC, datetime, timedelta
from functools import partial
from itertools import chain
from pathlib import Path
from typing import TypeVar

from pydantic import BaseModel, ConfigDict, Field
from pymongo.errors import PyMongoError

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
from dp3.database.database import (
    MASTER_REVISION_FIELD,
    DatabaseError,
    EntityDatabase,
    MasterRecordUpdate,
    run_with_db_retries,
)

DB_SEND_CHUNK = 100
AGGREGATION_MAX_RETRIES = 3
CLEANING_MAX_CURSOR_RESTARTS = 3
CLEANING_PROGRESS_INTERVAL = 300  # seconds
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
    """Configuration for pacing of datapoint cleaning.

    Attributes:
        chunk_size: Number of master records cleaned by a single database update.
        chunk_pause: Pause between consecutive chunks, to leave room for other database work.
    """

    model_config = ConfigDict(extra="forbid")

    chunk_size: int = Field(default=200, gt=0)
    chunk_pause: ParsedTimedelta = timedelta(seconds=1)


class HistoryManagerConfig(BaseModel):
    """Configuration for history manager.

    Attributes:
        aggregation_schedule: Schedule for master document aggregation.
        datapoint_cleaning_schedule: Schedule for datapoint cleaning.
        datapoint_cleaning: Pacing of datapoint cleaning.
        mark_datapoints_schedule: Deprecated schedule for marking datapoints in master docs.
        snapshot_cleaning: Configuration for snapshot cleaning.
        datapoint_archivation: Configuration for datapoint archivation.
    """

    model_config = ConfigDict(extra="forbid")

    aggregation_schedule: CronExpression
    datapoint_cleaning_schedule: CronExpression
    datapoint_cleaning: DatapointCleaningConfig = DatapointCleaningConfig()
    mark_datapoints_schedule: CronExpression | None = None
    snapshot_cleaning: SnapshotCleaningConfig
    datapoint_archivation: DPArchivationConfig


@dataclass
class _CleaningStats:
    """Progress of datapoint cleaning of a single entity type."""

    start: float
    records: int = 0
    modified: int = 0
    chunks: int = 0
    failed_chunks: int = 0
    update_time: float = 0.0
    pause_time: float = 0.0
    slowest_chunk: float = 0.0
    cursor_restarts: int = 0
    last_progress_log: float = 0.0

    def __post_init__(self):
        self.last_progress_log = self.start


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
        if self.config.mark_datapoints_schedule is not None:
            self.log.warning(
                "The history_manager.mark_datapoints_schedule option is deprecated and ignored."
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
        """Deletes old data points from master collection.

        Master records with any expired history are cleaned in chunks, each record is rewritten
        once for all of its expired histories. Chunks are separated by a configurable pause.
        """
        self.log.debug("Deleting old records ...")
        now = datetime.now(UTC)
        results = []
        try:
            self.db.save_metadata(now, {"cleaning_start": now})
        except DatabaseError:
            self.log.exception("Saving cleaning metadata failed")

        for etype in self.model_spec.entities:
            t_olds = {}
            for attr_name, attr_conf in self.model_spec.entity_attributes[etype].items():
                max_age = None
                if attr_conf.t == AttrType.OBSERVATIONS:
                    max_age = attr_conf.history_params.max_age
                elif attr_conf.t == AttrType.TIMESERIES:
                    max_age = attr_conf.timeseries_params.max_age
                if max_age:
                    t_olds[attr_name] = now - max_age

            if not t_olds:
                continue

            stats = _CleaningStats(start=time.monotonic())
            failed = False
            try:
                self._delete_old_dps_of_etype(etype, t_olds, stats)
            except Exception as e:
                failed = True
                self.log.exception("Deleting old datapoints of %s failed: %s", etype, e)
            result = asdict(stats)
            result["elapsed_seconds"] = time.monotonic() - result.pop("start")
            del result["last_progress_log"]
            results.append(result | {"entity": etype, "failed": failed})

        try:
            self.db.update_metadata(
                now,
                metadata={
                    "cleaning_start": now,
                    "cleaning_end": datetime.now(UTC),
                    "cleaning_metrics": {"entity_types": results},
                },
            )
        except DatabaseError:
            self.log.exception("Saving cleaning metadata failed")

    def _delete_old_dps_of_etype(
        self, etype: str, t_olds: dict[str, datetime], stats: _CleaningStats
    ):
        """Clean all master records of `etype` with datapoints older than `t_olds`.

        Cleaned records no longer match the candidate query, so a lost cursor
        (e.g. after a database restart) is recovered by simply running the query again.
        """
        try:
            while True:
                try:
                    self._delete_old_dps_pass(etype, t_olds, stats)
                    break
                except PyMongoError as e:
                    if stats.cursor_restarts >= CLEANING_MAX_CURSOR_RESTARTS:
                        raise
                    stats.cursor_restarts += 1
                    self.log.warning(
                        "Lost cursor while deleting old datapoints of %s (%s), restarting (%s/%s)",
                        etype,
                        e,
                        stats.cursor_restarts,
                        CLEANING_MAX_CURSOR_RESTARTS,
                    )
        finally:
            self.log.info(
                "Deleted old datapoints of %s: %s/%s master records modified in %s chunks "
                "(%s failed), %.1fs total (%.1fs updating, %.1fs paused, slowest chunk %.1fs), "
                "%s cursor restarts",
                etype,
                stats.modified,
                stats.records,
                stats.chunks,
                stats.failed_chunks,
                time.monotonic() - stats.start,
                stats.update_time,
                stats.pause_time,
                stats.slowest_chunk,
                stats.cursor_restarts,
            )

    def _delete_old_dps_pass(self, etype: str, t_olds: dict[str, datetime], stats: _CleaningStats):
        chunk_size = self.config.datapoint_cleaning.chunk_size
        chunk_pause = self.config.datapoint_cleaning.chunk_pause.total_seconds()

        eids = []
        # Fetching candidates in chunk-sized batches keeps them in cache for the update
        records_cursor = self.db.get_eids_with_old_dps(etype, t_olds, batch_size=chunk_size)
        try:
            for master_record in records_cursor:
                eids.append(master_record["_id"])
                if len(eids) < chunk_size:
                    continue
                self._delete_old_dps_chunk(etype, eids, t_olds, stats)
                eids = []

                pause_start = time.monotonic()
                time.sleep(chunk_pause)
                stats.pause_time += time.monotonic() - pause_start
                self._log_cleaning_progress(etype, stats)
        finally:
            records_cursor.close()

        if eids:
            self._delete_old_dps_chunk(etype, eids, t_olds, stats)

    def _delete_old_dps_chunk(
        self, etype: str, eids: list, t_olds: dict[str, datetime], stats: _CleaningStats
    ):
        start = time.monotonic()
        stats.chunks += 1
        stats.records += len(eids)
        try:
            res = self.db.delete_old_dps_of_eids(etype, eids, t_olds)
            stats.modified += res.modified_count
        except DatabaseError as e:
            stats.failed_chunks += 1
            self.log.error(e)
        finally:
            duration = time.monotonic() - start
            stats.update_time += duration
            stats.slowest_chunk = max(stats.slowest_chunk, duration)

    def _log_cleaning_progress(self, etype: str, stats: _CleaningStats):
        now = time.monotonic()
        if now - stats.last_progress_log < CLEANING_PROGRESS_INTERVAL:
            return
        stats.last_progress_log = now
        self.log.debug(
            "Deleting old datapoints of %s: %s/%s master records modified in %s chunks, "
            "%.0fs elapsed",
            etype,
            stats.modified,
            stats.records,
            stats.chunks,
            now - stats.start,
        )

    def delete_old_snapshots(self):
        """Deletes old snapshots."""
        t_old = datetime.now(UTC) - self.keep_snapshot_delta
        self.log.debug("Deleting all snapshots before %s", t_old)

        deleted_total = 0
        try:
            deleted_total = self.db.snapshots.delete_old(t_old)
        except DatabaseError as e:
            self.log.exception(e)
        self.log.debug("Deleted %s snapshots in total.", deleted_total)

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
            self.db.save_metadata(start, {"archivation_start": start})
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
            self.db.update_metadata(
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
        self.db.save_metadata(
            ts,
            {"aggregation_start": ts} | stats,
        )

        datapoint_counts = []
        for entity in self.model_spec.entities:
            attr_specs = {
                attr: spec
                for attr, spec in self.model_spec.entity_attributes[entity].items()
                if spec.t == AttrType.OBSERVATIONS and spec.history_params.aggregate
            }
            if not attr_specs:
                continue

            counts = {attr: {"attribute": attr, "before": 0, "after": 0} for attr in attr_specs}
            datapoint_counts.append({"entity": entity, "datapoints": list(counts.values())})
            projection = {
                **dict.fromkeys(attr_specs, True),
                "#ttl": True,
                "#time_created": True,
                MASTER_REVISION_FIELD: True,
            }
            aggregated_records = []
            records_cursor = self.db.get_worker_master_records(
                self.worker_index,
                self.num_workers,
                entity,
                projection=projection,
                no_cursor_timeout=True,
            )
            try:
                for master_document in records_cursor:
                    if entity_expired(ts, master_document):
                        continue  # Avoid expired entities to avoid conflict with garbage collector

                    stats["entities"] += 1
                    before = {attr: len(master_document.get(attr, [])) for attr in attr_specs}
                    fields = self.aggregate_master_doc(attr_specs, master_document)
                    # Computed initial-pass counts, not confirmed persisted changes.
                    for attr, count in counts.items():
                        count["before"] += before[attr]
                        count["after"] += len(fields.get(attr, master_document.get(attr, [])))
                    if not fields:
                        continue

                    aggregated_records.append(
                        (
                            master_document["_id"],
                            master_document.get(MASTER_REVISION_FIELD),
                            master_document.get("#time_created"),
                            fields,
                        )
                    )

                    if len(aggregated_records) >= DB_SEND_CHUNK:
                        self._update_master_records(
                            entity, attr_specs, projection, ts, aggregated_records, stats
                        )
                        aggregated_records.clear()

                if aggregated_records:
                    self._update_master_records(
                        entity, attr_specs, projection, ts, aggregated_records, stats
                    )
                    aggregated_records.clear()
            finally:
                records_cursor.close()

        self.db.update_metadata(
            ts,
            metadata={
                "aggregation_end": datetime.now(UTC),
                "aggregation_metrics": {"entity_types": datapoint_counts},
            },
            increase=stats,
        )
        self.log.debug("Master documents aggregation end: %s", stats)

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


def aggregate_multivalue_dp_history_on_equal(
    history: list[dict], spec: AttrSpecObservations
) -> tuple[list[dict], bool]:
    """
    Merge multivalue datapoints in the history with equal values and overlapping time validity.

    Avergages the confidence.
    Will keep a pool of "active" datapoints and merge them with the next datapoint
    if they have the same value and overlapping time validity.

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
