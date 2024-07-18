import logging
import threading
import time
from datetime import datetime

from pymongo import ASCENDING, UpdateOne

from dp3.common.callback_registrar import CallbackRegistrar
from dp3.common.config import PlatformConfig
from dp3.common.datapoint import DataPointObservationsBase, DataPointTimeseriesBase
from dp3.common.task import DataPointTask
from dp3.database.database import EntityDatabase


class Telemetry:
    def __init__(
        self, db: EntityDatabase, platform_config: PlatformConfig, registrar: CallbackRegistrar
    ) -> None:
        self.log = logging.getLogger("Telemetry")

        self.db = db
        self.model_spec = platform_config.model_spec
        # self.config = platform_config.config.get("telemetry")  # No config for now
        self.cache_col = self.db.get_module_cache("Telemetry")

        self.local_cache = {}
        self.local_cache_lock = threading.Lock()

        # Schedule master document aggregation
        registrar.register_task_hook("on_task_start", self.note_latest_src_timestamp)
        mod = 30
        proc_i = platform_config.process_index
        n_proc = platform_config.num_processes
        spread_proc_index = proc_i * (mod // n_proc) if n_proc < mod else proc_i
        seconds = ",".join(f"{int(i)}" for i in range(60) if int(i - spread_proc_index) % mod == 0)
        registrar.scheduler_register(
            self.sync_to_db, second=seconds, minute="*", hour="*", misfire_grace_time=10
        )

    def note_latest_src_timestamp(self, task: DataPointTask):
        """Note the latest timestamp of each source in the task"""
        latest_timestamps = {}
        for dp in task.data_points:
            has_timestamp = isinstance(dp, (DataPointObservationsBase, DataPointTimeseriesBase))
            if dp.src is None or not has_timestamp:
                continue
            latest_timestamp = dp.t2 or dp.t1
            latest_timestamps[dp.src] = latest_timestamp

        if not latest_timestamps:
            return

        with self.local_cache_lock:
            self.local_cache.update(latest_timestamps)

    def sync_to_db(self):
        """Sync local timestamp cache to database."""
        with self.local_cache_lock:
            synced_cache = self.local_cache
            self.local_cache = {}

        updates = [
            UpdateOne(
                {"_id": src},
                [{"$set": {"src_t": {"$max": ["$src_t", latest_timestamp]}}}],
            )
            for src, latest_timestamp in synced_cache.items()
        ]

        if not updates:
            return

        try:
            start = time.time()
            res = self.cache_col.bulk_write(updates, ordered=False)
            end = time.time()
            self.log.debug(
                "Updating %s src_timestamp records: %s matched %s modified in %.4fs",
                len(updates),
                res.matched_count,
                res.modified_count,
                (end - start),
            )
            if len(updates) != res.matched_count:
                upserts = [
                    UpdateOne(
                        {"_id": src},
                        [{"$set": {"_id": src, "src_t": {"$max": ["$src_t", latest_timestamp]}}}],
                        upsert=True,
                    )
                    for src, latest_timestamp in synced_cache.items()
                ]
                start = time.time()
                res = self.cache_col.bulk_write(upserts, ordered=False)
                end = time.time()
                self.log.debug(
                    "Upserting %s src_timestamp records: %s matched %s modified in %.4fs",
                    len(upserts),
                    res.matched_count,
                    res.modified_count,
                    (end - start),
                )
        except Exception as e:
            self.log.error("Error updating src_timestamp records: %s", e)


class TelemetryReader:
    """Reader of telemetry data

    Used by API.
    Not contained inside `Telemetry` class due to usage of `CallbackRegistrar`
    and all of it's requirements (doesn't make sense for API).
    """

    def __init__(self, db: EntityDatabase) -> None:
        self.db = db
        self.cache_col = self.db.get_module_cache("Telemetry")

    def get_sources_validity(self) -> dict[str, datetime]:
        """Return timestamps (datetimes) of current validity of all sources."""
        src_data = self.cache_col.find({}).sort([("_id", ASCENDING)])

        return {src["_id"]: src["src_t"] for src in src_data}
