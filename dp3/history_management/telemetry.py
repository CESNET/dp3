import logging
import threading
import time
from datetime import datetime

import requests
from pymongo import ASCENDING, UpdateOne

from dp3.common.callback_registrar import CallbackRegistrar
from dp3.common.config import PlatformConfig
from dp3.common.datapoint import DataPointObservationsBase, DataPointTimeseriesBase
from dp3.common.task import DataPointTask
from dp3.common.types import UTC
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
    """Reader of telemetry data.

    Used by API.
    Not contained inside `Telemetry` class due to usage of `CallbackRegistrar`
    and all of its requirements.
    """

    exported_queue_keys = {
        "messages": "total",
        "messages_ready": "ready",
        "messages_unacknowledged": "unacked",
        "consumers": "consumers",
        "memory": "memory",
        "message_bytes": "message_bytes",
    }
    exported_message_stats_keys = {
        "publish_details": "incoming",
        "deliver_get_details": "outgoing",
    }

    def __init__(
        self,
        db: EntityDatabase,
        app_name: str,
        num_processes: int,
        rabbit_config: dict,
    ) -> None:
        self.db = db
        self.app_name = app_name
        self.num_processes = num_processes
        self.rabbit_config = rabbit_config or {}
        self.cache_col = self.db.get_module_cache("Telemetry")

    def get_sources_validity(self) -> dict[str, datetime]:
        """Return timestamps (datetimes) of current validity of all sources."""
        src_data = self.cache_col.find({}).sort([("_id", ASCENDING)])
        return {src["_id"]: src["src_t"] for src in src_data}

    def get_sources_age(self, unit: str = "minutes") -> dict[str, int]:
        """Return ages of sources relative to now in requested units."""
        now = datetime.now(UTC)
        divider = 60 if unit == "minutes" else 1
        return {
            source: int((now - timestamp).total_seconds() / divider)
            for source, timestamp in self.get_sources_validity().items()
        }

    def get_entities_per_attr(self) -> dict[str, dict[str, int]]:
        """Return counts of entities with data present for each configured attribute."""
        return self.db.count_entities_per_attr()

    def get_snapshot_summary(self) -> dict:
        """Return summary of latest snapshot activity."""
        now = datetime.now(UTC)
        latest_started = next(self.db.find_metadata(module="SnapShooter").limit(1), None)
        latest_finished = next(
            self.db.find_metadata(
                module="SnapShooter",
                extra_filter={"workers_finished": self.num_processes, "linked_finished": True},
            ).limit(1),
            None,
        )

        summary = {
            "latest_age": None,
            "finished_age": None,
            "entities": None,
            "total_s": None,
        }
        if latest_started is not None:
            summary["latest_age"] = (now - latest_started["#time_created"]).total_seconds()
        if latest_finished is not None:
            summary["finished_age"] = (now - latest_finished["#time_created"]).total_seconds()
            summary["entities"] = latest_finished.get("entities")
            summary["total_s"] = (
                latest_finished["#last_update"] - latest_finished["#time_created"]
            ).total_seconds()
        return summary

    def get_metadata(
        self,
        module: str = None,
        date_from: datetime = None,
        date_to: datetime = None,
        newest_first: bool = True,
        skip: int = 0,
        limit: int = 0,
    ) -> list[dict]:
        """Return filtered metadata documents."""
        cursor = self.db.find_metadata(module, date_from, date_to, newest_first)
        if skip:
            cursor = cursor.skip(skip)
        if limit:
            cursor = cursor.limit(limit)
        return list(cursor)

    def get_rabbitmq_queues(self) -> dict[str, list[dict]]:
        """Return RabbitMQ queue telemetry for this application."""
        host = self.rabbit_config.get("host", "localhost")
        port = int(self.rabbit_config.get("management_port", 15672))
        username = self.rabbit_config.get("username", "guest")
        password = self.rabbit_config.get("password", "guest")
        response = requests.get(
            f"http://{host}:{port}/api/queues",
            auth=(username, password),
            timeout=5,
        )
        response.raise_for_status()

        queues = []
        app_prefix = f"{self.app_name}-worker-"
        for queue in response.json():
            queue_name = queue.get("name", "")
            if not queue_name.startswith(app_prefix):
                continue

            short_name = queue_name[len(app_prefix) :]
            alias = short_name if len(short_name) > 2 else f"{short_name}-main"
            queue_data = {"name": queue_name, "queue": alias}
            for key, alias_key in self.exported_queue_keys.items():
                queue_data[alias_key] = queue.get(key, 0)

            message_stats = queue.get("message_stats", {})
            for key, alias_key in self.exported_message_stats_keys.items():
                queue_data[alias_key] = message_stats.get(key, {}).get("rate", 0)
            queues.append(queue_data)

        queues.sort(key=lambda item: item["queue"])
        return {"queues": queues}
