import logging
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

        # Schedule master document aggregation
        registrar.register_task_hook("on_task_start", self.note_latest_src_timestamp)

    def note_latest_src_timestamp(self, task: DataPointTask):
        updates = []
        for dp in task.data_points:
            has_timestamp = isinstance(dp, (DataPointObservationsBase, DataPointTimeseriesBase))
            if dp.src is None or not has_timestamp:
                self.log.debug("Skipping datapoint without src or timestamp: %s", dp)
                continue
            latest_timestamp = dp.t2 or dp.t1
            updates.append(
                UpdateOne(
                    {"_id": dp.src},
                    [{"$set": {"_id": dp.src, "src_t": {"$max": ["$src_t", latest_timestamp]}}}],
                    upsert=True,
                )
            )

        if not updates:
            return

        res = self.cache_col.bulk_write(updates)
        self.log.debug(
            "Updating %s src_timestamp records: %s modified",
            len(updates),
            res.modified_count,
        )


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
