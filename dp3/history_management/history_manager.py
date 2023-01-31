import logging
from datetime import datetime

from dp3 import g
from dp3.common.attrspec import AttrType
from dp3.common.config import HierarchicalDict, ModelSpec
from dp3.database.database import DatabaseError, EntityDatabase


class HistoryManager:
    def __init__(
        self,
        db: EntityDatabase,
        model_spec: ModelSpec,
        worker_index: int,
        num_workers: int,
        config: HierarchicalDict,
    ) -> None:
        self.log = logging.getLogger("HistoryManager")

        self.db = db
        self.model_spec = model_spec
        self.worker_index = worker_index
        self.num_workers = num_workers
        self.config = config

        if worker_index != 0:
            self.log.debug(
                "History management will be disabled in this worker to avoid race conditions."
            )
            return

        # Schedule datapoints cleaning
        datapoint_cleaning_period = self.config["datapoint_cleaning"]["tick_rate"]
        g.scheduler.register(self.delete_old_dps, minute=f"*/{datapoint_cleaning_period}")

    def delete_old_dps(self):
        """Deletes old data points from master collection."""
        self.log.debug("Deleting old records ...")

        for etype in self.model_spec:
            for attr_name, attr_conf in self.model_spec.attribs(etype).items():
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
