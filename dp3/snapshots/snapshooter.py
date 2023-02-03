"""
Module managing creation of snapshots, enabling data correlation and saving snapshots to DB.

- Snapshots are created periodically (user configurable period)

- When a snapshot is created, several things need to happen:
    - all registered timeseries processing modules must be called
      - this should result in `observations` or `plain` datapoints, which will be saved to db
        and forwarded in processing
    - current value must be computed for all observations
      - load relevant section of observation's history and perform configured history analysis.
        Result = plain values
    - load plain attributes saved in master collection
    - A record of described plain data makes a `profile`
    - Profile is additionally extended by related entities
    - Callbacks for data correlation and fusion should happen here
    - Save the complete results into database as snapshots
"""
import logging

from pydantic import BaseModel

from dp3 import g
from dp3.common.config import HierarchicalDict, ModelSpec
from dp3.database.database import EntityDatabase


class SnapShooterConfig(BaseModel):
    creation_rate: int = 30


class SnapShooter:
    def __init__(
        self,
        db: EntityDatabase,
        model_spec: ModelSpec,
        worker_index: int,
        config: HierarchicalDict,
    ) -> None:
        self.log = logging.getLogger("SnapshotManager")

        self.db = db
        self.model_spec = model_spec
        self.worker_index = worker_index
        self.config = SnapShooterConfig.parse_obj(config)

        if worker_index != 0:
            self.log.debug(
                "Snapshot creation will be disabled in this worker to avoid race conditions."
            )
            return

        # Schedule datapoints cleaning
        snapshot_period = self.config.creation_rate
        g.scheduler.register(self.make_snapshots, minute=f"*/{snapshot_period}")

    def make_snapshots(self):
        """Create snapshots for all entities currently active in database."""
