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
from collections import defaultdict
from datetime import datetime
from typing import Any, Callable

from pydantic import BaseModel

from dp3 import g
from dp3.common.attrspec import (
    AttrSpecObservations,
    AttrType,
    ObservationsHistoryParams,
)
from dp3.common.config import HierarchicalDict, ModelSpec
from dp3.common.task import Task
from dp3.database.database import EntityDatabase
from dp3.snapshots.snapshot_hooks import (
    SnapshotCorrelationHookContainer,
    SnapshotTimeseriesHookContainer,
)
from dp3.task_processing.task_queue import TaskQueueWriter


class SnapShooterConfig(BaseModel):
    creation_rate: int = 30


class SnapShooter:
    def __init__(
        self,
        db: EntityDatabase,
        task_queue_writer: TaskQueueWriter,
        model_spec: ModelSpec,
        worker_index: int,
        config: HierarchicalDict,
    ) -> None:
        self.log = logging.getLogger("SnapshotManager")

        self.db = db
        self.task_queue_writer = task_queue_writer
        self.model_spec = model_spec
        self.worker_index = worker_index
        self.config = SnapShooterConfig.parse_obj(config)

        if worker_index != 0:
            self.log.debug(
                "Snapshot creation will be disabled in this worker to avoid race conditions."
            )
            return

        # Schedule snapshot period
        snapshot_period = self.config.creation_rate
        g.scheduler.register(self.make_snapshots, minute=f"*/{snapshot_period}")

        self._timeseries_hooks = SnapshotTimeseriesHookContainer(self.log, model_spec)
        self._correlation_hooks = SnapshotCorrelationHookContainer(self.log, model_spec)

    def register_timeseries_hook(self, hook: Callable, entity_type: str, attr_type: str):
        """Registers timeseries hook to specified attribute."""
        self._timeseries_hooks.register(hook, entity_type, attr_type)

    def register_correlation_hook(
        self,
        hook: Callable,
        entity_type: str,
        depends_on: list[list[str]],
        may_change: list[list[str]],
    ):
        self._correlation_hooks.register(hook, entity_type, depends_on, may_change)

    def make_snapshots(self):
        """Create snapshots for all entities currently active in database."""
        for etype in self.model_spec.entities.keys():
            for master_record in self.db.get_master_records(etype):
                self.make_snapshot(etype, master_record)

    def make_snapshot(self, etype, master_record):
        # - all registered timeseries processing modules must be called
        #   - this should result in `observations` or `plain` datapoints, which will be saved to db
        #     and forwarded in processing
        tasks = []
        for attr, attr_spec in self.model_spec.entity_attributes[etype].items():
            if attr_spec.t == AttrType.TIMESERIES and attr in master_record:
                new_tasks = self._timeseries_hooks.run(etype, attr, master_record[attr])
                tasks.extend(new_tasks)

        self.extend_master_record(etype, master_record, tasks)
        for task in tasks:
            self.task_queue_writer.put_task(task)

        # compute current values for all observations
        current_values = self.get_current_values(etype, master_record)

        # TODO Callbacks for data correlation and fusion should happen here

        # - Save the complete results into database as snapshots
        self.db.save_snapshot(etype, current_values)

    @staticmethod
    def extend_master_record(etype, master_record, new_tasks: list[Task]):
        """Update existing master record with datapoints from new tasks"""
        for task in new_tasks:
            for datapoint in task.data_points:
                if datapoint.etype != etype:
                    continue
                dp_dict = datapoint.dict(include={"v", "t1", "t2", "c"})
                if datapoint.attr in master_record:
                    master_record[datapoint.attr].append()
                else:
                    master_record[datapoint.attr] = [dp_dict]

    def get_current_values(self, etype, master_record):
        current_values = {"eid": master_record["_id"]}
        for attr, attr_spec in self.model_spec.entity_attributes[etype].items():
            if attr_spec.t != AttrType.OBSERVATIONS or attr not in master_record:
                continue

            if attr_spec.multi_value:
                val, conf = self.get_current_multivalue(attr_spec, master_record[attr])

                if attr_spec.is_relation:
                    val = [
                        self.get_current_values_relation(attr_spec.relation_to, eid) for eid in val
                    ]
            else:
                val, conf = self.get_current_value(attr_spec, master_record[attr])

                if attr_spec.is_relation:
                    val = self.get_current_values_relation(attr_spec.relation_to, val)

            if conf:  # conf != 0.0 or len(conf) > 0
                current_values[attr] = val
                current_values[f"{attr}_c"] = conf
        return current_values

    def get_current_values_relation(self, linked_etype, linked_eid):
        linked_record = self.db.get_master_record(linked_etype, linked_eid)
        return self.get_current_values(linked_etype, linked_record)

    def get_current_value(self, attr_spec: AttrSpecObservations, attr_history) -> tuple[Any, float]:
        """Get current value of an attribute from its history. Assumes multi_value = False."""
        time = datetime.now()
        return max(
            (
                (point["v"], self.extrapolate_confidence(point, time, attr_spec.history_params))
                for point in attr_history
            ),
            key=lambda val_conf: val_conf[1],
            default=(None, 0.0),
        )

    def get_current_multivalue(
        self, attr_spec: AttrSpecObservations, attr_history
    ) -> tuple[list, list[float]]:
        """Get current value of a multi_value attribute from its history."""
        time = datetime.now()
        if attr_spec.data_type.hashable:
            values_with_confidence = defaultdict(float)
            for point in attr_history:
                value = point["v"]
                confidence = self.extrapolate_confidence(point, time, attr_spec.history_params)
                if values_with_confidence[value] < confidence:
                    values_with_confidence[value] = confidence
            return list(values_with_confidence.keys()), list(values_with_confidence.values())
        else:
            values = []
            confidence_list = []
            for point in attr_history:
                value = point["v"]
                confidence = self.extrapolate_confidence(point, time, attr_spec.history_params)
                if value in values:
                    i = values.index(value)
                    if confidence_list[i] < confidence:
                        confidence_list[i] = confidence
                elif confidence > 0.0:
                    values.append(value)
                    confidence_list.append(confidence)
            return values, confidence_list

    @staticmethod
    def extrapolate_confidence(
        datapoint: dict, time: datetime, history_params: ObservationsHistoryParams
    ) -> float:
        """Get the confidence value at given time."""
        t1 = datapoint["t1"]
        t2 = datapoint["t2"]
        base_confidence = datapoint["c"]

        if time < t1:
            if time <= t1 - history_params.pre_validity:
                return 0.0
            return base_confidence * (1 - (t1 - time) / history_params.pre_validity)
        if time <= t2:
            return base_confidence  # completely inside the (strict) interval
        if time >= t2 + history_params.post_validity:
            return 0.0
        return base_confidence * (1 - (time - t2) / history_params.post_validity)
