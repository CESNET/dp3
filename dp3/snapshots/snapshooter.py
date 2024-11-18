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
from typing import Any, Callable, Optional, Union

import pymongo.errors
from event_count_logger import DummyEventGroup
from pydantic import BaseModel

from dp3.common.attrspec import (
    AttrSpecObservations,
    AttrSpecType,
    AttrType,
    ObservationsHistoryParams,
)
from dp3.common.config import CronExpression, PlatformConfig, entity_type_context
from dp3.common.scheduler import Scheduler
from dp3.common.task import (
    DataPointTask,
    Snapshot,
    SnapshotMessageType,
    parse_eid_tuples_from_cache,
    parse_eids_from_cache,
    task_context,
)
from dp3.common.types import EventGroupType
from dp3.common.utils import get_func_name
from dp3.database.database import EntityDatabase
from dp3.snapshots.snapshot_hooks import (
    SnapshotCorrelationHookContainer,
    SnapshotTimeseriesHookContainer,
)
from dp3.task_processing.task_queue import TaskQueueReader, TaskQueueWriter

DB_SEND_CHUNK = 100
RETRY_COUNT = 3


class SnapShooterConfig(BaseModel):
    creation_rate: CronExpression = CronExpression(minute="*/30")
    keep_empty: bool = True


class SnapShooter:
    """Class responsible for creating entity snapshots."""

    def __init__(
        self,
        db: EntityDatabase,
        task_queue_writer: TaskQueueWriter,
        platform_config: PlatformConfig,
        scheduler: Scheduler,
        elog: Optional[EventGroupType] = None,
    ) -> None:
        self.log = logging.getLogger("SnapShooter")

        self.db = db
        self.task_queue_writer = task_queue_writer
        self.model_spec = platform_config.model_spec
        self.entity_relation_attrs = defaultdict(dict)
        for (entity, attr), _ in self.model_spec.relations.items():
            self.entity_relation_attrs[entity][attr] = True
        for entity in self.model_spec.entities:
            self.entity_relation_attrs[entity]["_id"] = True

        self.worker_index = platform_config.process_index
        self.worker_cnt = platform_config.num_processes
        self.config = SnapShooterConfig.model_validate(platform_config.config.get("snapshots"))

        self.elog = elog or DummyEventGroup()

        self._timeseries_hooks = SnapshotTimeseriesHookContainer(self.log, self.model_spec, elog)
        self._correlation_hooks = SnapshotCorrelationHookContainer(self.log, self.model_spec, elog)
        self._init_hooks: list[Callable[[], list[DataPointTask]]] = []
        self._finalize_hooks: list[Callable[[], list[DataPointTask]]] = []

        queue = f"{platform_config.app_name}-worker-{platform_config.process_index}-snapshots"
        self.snapshot_queue_reader = TaskQueueReader(
            callback=self.process_snapshot_task,
            parse_task=Snapshot.get_validator(self.model_spec),
            app_name=platform_config.app_name,
            worker_index=platform_config.process_index,
            rabbit_config=platform_config.config.get("processing_core.msg_broker", {}),
            queue=queue,
            priority_queue=False,
            parent_logger=self.log,
        )
        scheduler.register(self.snapshot_queue_reader.watchdog, second="10,40")

        self.snapshot_entities = [
            entity for entity, spec in self.model_spec.entities.items() if spec.snapshot
        ]
        self.log.info("Snapshots will be created for entities: %s", self.snapshot_entities)
        self.link_mirrors = defaultdict(dict)
        for (entity, attr), spec in self.model_spec.relations.items():
            if spec.is_relation and spec.is_mirrored:
                self.link_mirrors[spec.relation_to][spec.mirror_as] = (entity, attr)
        self.log.info("Link mirrors: %s", self.link_mirrors)

        # Get link cache
        self.cache = self.db.get_module_cache("Link")

        if platform_config.process_index != 0:
            self.log.debug(
                "Snapshot task creation will be disabled in this worker to avoid race conditions."
            )
            self.snapshot_queue_writer = None
            return

        self.snapshot_queue_writer = TaskQueueWriter(
            platform_config.app_name,
            platform_config.num_processes,
            platform_config.config.get("processing_core.msg_broker"),
            f"{platform_config.app_name}-main-snapshot-exchange",
            parent_logger=self.log,
        )

        # Schedule snapshot period
        snapshot_cron = self.config.creation_rate.model_dump(exclude_none=True)
        scheduler.register(self.make_snapshots, **snapshot_cron)

    def start(self):
        """Connect to RabbitMQ and start consuming from TaskQueue."""
        self.log.info("Connecting to RabbitMQ")
        self.snapshot_queue_reader.connect()
        self.snapshot_queue_reader.check()  # check presence of needed queues
        if self.snapshot_queue_writer is not None:
            self.snapshot_queue_writer.connect()
            self.snapshot_queue_writer.check()  # check presence of needed exchanges

        self.snapshot_queue_reader.start()
        self.log.info("Following used links detected: %s", self._correlation_hooks.used_links)
        self.log.info("SnapShooter started.")

    def stop(self):
        """Stop consuming from TaskQueue, disconnect from RabbitMQ."""
        self.snapshot_queue_reader.stop()

        if self.snapshot_queue_writer is not None:
            self.snapshot_queue_writer.disconnect()
        self.snapshot_queue_reader.disconnect()

    def register_timeseries_hook(
        self,
        hook: Callable[[str, str, list[dict]], list[DataPointTask]],
        entity_type: str,
        attr_type: str,
    ):
        """
        Registers passed timeseries hook to be called during snapshot creation.

        Binds hook to specified `entity_type` and `attr_type` (though same hook can be bound
        multiple times).

        Args:
            hook: `hook` callable should expect entity_type, attr_type and attribute
                history as arguments and return a list of `DataPointTask` objects.
            entity_type: specifies entity type
            attr_type: specifies attribute type

        Raises:
            ValueError: If entity_type and attr_type do not specify a valid timeseries attribute,
                a ValueError is raised.
        """
        self._timeseries_hooks.register(hook, entity_type, attr_type)

    def register_correlation_hook(
        self,
        hook: Callable[[str, dict], Union[None, list[DataPointTask]]],
        entity_type: str,
        depends_on: list[list[str]],
        may_change: list[list[str]],
    ):
        """
        Registers passed hook to be called during snapshot creation.

        Binds hook to specified entity_type (though same hook can be bound multiple times).

        `entity_type` and attribute specifications are validated, `ValueError` is raised on failure.

        Args:
            hook: `hook` callable should expect entity type as str
                and its current values, including linked entities, as dict
                Can optionally return a list of DataPointTask objects to perform.
            entity_type: specifies entity type
            depends_on: each item should specify an attribute that is depended on
                in the form of a path from the specified entity_type to individual attributes
                (even on linked entities).
            may_change: each item should specify an attribute that `hook` may change.
                specification format is identical to `depends_on`.

        Raises:
            ValueError: On failure of specification validation.
        """
        self._correlation_hooks.register(hook, entity_type, depends_on, may_change)

    def register_run_init_hook(self, hook: Callable[[], list[DataPointTask]]):
        """
        Registers passed hook to be called before a run of  snapshot creation begins.

        Args:
            hook: `hook` callable should expect no arguments and
                return a list of DataPointTask objects to perform.
        """
        self._init_hooks.append(hook)

    def register_run_finalize_hook(self, hook: Callable[[], list[DataPointTask]]):
        """
        Registers passed hook to be called after a run of  snapshot creation ends.

        Args:
            hook: `hook` callable should expect no arguments and
                return a list of DataPointTask objects to perform.
        """
        self._finalize_hooks.append(hook)

    def make_snapshots(self):
        """Creates snapshots for all entities currently active in database."""
        time = datetime.utcnow()
        self.db.save_metadata(
            time,
            {
                "task_creation_start": time,
                "entities": 0,
                "components": 0,
                "workers_finished": 0,
                "linked_finished": False,
            },
        )
        # Broadcast run start
        with entity_type_context(self.model_spec):
            self.snapshot_queue_writer.broadcast_task(
                task=Snapshot(type=SnapshotMessageType.run_start, time=time)
            )

        # distribute list of possibly linked entities to all workers
        cached = self.get_cached_link_entity_ids()
        self.log.debug("Broadcasting %s cached linked entities", len(cached))
        with entity_type_context(self.model_spec):
            self.snapshot_queue_writer.broadcast_task(
                task=Snapshot(entities=cached, time=time, type=SnapshotMessageType.linked_entities)
            )

        # Load links only for a reduced set of entities
        self.log.debug("Loading linked entities.")

        times = {}
        counts = {"entities": 0, "components": 0}
        try:
            linked_entities = self.get_linked_entities(time, cached)
            times["components_loaded"] = datetime.utcnow()

            for i, linked_entities_component in enumerate(linked_entities):
                counts["entities"] += len(linked_entities_component)
                counts["components"] += 1

                with entity_type_context(self.model_spec):
                    self.snapshot_queue_writer.put_task(
                        task=Snapshot(
                            entities=linked_entities_component,
                            time=time,
                            type=SnapshotMessageType.task,
                            final=(i + 1 == len(linked_entities)),
                        )
                    )

            if len(linked_entities) == 0:
                self.db.update_metadata(time, metadata={"linked_finished": True})
        except pymongo.errors.CursorNotFound as err:
            self.log.exception(err)
        finally:
            times["task_creation_end"] = datetime.utcnow()
            self.db.update_metadata(
                time,
                metadata=times,
                increase=counts,
            )

        # Broadcast run end
        with entity_type_context(self.model_spec):
            self.snapshot_queue_writer.broadcast_task(
                task=Snapshot(type=SnapshotMessageType.run_end, time=time)
            )

    def get_cached_link_entity_ids(self):
        used = [f"{etype}#{attr}" for etype, attr in self._correlation_hooks.used_links]
        result = self.cache.aggregate(
            [{"$match": {"using_attr": {"$in": used}}}, {"$group": {"_id": "$from"}}]
        )
        links_from = parse_eid_tuples_from_cache(self.model_spec, [doc["_id"] for doc in result])
        result = self.cache.aggregate(
            [{"$match": {"using_attr": {"$in": used}}}, {"$group": {"_id": "$to"}}]
        )
        links_to = parse_eid_tuples_from_cache(self.model_spec, [doc["_id"] for doc in result])
        return list(set(links_from) | set(links_to))

    def get_linked_entities(self, time: datetime, cached_linked_entities: list[tuple[str, str]]):
        """Get weakly connected components from entity graph."""
        visited_entities = set()
        entity_to_component = {}
        linked_components = []
        for etype, eid in cached_linked_entities:
            typed_eid = self.model_spec.parse_eid(etype, eid)
            master_record = self.db.get_master_record(
                etype, typed_eid, projection=self.entity_relation_attrs[etype]
            ) or {"_id": typed_eid}

            if not self.config.keep_empty and len(master_record) == 1:
                continue

            if (etype, master_record["_id"]) not in visited_entities:
                # Get entities linked by current entity
                current_values = self.get_values_at_time(etype, master_record, time)
                linked_entities = self.load_linked_entity_ids(etype, current_values, time)

                # Set linked as visited
                visited_entities.update(linked_entities)

                # Update component, take all connected components into account
                have_component = linked_entities & set(entity_to_component.keys())
                if have_component:
                    for entity in have_component:
                        component = entity_to_component[entity]
                        linked_entities.update(component)
                entity_to_component.update({entity: linked_entities for entity in linked_entities})

        # Make a list of unique components
        visited_entities.clear()
        for entity, component in entity_to_component.items():
            if entity in visited_entities:
                continue
            visited_entities.update(component)
            linked_components.append(component)

        return linked_components

    def process_snapshot_task(self, msg_id, task: Snapshot):
        """
        Acknowledges the received message and makes a snapshot according to the `task`.

        This function should not be called directly, but set as callback for TaskQueueReader.
        """
        if not self.snapshot_queue_reader.ack(msg_id):
            self.log.warning("Acking message %s failed, will not process.", msg_id)
            return
        if task.type == SnapshotMessageType.task:
            self.make_snapshot(task)
        elif task.type == SnapshotMessageType.linked_entities:
            self.make_snapshots_by_hash(task)
        elif task.type == SnapshotMessageType.run_start:
            self.log.debug("Run start, running init hooks")
            self._run_hooks(self._init_hooks)
        elif task.type == SnapshotMessageType.run_end:
            self.log.debug("Run end, running finalize hooks")
            self._run_hooks(self._finalize_hooks)
        else:
            raise ValueError("Unknown SnapshotMessageType.")

    def _run_hooks(self, hooks: list[Callable[[], list[DataPointTask]]]):
        tasks = []
        with task_context(self.model_spec):
            for hook in hooks:
                self.log.debug("Running hook: '%s'", get_func_name(hook))
                try:
                    new_tasks = hook()
                    tasks.extend(new_tasks)
                except Exception as e:
                    self.elog.log("module_error")
                    self.log.error(f"Error during running hook {hook}: {e}")

        for task in tasks:
            self.task_queue_writer.put_task(task)

    def make_snapshots_by_hash(self, task: Snapshot):
        """
        Make snapshots for all entities with routing key belonging to this worker.
        """
        self.log.debug("Creating snapshots for worker portion by hash.")
        have_links = set(task.entities)
        entity_cnt = 0
        for etype in self.snapshot_entities:
            records_cursor = self.db.get_worker_master_records(
                self.worker_index, self.worker_cnt, etype, no_cursor_timeout=True
            )
            for attempt in range(RETRY_COUNT):
                try:
                    entity_cnt += self.make_linkless_snapshots(
                        etype, records_cursor, task.time, have_links
                    )
                except Exception as err:
                    self.log.exception("Uncaught exception while creating snapshots: %s", err)
                    if attempt < RETRY_COUNT - 1:
                        self.log.info("Retrying snapshot creation for '%s' due to errors.", etype)
                    continue
                finally:
                    records_cursor.close()
                break
            else:
                self.log.error(
                    "Failed to create snapshots for '%s' after %s attempts.", etype, attempt + 1
                )
        self.db.update_metadata(
            task.time,
            metadata={},
            increase={"entities": entity_cnt, "components": entity_cnt, "workers_finished": 1},
            worker_id=0,
        )
        self.log.debug("Worker snapshot creation done.")

    def make_linkless_snapshots(
        self, etype: str, records_cursor, time: datetime, have_links: set
    ) -> int:
        entity_cnt = 0
        snapshots = []
        for master_record in records_cursor:
            if (etype, master_record["_id"]) in have_links:
                continue
            if not self.config.keep_empty and len(master_record) == 1:
                continue
            entity_cnt += 1
            snapshots.append(self.make_linkless_snapshot(etype, master_record, time))
            if len(snapshots) >= DB_SEND_CHUNK:
                self.db.snapshots.save_many(etype, snapshots, time)
                snapshots.clear()

        if snapshots:
            self.db.snapshots.save_many(etype, snapshots, time)
            snapshots.clear()
        return entity_cnt

    def make_linkless_snapshot(self, entity_type: str, master_record: dict, time: datetime):
        """
        Make a snapshot for given entity `master_record` and `time`.

        Runs timeseries and correlation hooks.
        The resulting snapshot is saved into DB.
        """
        self.run_timeseries_processing(entity_type, master_record)
        values = self.get_values_at_time(entity_type, master_record, time)
        self.add_mirrored_links(entity_type, values)
        entity_values = {(entity_type, master_record["_id"]): values}

        tasks = self._correlation_hooks.run(entity_values)
        for task in tasks:
            self.task_queue_writer.put_task(task)

        assert len(entity_values) == 1, "Expected a single entity."
        for record in entity_values.values():
            return record

    def add_mirrored_links(self, entity_type: str, values: dict):
        """
        This function adds mirrored links to the dict with current values of an entity.

        The links are added in the same format as normal links, i.e. as a list of dicts.
        """
        if entity_type in self.link_mirrors:
            for mirror_name, (etype, attr) in self.link_mirrors[entity_type].items():
                link_sources = self.cache.aggregate(
                    [
                        {
                            "$match": {
                                "to": f"{entity_type}#{values['eid']}",
                                "using_attr": f"{etype}#{attr}",
                            }
                        },
                        {"$project": {"from": 1}},
                    ]
                )
                parsed = parse_eids_from_cache(
                    self.model_spec, [doc["from"] for doc in link_sources]
                )
                values[mirror_name] = [{"eid": eid} for eid in parsed]

    def make_snapshot(self, task: Snapshot):
        """
        Make a snapshot for entities and time specified by `task`.

        Runs timeseries and correlation hooks.
        The resulting snapshots are saved into DB.
        """
        entity_values = {}
        for entity_type, entity_id in task.entities:
            record = self.db.get_master_record(entity_type, entity_id) or {"_id": entity_id}
            if not self.config.keep_empty and len(record) == 1:
                continue

            self.run_timeseries_processing(entity_type, record)
            values = self.get_values_at_time(entity_type, record, task.time)
            self.add_mirrored_links(entity_type, values)
            entity_values[entity_type, entity_id] = values

        self.link_loaded_entities(entity_values)
        created_tasks = self._correlation_hooks.run(entity_values)
        for created_task in created_tasks:
            self.task_queue_writer.put_task(created_task)

        # unlink entities again
        for (rtype, _rid), record in entity_values.items():
            for attr, value in record.items():
                if (rtype, attr) not in self.model_spec.relations:
                    continue
                spec = self.model_spec.relations[rtype, attr]
                if spec.t == AttrType.OBSERVATIONS and spec.multi_value:
                    for val in value:
                        self._remove_record_from_value(spec, val)
                else:
                    self._remove_record_from_value(spec, value)

        for (rtype, _rid), record in entity_values.items():
            if len(record) == 1 and not self.config.keep_empty:
                continue
            self.db.snapshots.save_one(rtype, record, task.time)

        if task.final:
            self.db.update_metadata(task.time, metadata={"linked_finished": True}, worker_id=0)

    @staticmethod
    def _remove_record_from_value(spec: AttrSpecType, value: Union[dict, list[dict]]):
        if spec.is_iterable:
            for link_dict in value:
                if "record" in link_dict:
                    del link_dict["record"]
        elif "record" in value:
            del value["record"]

    def run_timeseries_processing(self, entity_type, master_record):
        """
        - all registered timeseries processing modules must be called
          - this should result in `observations` or `plain` datapoints, which will be saved to db
            and forwarded in processing
        """
        tasks = []
        for attr, attr_spec in self.model_spec.entity_attributes[entity_type].items():
            if attr_spec.t == AttrType.TIMESERIES and attr in master_record:
                new_tasks = self._timeseries_hooks.run(entity_type, attr, master_record[attr])
                tasks.extend(new_tasks)

        self.extend_master_record(entity_type, master_record, tasks)
        for task in tasks:
            self.task_queue_writer.put_task(task)

    @staticmethod
    def extend_master_record(etype, master_record, new_tasks: list[DataPointTask]):
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

    def get_values_at_time(self, etype: str, master_record: dict, time: datetime) -> dict:
        current_values = {"eid": master_record["_id"]}
        for attr, attr_spec in self.model_spec.entity_attributes[etype].items():
            if (
                attr not in master_record
                or attr_spec.t not in AttrType.PLAIN | AttrType.OBSERVATIONS
            ):
                continue
            if attr_spec.t == AttrType.PLAIN:
                current_values[attr] = master_record[attr]["v"]
                continue

            if attr_spec.multi_value:
                val, conf = self.get_multi_value_at_time(attr_spec, master_record[attr], time)
            else:
                val, conf = self.get_value_at_time(attr_spec, master_record[attr], time)

            if conf:  # conf != 0.0 or len(conf) > 0
                current_values[attr] = val
                current_values[f"{attr}#c"] = conf
        return current_values

    def load_linked_entity_ids(self, entity_type: str, current_values: dict, time: datetime):
        """
        Loads the subgraph of entities linked to the current entity,
        returns a list of their types and ids.
        """
        loaded_entity_ids = {(entity_type, current_values["eid"])}
        linked_entity_ids_to_process = (
            self.get_linked_entity_ids(entity_type, current_values) - loaded_entity_ids
        )

        while linked_entity_ids_to_process:
            entity_identifiers = linked_entity_ids_to_process.pop()
            linked_etype, linked_eid = entity_identifiers
            relevant_attributes = self.entity_relation_attrs[linked_etype]
            record = self.db.get_master_record(
                linked_etype, linked_eid, projection=relevant_attributes
            ) or {"_id": linked_eid}
            linked_values = self.get_values_at_time(linked_etype, record, time)

            linked_entity_ids_to_process.update(
                self.get_linked_entity_ids(entity_type, linked_values) - set(loaded_entity_ids)
            )
            loaded_entity_ids.add((linked_etype, linked_eid))

        return loaded_entity_ids

    def get_linked_entity_ids(self, entity_type: str, current_values: dict) -> set[tuple[str, str]]:
        """
        Returns a set of tuples (entity_type, entity_id) identifying entities linked by
        `current_values`.
        """
        related_entity_ids = set()
        for attr, val in current_values.items():
            if (entity_type, attr) not in self._correlation_hooks.used_links:
                continue
            attr_spec = self.model_spec.relations[entity_type, attr]
            if attr_spec.t == AttrType.OBSERVATIONS and attr_spec.multi_value:
                for v in val:
                    related_entity_ids.update(self._get_link_entity_ids(attr_spec, v))
            else:
                related_entity_ids.update(self._get_link_entity_ids(attr_spec, val))
        return related_entity_ids

    @staticmethod
    def _get_link_entity_ids(
        spec: AttrSpecType, link_value: Union[list[dict], dict]
    ) -> set[tuple[str, str]]:
        if spec.is_iterable:
            return {(spec.relation_to, v["eid"]) for v in link_value}
        return {(spec.relation_to, link_value["eid"])}

    def link_loaded_entities(self, loaded_entities: dict):
        for (entity_type, _entity_id), entity in loaded_entities.items():
            del_keys = []
            for attr, val in entity.items():
                if (entity_type, attr) not in self._correlation_hooks.used_links:
                    continue
                attr_spec = self.model_spec.relations[entity_type, attr]
                if attr_spec.t == AttrType.OBSERVATIONS and attr_spec.multi_value:
                    entity[attr] = []
                    val_conf = entity[f"{attr}#c"]
                    pruned_conf = []
                    for v, conf in zip(val, val_conf):
                        if self._keep_link(loaded_entities, attr_spec, v):
                            self._link_record(loaded_entities, attr_spec, v)
                            entity[attr].append(v)
                            pruned_conf.append(conf)
                    entity[f"{attr}#c"] = pruned_conf
                elif self._keep_link(loaded_entities, attr_spec, val):
                    self._link_record(loaded_entities, attr_spec, val)
                    entity[attr] = val
                else:  # The linked entity does not exist, and we do not want to keep empty
                    del_keys.append(attr)
                    if f"{attr}#c" in entity:
                        del_keys.append(f"{attr}#c")

            for key in del_keys:
                del entity[key]

    def _keep_link(
        self, loaded_entities: dict, attr_spec: AttrSpecType, val: Union[dict, list[dict]]
    ) -> bool:
        if self.config.keep_empty:
            return True
        if attr_spec.is_iterable:
            return any(
                loaded_entities.get((attr_spec.relation_to, v["eid"])) is not None for v in val
            )
        return loaded_entities.get((attr_spec.relation_to, val["eid"])) is not None

    @staticmethod
    def _link_record(loaded_entities: dict, attr_spec: AttrSpecType, val: Union[dict, list[dict]]):
        if attr_spec.is_iterable:
            for link_dict in val:
                link_dict["record"] = loaded_entities.get(
                    (attr_spec.relation_to, link_dict["eid"]), {"eid": link_dict["eid"]}
                )
        else:
            val["record"] = loaded_entities.get(
                (attr_spec.relation_to, val["eid"]), {"eid": val["eid"]}
            )

    def get_value_at_time(
        self, attr_spec: AttrSpecObservations, attr_history, time: datetime
    ) -> tuple[Any, float]:
        """Get current value of an attribute from its history. Assumes `multi_value = False`."""
        return max(
            (
                (point["v"], self.extrapolate_confidence(point, time, attr_spec.history_params))
                for point in attr_history
            ),
            key=lambda val_conf: val_conf[1],
            default=(None, 0.0),
        )

    def get_multi_value_at_time(
        self, attr_spec: AttrSpecObservations, attr_history, time: datetime
    ) -> tuple[list, list[float]]:
        """Get current value of a multi_value attribute from its history."""
        if attr_spec.data_type.hashable:
            values_with_confidence = defaultdict(float)
            for point in attr_history:
                value = point["v"]
                confidence = self.extrapolate_confidence(point, time, attr_spec.history_params)
                if confidence > 0.0 and values_with_confidence[value] < confidence:
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
                    confidence_list[i] = max(confidence_list[i], confidence)
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
