"""
Core module performing deletion of entities based on specified policy.
"""

import logging
from collections import defaultdict
from datetime import datetime, timedelta
from functools import partial

from pydantic import BaseModel

from dp3.common.attrspec import AttrType
from dp3.common.callback_registrar import CallbackRegistrar
from dp3.common.config import CronExpression, PlatformConfig
from dp3.common.datapoint import DataPointBase, DataPointObservationsBase, DataPointTimeseriesBase
from dp3.common.datatype import AnyEidT
from dp3.common.task import DataPointTask, parse_eids_from_cache
from dp3.database.database import EntityDatabase

DB_SEND_CHUNK = 1000


class GarbageCollectorConfig(BaseModel):
    """The configuration of the Collector module.

    Attributes:
        collection_rate: The rate at which the collector module runs. Default is 3:00 AM every day.
    """

    collection_rate: CronExpression = CronExpression(hour="3", minute="0", second="0")


class GarbageCollector:
    """Collector module manages the lifetimes of entities based on specified policy."""

    def __init__(
        self,
        db: EntityDatabase,
        platform_config: PlatformConfig,
        registrar: CallbackRegistrar,
    ):
        self.log = logging.getLogger("GarbageCollector")
        self.model_spec = platform_config.model_spec
        self.config = GarbageCollectorConfig.model_validate(
            platform_config.config.get("garbage_collector", {})
        )
        self.db = db

        self.worker_index = platform_config.process_index
        self.num_workers = platform_config.num_processes

        # Get link cache
        self.cache = self.db.get_module_cache("Link")
        self.inverse_relations = self._get_inverse_relations()

        for entity, entity_config in self.model_spec.entities.items():
            lifetime = entity_config.lifetime
            if lifetime.type == "immortal":
                pass
            elif lifetime.type == "ttl":
                registrar.register_entity_hook(
                    "on_entity_creation",
                    partial(self.extend_ttl_on_create, base_ttl=lifetime.on_create),
                    entity,
                )

                self._register_ttl_extensions(entity, registrar, lifetime.mirror_data)

                registrar.scheduler_register(
                    self.collect_ttl,
                    func_args=[entity],
                    **self.config.collection_rate.model_dump(),
                )
            elif lifetime.type == "weak":
                if self.inverse_relations[entity]:
                    registrar.scheduler_register(
                        self.collect_weak,
                        func_args=[entity],
                        **self.config.collection_rate.model_dump(),
                    )
                else:
                    raise ValueError(
                        f"Entity {entity} has weak lifetime "
                        f"but is not referenced by any other entities."
                    )

            else:
                raise ValueError(f"Unknown lifetime type: {lifetime.type}")

    def _get_inverse_relations(self) -> dict[str, set[tuple[str, str]]]:
        inverse_relations = defaultdict(set)
        for entity_attr, attr_spec in self.model_spec.relations.items():
            inverse_relations[attr_spec.relation_to].add(entity_attr)
        return inverse_relations

    def extend_ttl_on_create(
        self, eid: AnyEidT, task: DataPointTask, base_ttl: timedelta
    ) -> list[DataPointTask]:
        """Extends the TTL of the entity by the specified timedelta."""
        task = DataPointTask(
            etype=task.etype,
            eid=eid,
            ttl_tokens={"base": datetime.utcnow() + base_ttl},
        )
        return [task]

    def _register_ttl_extensions(
        self, entity: str, registrar: CallbackRegistrar, mirror_data: bool
    ):
        for attr, attr_spec in self.model_spec.entity_attributes[entity].items():
            if attr_spec.t == AttrType.TIMESERIES:
                if mirror_data and attr_spec.timeseries_params.max_age is not None:
                    if attr_spec.ttl:
                        ttl = max(attr_spec.ttl, attr_spec.timeseries_params.max_age)
                    else:
                        ttl = attr_spec.timeseries_params.max_age
                else:
                    ttl = attr_spec.ttl
                if not ttl:
                    continue

                registrar.register_attr_hook(
                    "on_new_ts_chunk",
                    partial(self.extend_timeseries_ttl, extend_by=ttl),
                    entity,
                    attr,
                )
            elif attr_spec.t == AttrType.OBSERVATIONS:
                if mirror_data and attr_spec.history_params.max_age is not None:
                    if attr_spec.ttl:
                        ttl = max(attr_spec.ttl, attr_spec.history_params.max_age)
                    else:
                        ttl = attr_spec.history_params.max_age
                else:
                    ttl = attr_spec.ttl
                if not ttl:
                    continue

                registrar.register_attr_hook(
                    "on_new_observation",
                    partial(self.extend_observations_ttl, extend_by=ttl),
                    entity,
                    attr,
                )
            elif attr_spec.t == AttrType.PLAIN:
                if not attr_spec.ttl:
                    continue

                registrar.register_attr_hook(
                    "on_new_plain",
                    partial(self.extend_plain_ttl, extend_by=attr_spec.ttl),
                    entity,
                    attr,
                )
            else:
                raise ValueError(f"Unknown attribute type: {attr_spec.t}")

    def collect_weak(self, etype: str):
        """Deletes weak entities when their last reference has expired."""
        self.log.debug("Starting removal of '%s' weak entities", etype)
        start = datetime.now()
        entities = 0
        deleted = 0

        self.db.save_metadata(
            start,
            {"entities": 0, "deleted": 0, "weak_collect_start": start, "entity": etype},
        )

        # Aggregate the cache entities by their "to" field, which contains the entity
        aggregated = self.cache.aggregate(
            [
                {"$match": {"to": {"$regex": f"^{etype}#"}}},
                {"$group": {"_id": "$to"}},
            ]
        )
        have_references = parse_eids_from_cache(self.model_spec, [doc["_id"] for doc in aggregated])
        entities += len(have_references)

        to_delete = []
        records_cursor = self.db.get_worker_master_records(
            self.worker_index,
            self.num_workers,
            etype,
            query_filter={"_id": {"$nin": have_references}},
        )
        try:
            for master_document in records_cursor:
                entities += 1
                deleted += 1
                to_delete.append(master_document["_id"])

                if len(to_delete) >= DB_SEND_CHUNK:
                    self.db.delete_eids(etype, to_delete)
                    to_delete.clear()

            if to_delete:
                self.db.delete_eids(etype, to_delete)
                to_delete.clear()

        finally:
            records_cursor.close()

        self.db.update_metadata(
            start,
            metadata={"weak_collect_end": datetime.now()},
            increase={"entities": entities, "deleted": deleted},
        )
        self.log.info(
            "Removal of '%s' weak entities done - %s tracked, %s processed & deleted",
            etype,
            entities,
            deleted,
        )

    def collect_ttl(self, etype: str):
        """Deletes entities after their TTL lifetime has expired."""
        self.log.debug("Starting removal of '%s' entities by TTL", etype)
        start = datetime.now()
        utc_now = datetime.utcnow()
        entities = 0
        deleted = 0

        to_delete = []
        expired_ttls = {}

        self.db.save_metadata(
            start, {"entities": 0, "deleted": 0, "ttl_collect_start": start, "entity": etype}
        )

        records_cursor = self.db.get_worker_master_records(
            self.worker_index, self.num_workers, etype, no_cursor_timeout=True
        )
        try:
            for master_document in records_cursor:
                entities += 1
                if "#ttl" not in master_document:
                    continue  # TTL not set, ignore for now

                if all(ttl < utc_now for ttl in master_document["#ttl"].values()):
                    deleted += 1
                    to_delete.append(master_document["_id"])
                else:
                    eid_expired_ttls = [
                        name for name, ttl in master_document["#ttl"].items() if ttl < start
                    ]
                    if eid_expired_ttls:
                        expired_ttls[master_document["_id"]] = eid_expired_ttls

                if len(to_delete) >= DB_SEND_CHUNK:
                    self.db.delete_eids(etype, to_delete)
                    to_delete.clear()
                if len(expired_ttls) >= DB_SEND_CHUNK:
                    self.db.remove_expired_ttls(etype, expired_ttls)
                    expired_ttls.clear()

            if to_delete:
                self.db.delete_eids(etype, to_delete)
                to_delete.clear()
            if expired_ttls:
                self.db.remove_expired_ttls(etype, expired_ttls)
                expired_ttls.clear()

        finally:
            records_cursor.close()

        self.db.update_metadata(
            start,
            metadata={"ttl_collect_end": datetime.now()},
            increase={"entities": entities, "deleted": deleted},
        )
        self.log.info(
            "Removal of '%s' entities by TTL done - %s processed, %s deleted",
            etype,
            entities,
            deleted,
        )

    def extend_plain_ttl(
        self, eid: AnyEidT, dp: DataPointBase, extend_by: timedelta
    ) -> list[DataPointTask]:
        """Extends the TTL of the entity by the specified timedelta."""
        now = datetime.utcnow()
        task = DataPointTask(
            etype=dp.etype,
            eid=eid,
            ttl_tokens={"data": now + extend_by},
        )
        return [task]

    def extend_observations_ttl(
        self, eid: AnyEidT, dp: DataPointObservationsBase, extend_by: timedelta
    ) -> list[DataPointTask]:
        """Extends the TTL of the entity by the specified timedelta."""
        task = DataPointTask(
            etype=dp.etype,
            eid=eid,
            ttl_tokens={"data": dp.t2 + extend_by},
        )
        return [task]

    def extend_timeseries_ttl(
        self, eid: AnyEidT, dp: DataPointTimeseriesBase, extend_by: timedelta
    ) -> list[DataPointTask]:
        """Extends the TTL of the entity by the specified timedelta."""
        task = DataPointTask(
            etype=dp.etype,
            eid=eid,
            ttl_tokens={"data": dp.t2 + extend_by},
        )
        return [task]
