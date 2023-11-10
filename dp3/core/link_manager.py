"""
Core module managing links between entities.
"""
import logging
from datetime import datetime, timedelta
from functools import partial

from pymongo import DeleteMany

from dp3.common.attrspec import AttrType
from dp3.common.callback_registrar import CallbackRegistrar
from dp3.common.config import PlatformConfig
from dp3.common.datapoint import DataPointBase, DataPointObservationsBase
from dp3.database.database import EntityDatabase


class LinkManager:
    """Manages the shared Link cache and updates links after entity deletion."""

    def __init__(
        self,
        db: EntityDatabase,
        platform_config: PlatformConfig,
        registrar: CallbackRegistrar,
    ):
        self.log = logging.getLogger("LinkManager")
        self.model_spec = platform_config.model_spec
        self.db = db

        self.cache = self.db.get_module_cache("Link")
        self._setup_cache_indexes()
        self.db.register_on_entity_delete(
            self.remove_link_cache_of_deleted, self.remove_link_cache_of_many_deleted
        )
        self.max_date = datetime.max.replace(tzinfo=None)
        for (entity, attr), spec in self.model_spec.relations.items():
            if spec.t == AttrType.PLAIN:
                registrar.register_attr_hook(
                    "on_new_plain",
                    partial(self.add_plain_to_link_cache, spec.relation_to),
                    entity,
                    attr,
                )
            elif spec.t == AttrType.OBSERVATIONS:
                registrar.register_attr_hook(
                    "on_new_observation",
                    partial(
                        self.add_observation_to_link_cache,
                        spec.relation_to,
                        spec.history_params.post_validity,
                    ),
                    entity,
                    attr,
                )

    def _setup_cache_indexes(self):
        """Sets up indexes for the cache collection.

        In the collection, these fields are covered by an index:

        * The `to` and `from` fields are used when loading linked entities,
          as well as when removing links of deleted entities
        * The `using_attr` field is used to filter which link attributes are used
        * The `ttl` field serves as a MongoDB expiring collection index
        """
        self.cache.create_index("to", background=True)
        self.cache.create_index("from", background=True)
        self.cache.create_index("using_attr", background=True)
        self.cache.create_index("ttl", expireAfterSeconds=0, background=True)

    def add_plain_to_link_cache(self, etype_to: str, eid: str, dp: DataPointBase):
        self.cache.update_one(
            {
                "to": f"{etype_to}#{dp.v.eid}",
                "from": f"{dp.etype}#{eid}",
                "using_attr": f"{dp.etype}#{dp.attr}",
            },
            {"$max": {"ttl": self.max_date}},
            upsert=True,
        )

    def add_observation_to_link_cache(
        self, etype_to: str, post_validity: timedelta, eid: str, dp: DataPointObservationsBase
    ):
        self.cache.update_one(
            {
                "to": f"{etype_to}#{dp.v.eid}",
                "from": f"{dp.etype}#{eid}",
                "using_attr": f"{dp.etype}#{dp.attr}",
            },
            {"$max": {"ttl": dp.t2 + post_validity}},
            upsert=True,
        )

    def remove_link_cache_of_deleted(self, etype: str, eid: str):
        self.cache.bulk_write(
            [DeleteMany({"from": f"{etype}#{eid}"}), DeleteMany({"to": f"{etype}#{eid}"})]
        )

    def remove_link_cache_of_many_deleted(self, etype: str, eids: list[str]):
        self.cache.bulk_write(
            [DeleteMany({"from": f"{etype}#{eid}"}) for eid in eids]
            + [DeleteMany({"to": f"{etype}#{eid}"}) for eid in eids]
        )
