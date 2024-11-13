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
from dp3.common.datatype import AnyEidT
from dp3.common.task import parse_eids_from_cache
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
                if spec.is_iterable:
                    func = self.add_iterable_plain_to_link_cache
                else:
                    func = self.add_plain_to_link_cache
                registrar.register_attr_hook(
                    "on_new_plain", partial(func, spec.relation_to), entity, attr
                )
            elif spec.t == AttrType.OBSERVATIONS:
                if spec.is_iterable:
                    func = self.add_iterable_observation_to_link_cache
                else:
                    func = self.add_observation_to_link_cache
                registrar.register_attr_hook(
                    "on_new_observation",
                    partial(func, spec.relation_to, spec.history_params.post_validity),
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

    def add_plain_to_link_cache(self, etype_to: str, eid: AnyEidT, dp: DataPointBase):
        self.cache.update_one(
            {
                "to": f"{etype_to}#{dp.v.eid}",
                "from": f"{dp.etype}#{eid}",
                "using_attr": f"{dp.etype}#{dp.attr}",
            },
            {"$max": {"ttl": self.max_date}},
            upsert=True,
        )

    def add_iterable_plain_to_link_cache(self, etype_to: str, eid: AnyEidT, dp: DataPointBase):
        linked_eids = [v.eid for v in dp.v]
        self.cache.update_many(
            {
                "to": {"$in": [f"{etype_to}#{eid_}" for eid_ in linked_eids]},
                "from": f"{dp.etype}#{eid}",
                "using_attr": f"{dp.etype}#{dp.attr}",
            },
            {"$max": {"ttl": self.max_date}},
            upsert=True,
        )

    def add_observation_to_link_cache(
        self, etype_to: str, post_validity: timedelta, eid: AnyEidT, dp: DataPointObservationsBase
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

    def add_iterable_observation_to_link_cache(
        self, etype_to: str, post_validity: timedelta, eid: AnyEidT, dp: DataPointObservationsBase
    ):
        linked_eids = [v.eid for v in dp.v]
        self.cache.update_many(
            {
                "to": {"$in": [f"{etype_to}#{eid_}" for eid_ in linked_eids]},
                "from": f"{dp.etype}#{eid}",
                "using_attr": f"{dp.etype}#{dp.attr}",
            },
            {"$max": {"ttl": dp.t2 + post_validity}},
            upsert=True,
        )

    def remove_link_cache_of_deleted(self, etype: str, eid: AnyEidT):
        # Delete from master records
        # Get all links to the deleted entity grouped by attribute
        links = self.cache.aggregate(
            [
                {"$match": {"to": f"{etype}#{eid}"}},
                {"$group": {"_id": "$using_attr", "eids_from": {"$push": "$from"}}},
            ]
        )
        # Delete the retrieved references
        for link in links:
            etype_from, attr = link["_id"].split("#", maxsplit=1)
            eids_from = parse_eids_from_cache(self.model_spec, link["eids_from"])
            self.db.delete_link_dps(etype_from, eids_from, attr, eid)

        # Delete from cache
        self.cache.bulk_write(
            [DeleteMany({"from": f"{etype}#{eid}"}), DeleteMany({"to": f"{etype}#{eid}"})]
        )

    def remove_link_cache_of_many_deleted(self, etype: str, eids: list[AnyEidT]):
        # Delete from master records
        # Get all links to the deleted entities grouped by attribute
        links = self.cache.aggregate(
            [
                # Match links to the deleted entities
                {"$match": {"to": {"$in": [f"{etype}#{eid}" for eid in eids]}}},
                # Group the links by attribute (which includes the etype_from)
                {
                    "$group": {
                        "_id": "$using_attr",
                        "eids_to": {"$push": "$to"},
                        "eids_from": {"$push": "$from"},
                    }
                },
            ]
        )

        etypes_from = []
        attrs = []
        affected_eids = []
        eids_to = []
        for link in links:
            etype_from, attr = link["_id"].split("#", maxsplit=1)
            etypes_from.append(etype_from)
            attrs.append(attr)
            affected_eids.append(parse_eids_from_cache(self.model_spec, link["eids_from"]))
            eids_to.append(parse_eids_from_cache(self.model_spec, link["eids_to"]))

        self.db.delete_many_link_dps(etypes_from, affected_eids, attrs, eids_to)

        self.cache.bulk_write(
            [DeleteMany({"from": f"{etype}#{eid}"}) for eid in eids]
            + [DeleteMany({"to": f"{etype}#{eid}"}) for eid in eids]
        )
