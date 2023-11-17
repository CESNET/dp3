import logging
import time
from collections import defaultdict
from logging import Logger

import pymongo
from pymongo.collection import Collection
from pymongo.database import Database

from dp3.common.attrspec import ID_REGEX, AttrSpecType, AttrType
from dp3.common.config import ModelSpec

# number of seconds to wait for the i-th attempt to reconnect after error
RECONNECT_DELAYS = [1, 2, 5, 10, 30]
# current database schema version
SCHEMA_VERSION = 1


class SchemaCleaner:
    """
    Maintains a history of `model_spec` defined schema, updates the database when needed.

    Args:
        db: Target database mapping. (not just a database connection)
        schema_col: Collection where schema history is stored.
        model_spec: Model specification.
        log: Logger instance to serve as parent. If not provided, a new logger will be created.
    """

    def __init__(
        self, db: Database, schema_col: Collection, model_spec: ModelSpec, log: Logger = None
    ):
        if log is None:
            self.log = logging.getLogger("SchemaCleaner")
        else:
            self.log = log.getChild("SchemaCleaner")

        self._db = db
        self._model_spec = model_spec
        self.schemas = schema_col

    def update_schema(self):
        """
        Checks whether schema saved in database is up-to-date and updates it if necessary.

        Will perform changes in master collections where required based on schema changes.
        As this method modifies the schema collection, it should be called only by the main worker.

        The schema collection format is as follows:

        ```py
            {
                "_id": int,
                "schema": { ... }, # (1)!
                "version": int
            }
        ```

        1. see [schema construction][dp3.database.schema_cleaner.SchemaCleaner.construct_schema_doc]
        """
        schema_doc = self.schemas.find_one({}, sort=[("_id", pymongo.DESCENDING)])
        if schema_doc is None:
            self.log.info("No schema found, inferring initial schema from database")
            schema = self.infer_current_schema()
            schema_doc = {"_id": 0, "schema": schema, "version": SCHEMA_VERSION}

        current_schema = self.construct_schema_doc()
        if schema_doc["schema"] != current_schema:
            new_schema = {
                "_id": schema_doc["_id"] + 1,
                "schema": current_schema,
                "version": SCHEMA_VERSION,
            }
            if schema_doc["version"] != SCHEMA_VERSION:
                self.log.info("Schema version changed, updating without changes.")
                self.schemas.insert_one(new_schema)
                return

            for entity, attributes in schema_doc["schema"].items():
                updates = defaultdict(dict)
                current_attributes = current_schema[entity]

                # Unset deleted attributes
                deleted = set(attributes) - set(current_attributes)
                if deleted:
                    updates["$unset"] |= {attr: "" for attr in deleted}

                for attr, spec in attributes.items():
                    if attr in deleted:
                        continue
                    model = current_attributes[attr]

                    for key, val in spec.items():
                        ref = model[key]
                        if ref != val:
                            self.log.info(
                                "'%s' of %s.%s changed: %s -> %s",
                                key,
                                entity,
                                attr,
                                spec[key],
                                model[key],
                            )
                            updates["$unset"][attr] = ""

                if not updates:
                    self.log.debug("%s: No changes required", entity)
                    continue

                try:
                    self.log.info(
                        "%s: Performing master collection update: %s", entity, dict(updates)
                    )
                    master_name = f"{entity}#master"
                    res = self._db[master_name].update_many({}, updates)
                    self.log.debug("%s: Updated %s master records.", entity, res.modified_count)
                except Exception as e:
                    raise ValueError(f"Schema update failed: {e}") from e

            self.schemas.insert_one(new_schema)
            self.log.info("Updated schema, OK now!")
        else:
            self.log.info("Schema OK!")

    def infer_current_schema(self) -> dict:
        """
        Infers schema from current database state.

        Will detect the attribute type, i.e. whether it is
        a plain, timeseries or observations attribute.

        All other properties will be set based on configuration.

        Returns:
            Dictionary with inferred schema.
        """

        cols = self._db.list_collections(filter={"name": {"$regex": r"^[^#]+#master$"}})
        col_names = [col["name"] for col in cols]

        entities = defaultdict(dict)
        for name in col_names:
            entity = name.split("#", maxsplit=1)[0]
            self.log.debug("Inferring schema for '%s':", entity)
            self.log.debug("%s: Testing attributes for history", entity)
            res = self._db[name].aggregate(
                [
                    # Get each attribute as a separate document
                    {"$project": {"items": {"$objectToArray": "$$ROOT"}}},
                    {"$unwind": "$items"},
                    # Check if attribute has history
                    {"$set": {"array": {"$isArray": "$items.v"}}},
                    # Group by attribute name
                    {"$group": {"_id": "$items.k", "array": {"$addToSet": "$array"}}},
                    # Filter out non-attribute properties
                    {"$match": {"_id": {"$regex": ID_REGEX}}},
                    {"$match": {"_id": {"$regex": r"^(?!_id$)"}}},
                ]
            )
            attributes = entities[entity]
            undecided = []
            for doc in res:
                if doc["array"] == [False]:
                    attributes[doc["_id"]] = {"t": AttrType.PLAIN.value}
                elif doc["array"] == [True]:
                    attributes[doc["_id"]] = {
                        "t": AttrType.TIMESERIES.value | AttrType.OBSERVATIONS.value
                    }
                    undecided.append(doc["_id"])
                else:
                    self.log.error("Unknown attribute type: %s.%s", entity, doc["_id"])

            if not undecided:
                continue

            self.log.debug("%s: Testing observations", entity)
            res = self._db[name].aggregate(
                [
                    # Get each attribute as a separate document
                    {"$project": {"items": {"$objectToArray": "$$ROOT"}}},
                    {"$unwind": "$items"},
                    # Check if attribute has confidence (i.e. is observation)
                    {"$match": {"items.k": {"$in": undecided}, "items.v.c": {"$exists": True}}},
                    # Group by attribute name
                    {"$group": {"_id": "$items.k"}},
                ]
            )
            for doc in res:
                attributes[doc["_id"]]["t"] = AttrType.OBSERVATIONS.value
                undecided.remove(doc["_id"])

            if not undecided:
                continue

            self.log.debug("%s: Testing timeseries", entity)
            res = self._db[name].aggregate(
                [
                    # Get each attribute as a separate document
                    {"$project": {"items": {"$objectToArray": "$$ROOT"}}},
                    {"$unwind": "$items"},
                    # Check if attribute has no confidence (i.e. is timeseries)
                    {
                        "$match": {
                            "items.k": {"$in": undecided},
                            "items.v.c": {"$exists": False},
                            "items.v.0": {"$exists": True},
                        }
                    },
                    # Group by attribute name
                    {"$group": {"_id": "$items.k"}},
                ]
            )
            for doc in res:
                attributes[doc["_id"]]["t"] = AttrType.TIMESERIES.value
                undecided.remove(doc["_id"])

            if not undecided:
                continue

            self.log.debug("Resolving undecided attributes: %s", undecided)
            for attr, spec in attributes.items():
                if attr in undecided and (entity, attr) in self._model_spec.attributes:
                    config_spec = self._model_spec.attr(entity, attr)
                    self.log.debug("Assuming configuration type correct for %s.%s", entity, attr)
                    spec["t"] = config_spec.t.value

        for entity, attributes in entities.items():
            for attr, spec in attributes.items():
                if (entity, attr) in self._model_spec.attributes:
                    config_spec = self._model_spec.attr(entity, attr)
                    config_dict = self._construct_spec_dict(config_spec)
                    config_dict.update(spec)
                    spec.update(config_dict)
                self.log.info("Inferred schema for %s.%s: %s", entity, attr, spec)

        return dict(entities)

    def construct_schema_doc(self):
        """
        Constructs schema document from current schema configuration.

        The schema document format is as follows:

        ```py
            {
                "entity_type": {
                    "attribute": {
                        "t": 1 | 2 | 4,
                        "data_type": <data_type.type_info> | None
                        "timeseries_type": <timeseries_type> | None
                        "series": dict[str, <data_type.type_info>] | None
                    }
                }
            }
        ```

        where `t` is [attribute type][dp3.common.attrspec.AttrType],
        and `data_type` is the data type string.
        `timeseries_type` is one of `regular`, `irregular` or `irregular_intervals`.
        `series` is a dictionary of series names and their data types.

        `timeseries_type` and `series` are present only for timeseries attributes,
        `data_type` is present only for plain and observations attributes.
        """
        return {
            entity_type: {
                attr: self._construct_spec_dict(spec) for attr, spec in attributes.items()
            }
            for entity_type, attributes in self._model_spec.entity_attributes.items()
        }

    @staticmethod
    def _construct_spec_dict(spec: AttrSpecType):
        res = {"t": spec.t.value, "data_type": None, "timeseries_type": None, "series": None}
        if spec.t in AttrType.PLAIN | AttrType.OBSERVATIONS:
            res["data_type"] = spec.data_type.type_info
        elif spec.t == AttrType.TIMESERIES:
            res["timeseries_type"] = spec.timeseries_type
            res["series"] = {s: sspec.data_type.type_info for s, sspec in spec.series.items()}
        return res

    def await_updated(self):
        """
        Checks whether schema saved in database is up-to-date and awaits its update
        by the main worker on mismatch.
        """
        self.log.info("Fetching current schema")
        schema_doc = self.schemas.find_one(sort=[("_id", pymongo.DESCENDING)])
        for delay in RECONNECT_DELAYS:
            if schema_doc is not None:
                break
            self.log.info("Schema not found, will retry in %s seconds", delay)
            time.sleep(delay)
            schema_doc = self.schemas.find_one(sort=[("_id", pymongo.DESCENDING)])

        if schema_doc is None:
            raise ValueError("Unable to establish schema")

        configured_schema = self.construct_schema_doc()
        for delay in RECONNECT_DELAYS:
            if schema_doc["schema"] == configured_schema:
                break
            self.log.info("Schema mismatch, will await update by main worker in %s seconds", delay)
            time.sleep(delay)
            schema_doc = self.schemas.find_one(sort=[("_id", pymongo.DESCENDING)])

        if schema_doc["schema"] != configured_schema:
            raise ValueError("Unable to establish matching schema")

        self.log.info("Schema OK!")
