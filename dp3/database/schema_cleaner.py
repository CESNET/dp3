import logging
import time
from collections import defaultdict
from datetime import datetime
from logging import Logger
from typing import Callable

import pymongo
from pymongo import DeleteOne, InsertOne
from pymongo.collection import Collection
from pymongo.database import Database

from dp3.common.attrspec import ID_REGEX, AttrSpecType, AttrType
from dp3.common.config import HierarchicalDict, ModelSpec
from dp3.common.utils import batched

# number of seconds to wait for the i-th attempt to reconnect after error
RECONNECT_DELAYS = [1, 2, 5, 10, 30]
# current database schema version
SCHEMA_VERSION = 4

# Collections belonging to entity
# Used when deleting no-longer existing entity.
ENTITY_COLLECTIONS = ["{}#master", "{}#raw", "{}#snapshots"]


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
        self,
        db: Database,
        schema_col: Collection,
        model_spec: ModelSpec,
        config: HierarchicalDict,
        log: Logger = None,
    ):
        if log is None:
            self.log = logging.getLogger("SchemaCleaner")
        else:
            self.log = log.getChild("SchemaCleaner")

        self._db = db
        self._model_spec = model_spec
        self._config = config
        self.schemas = schema_col

        self.storage = {
            "snapshot_bucket_size": self._config.get("database.storage.snapshot_bucket_size", 32)
        }

        self.migrations: dict[int, Callable[[dict], dict]] = {
            2: self.migrate_schema_2_to_3,
            3: self.migrate_schema_3_to_4,
        }

    def get_current_schema_doc(self, infer: bool = False) -> dict:
        """
        Args:
            infer: Whether to infer the schema if it is not found in the database.
        Returns:
            Current schema
        """
        schema_doc = self.schemas.find_one({}, sort=[("_id", pymongo.DESCENDING)])
        if schema_doc is None and infer:
            self.log.info("No schema found, inferring initial schema from database")
            schema = self.infer_current_schema()
            entity_id_types = self.construct_entity_types()
            schema_doc = {
                "_id": 0,
                "entity_id_types": entity_id_types,
                "schema": schema,
                "storage": self.storage,
                "version": SCHEMA_VERSION,
            }
        return schema_doc

    def safe_update_schema(self):
        """
        Checks whether schema saved in database is up-to-date and updates it if necessary.

        Will NOT perform any changes in master collections on conflicting model changes,
        but will raise an exception instead.
        Any such changes must be performed via the CLI.

        As this method modifies the schema collection, it should be called only by the main worker.

        The schema collection format is as follows:

        ```py
            {
                "_id": int,
                entity_id_types: {
                    <entity_type>: <entity_id_type>,
                    ...
                },
                "schema": { ... },  # (1)!
                "storage": {
                    "snapshot_bucket_size": int
                },
                "version": int
            }
        ```

        1. see [schema construction][dp3.database.schema_cleaner.SchemaCleaner.construct_schema_doc]

        Raises:
            ValueError: If conflicting changes are detected.
        """
        db_schema, config_schema, eid_updates, updates, deleted_entites = self.get_schema_status()

        if db_schema["version"] != config_schema["version"]:
            self.log.warning(
                "Schema version mismatch: %s (DB) != %s (config)",
                db_schema["version"],
                config_schema["version"],
            )
            return self._error_on_conflict()

        if db_schema["storage"] != config_schema["storage"]:
            self.log.warning(
                "DB storage settings mismatch: %s (DB) != %s (config)",
                db_schema["storage"],
                config_schema["storage"],
            )
            return self._error_on_conflict()

        if eid_updates:
            for entity in eid_updates:
                self.log.warning(
                    "Entity ID type mismatch for %s: %s (DB) -> %s (config)",
                    entity,
                    db_schema["entity_id_types"][entity],
                    config_schema["entity_id_types"][entity],
                )
            return self._error_on_conflict()

        if db_schema["schema"] == config_schema["schema"]:
            self.log.info("Schema OK!")
        elif not updates and not deleted_entites:
            self.schemas.insert_one(config_schema)
            self.log.info("Updated schema, OK now!")
        else:
            return self._error_on_conflict()

    def _error_on_conflict(self):
        self.log.warning("Schema update that cannot be performed automatically is required.")
        self.log.warning("Please run `dp3 schema-update` to make the changes.")
        raise ValueError("Schema update failed: Conflicting changes detected.")

    def get_schema_status(self) -> tuple[dict, dict, dict, dict, list]:
        """
        Gets the current schema status.
        `database_schema` is the schema document from the database.
        `configuration_schema` is the schema document constructed from the current configuration.
        `updates` is a dictionary of required updates to each entity.

        Returns:
            Tuple of (`database_schema`, `configuration_schema`, `updates`, `deleted_entites`).
        """
        schema_doc = self.get_current_schema_doc(infer=True)

        current_schema = self.construct_schema_doc()
        current_entity_types = self.construct_entity_types()

        new_schema = {
            "_id": schema_doc["_id"],
            "entity_id_types": current_entity_types,
            "schema": current_schema,
            "storage": self.storage,
            "version": SCHEMA_VERSION,
        }

        if schema_doc == new_schema:
            return schema_doc, schema_doc, {}, {}, []

        new_schema["_id"] += 1

        eid_updates, updates, deleted_entites = self.detect_changes(
            schema_doc, current_entity_types, current_schema
        )
        return schema_doc, new_schema, eid_updates, updates, deleted_entites

    def detect_changes(
        self, db_schema_doc: dict, current_entity_types: dict, current_schema: dict
    ) -> tuple[dict[str, str], dict[str, dict[str, dict[str, str]]], list[str]]:
        """
        Detects changes between configured schema and the one saved in the database.

        Args:
            db_schema_doc: Schema document from the database.
            current_entity_types: Entity ID types from the configuration.
            current_schema: Schema from the configuration.

        Returns:
            Tuple of required updates to each entity and list of deleted entites.
        """
        if db_schema_doc["version"] != SCHEMA_VERSION:
            self.log.info("Schema version changed, skipping detecting changes.")
            return {}, {}, []

        eid_updates = {}
        for entity, id_type in db_schema_doc["entity_id_types"].items():
            if entity not in current_schema:
                self.log.info("Schema breaking change: Entity %s was deleted", entity)
                continue

            if id_type != current_entity_types[entity]:
                self.log.info(
                    "Schema breaking change: Entity %s ID type changed: %s -> %s",
                    entity,
                    id_type,
                    current_entity_types[entity],
                )
                eid_updates[entity] = "$drop"

        if db_schema_doc["schema"] == current_schema:
            return eid_updates, {}, []

        updates = {}
        deleted_entites = []
        for entity, attributes in db_schema_doc["schema"].items():
            # Unset deleted entities
            if entity not in current_schema:
                self.log.info("Schema breaking change: Entity %s was deleted", entity)
                deleted_entites.append(entity)
                continue

            entity_updates = defaultdict(dict)
            current_attributes = current_schema[entity]

            # Unset deleted attributes
            deleted = set(attributes) - set(current_attributes)
            for attr in deleted:
                self.log.info("Schema breaking change: Attribute %s.%s was deleted", entity, attr)
                entity_updates["$unset"][attr] = ""

            for attr, spec in attributes.items():
                if attr in deleted:
                    continue
                model = current_attributes[attr]

                for key, val in spec.items():
                    ref = model[key]
                    if ref == val:
                        continue

                    self.log.info(
                        "Schema breaking change: '%s' of %s.%s changed: %s -> %s",
                        key,
                        entity,
                        attr,
                        val,
                        ref,
                    )
                    entity_updates["$unset"][attr] = ""

            if entity_updates:
                updates[entity] = entity_updates

        return eid_updates, updates, deleted_entites

    def execute_updates(
        self, updates: dict[str, dict[str, dict[str, str]]], deleted_entites: list[str]
    ):
        # Delete entities
        for entity in deleted_entites:
            try:
                for col_placeholder in ENTITY_COLLECTIONS:
                    col = col_placeholder.format(entity)
                    self.log.info("%s: Deleting collection: %s", entity, col)
                    self._db[col].drop()

                # Delete entity from Link cache
                self.log.info("%s: Deleting entity entries from Link cache", entity)
                self._db["#cache#Link"].delete_many(
                    {
                        "$or": [
                            {"from": {"$regex": f"^{entity}#"}},
                            {"to": {"$regex": f"^{entity}#"}},
                        ]
                    }
                )
            except Exception as e:
                raise ValueError(f"Schema update failed: {e}") from e

        # Update attributes
        for entity, entity_updates in updates.items():
            try:
                self.log.info(
                    "%s: Performing master collection update: %s", entity, dict(entity_updates)
                )
                master_name = f"{entity}#master"
                res = self._db[master_name].update_many({}, entity_updates)
                self.log.debug("%s: Updated %s master records.", entity, res.modified_count)
            except Exception as e:
                raise ValueError(f"Schema update failed: {e}") from e

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

    def construct_entity_types(self) -> dict:
        """
        Constructs entity ID type dictionary from model specification.

        Returns:
            Dictionary with entity ID types.
        """
        return {
            entity: entity_spec.id_data_type.type_info
            for entity, entity_spec in self._model_spec.entities.items()
        }

    def await_updated(self):
        """
        Checks whether schema saved in database is up-to-date and awaits its update
        by the main worker on mismatch.
        """
        self.log.info("Fetching current schema")
        schema_doc = self.get_current_schema_doc()
        for delay in RECONNECT_DELAYS:
            if schema_doc is not None:
                break
            self.log.info("Schema not found, will retry in %s seconds", delay)
            time.sleep(delay)
            schema_doc = self.get_current_schema_doc()

        if schema_doc is None:
            raise ValueError("Unable to establish schema")

        db_schema, config_schema, *_ = self.get_schema_status()
        for delay in RECONNECT_DELAYS:
            if db_schema == config_schema:
                break
            self.log.info("Schema mismatch, will await update by main worker in %s seconds", delay)
            time.sleep(delay)
            db_schema, config_schema, *_ = self.get_schema_status()

        if db_schema != config_schema:
            raise ValueError("Unable to establish matching schema")

        self.log.info("Schema OK!")

    def get_index_name_by_keys(self, collection: str, keys: dict) -> str:
        """
        Gets index name by keys.

        Args:
            collection: Collection name.
            keys: Index keys.
        Returns:
            Index name.
        """
        for index in self._db[collection].list_indexes():
            if index["key"] == keys:
                return index["name"]
        raise ValueError(f"Index not found for {collection} with keys {keys}")

    def fix_latest_bucket_index(self, collection: str, new_bucket_size: int):
        index = self.get_index_name_by_keys(collection, {"_id": -1, "count": 1})
        self.log.info("Dropping index %s.%s", collection, index)
        self._db[collection].drop_index(index)
        name = self._db[collection].create_index(
            [("_id", pymongo.DESCENDING), ("count", pymongo.ASCENDING)],
            partialFilterExpression={"count": {"$lt": new_bucket_size}},
            background=True,
        )
        self.log.info("Created index %s.%s", collection, name)

    def update_storage(self, prev_storage: dict, curr_storage: dict):
        """
        Updates storage settings in schema.

        Args:
            prev_storage: Previous storage settings.
            curr_storage: Current storage settings.
        """
        self.log.info("Updating storage settings")

        if prev_storage["snapshot_bucket_size"] != curr_storage["snapshot_bucket_size"]:
            self.log.info(
                "Snapshot bucket size changed: %s -> %s",
                prev_storage["snapshot_bucket_size"],
                curr_storage["snapshot_bucket_size"],
            )

            for entity in self._model_spec.entities:
                snapshot_col = f"{entity}#snapshots"

                # Bucket size decreased, fix indexes before updating
                if prev_storage["snapshot_bucket_size"] > curr_storage["snapshot_bucket_size"]:
                    self.fix_latest_bucket_index(snapshot_col, curr_storage["snapshot_bucket_size"])

                self.log.info("Updating %s", entity)
                self._db[snapshot_col].update_many(
                    {"oversized": False},
                    {"$set": {"count": curr_storage["snapshot_bucket_size"]}},
                )

                # Bucket size increased, fix indexes after updating
                if prev_storage["snapshot_bucket_size"] <= curr_storage["snapshot_bucket_size"]:
                    self.fix_latest_bucket_index(snapshot_col, curr_storage["snapshot_bucket_size"])

    def migrate(self, schema_doc: dict) -> dict:
        """
        Migrates schema to the latest version.

        Args:
            schema_doc: Schema to migrate.
        Returns:
            Migrated schema.
        """
        if schema_doc["version"] == SCHEMA_VERSION:
            return schema_doc

        for version in range(schema_doc["version"], SCHEMA_VERSION):
            self.log.info("Migrating schema from version %s to %s", version, version + 1)
            schema_doc = self.migrations[version](schema_doc)
            self.schemas.insert_one(schema_doc)

        return schema_doc

    def migrate_schema_2_to_3(self, schema: dict) -> dict:
        """
        Migrates schema from version 2 to version 3.

        This migration adds the storage setting for snapshot bucket size to the schema.

        Args:
            schema: Schema to migrate.
        Returns:
            Migrated schema.
        """
        # Set created time for all snapshots, append last to history
        bucket_size = self._config.get("database.storage.snapshot_bucket_size", 32)

        for entity in self._model_spec.entities:
            self.log.info("Migrating %s", entity)
            snapshot_col = self._db[f"{entity}#snapshots"]
            for doc in self._db[f"{entity}#snapshots"].find(
                {"_id": {"$not": {"$regex": r"_#\d+$"}}}
            ):
                if doc.get("oversized", False):
                    ctime = doc["last"].get("_time_created", datetime.now())
                    snapshot_col.bulk_write(
                        [
                            InsertOne(
                                {
                                    **doc,
                                    "_id": f"{doc['_id']}_#{int(ctime.timestamp())}",
                                    "_time_created": ctime,
                                    "count": 0,
                                }
                            ),
                            DeleteOne({"_id": doc["_id"]}),
                        ]
                    )
                    continue

                to_insert = []
                for snapshot_bucket in batched([doc["last"]] + doc["history"], bucket_size):
                    ctime = snapshot_bucket[-1].get(
                        "_time_created",
                    )
                    to_insert.append(
                        {
                            "_id": f"{doc['_id']}_#{int(ctime.timestamp())}",
                            "last": snapshot_bucket[0],
                            "history": snapshot_bucket,
                            "_time_created": ctime,
                            "count": len(snapshot_bucket),
                            "oversized": False,
                        }
                    )
                snapshot_col.insert_many(to_insert)
                snapshot_col.delete_one({"_id": doc["_id"]})

        # Update schema
        schema["_id"] += 1
        schema["version"] = 3
        schema["storage"] = {"snapshot_bucket_size": bucket_size}

        return schema

    @staticmethod
    def migrate_schema_3_to_4(schema: dict) -> dict:
        """
        Migrates schema from version 3 to version 4.

        This migration adds eid_data_type specification to the schema.
        As all entities were previously using string as their entity ID type,
        this is merely an accounting change.
        Actual data types are to be changed by following migrations.

        Args:
            schema: Schema to migrate.
        Returns:
            Migrated schema.
        """
        schema["_id"] += 1
        schema["version"] = 4
        schema["entity_id_types"] = {entity: str(str) for entity in schema["schema"]}

        return schema
