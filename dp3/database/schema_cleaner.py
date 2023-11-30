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
SCHEMA_VERSION = 2


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
            schema_doc = {"_id": 0, "schema": schema, "version": SCHEMA_VERSION}
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
                "schema": { ... }, # (1)!
                "version": int
            }
        ```

        1. see [schema construction][dp3.database.schema_cleaner.SchemaCleaner.construct_schema_doc]

        Raises:
            ValueError: If conflicting changes are detected.
        """
        db_schema, config_schema, updates = self.get_schema_status()
        if db_schema["schema"] == config_schema["schema"]:
            self.log.info("Schema OK!")
            return

        if not updates:
            self.schemas.insert_one(config_schema)
            self.log.info("Updated schema, OK now!")
            return

        self.log.warning("Schema update that cannot be performed automatically is required.")
        self.log.warning("Please run `dp3 schema-update` to make the changes.")
        raise ValueError("Schema update failed: Conflicting changes detected.")

    def get_schema_status(self) -> tuple[dict, dict, dict]:
        """
        Gets the current schema status.
        `database_schema` is the schema document from the database.
        `configuration_schema` is the schema document constructed from the current configuration.
        `updates` is a dictionary of required updates to each entity.

        Returns:
            Tuple of (`database_schema`, `configuration_schema`, `updates`).
        """
        schema_doc = self.get_current_schema_doc(infer=True)

        current_schema = self.construct_schema_doc()

        if schema_doc["schema"] == current_schema:
            return schema_doc, schema_doc, {}

        new_schema = {
            "_id": schema_doc["_id"] + 1,
            "schema": current_schema,
            "version": SCHEMA_VERSION,
        }
        updates = self.detect_changes(schema_doc, current_schema)
        return schema_doc, new_schema, updates

    def detect_changes(
        self, db_schema_doc: dict, current_schema: dict
    ) -> dict[str, dict[str, dict[str, str]]]:
        """
        Detects changes between configured schema and the one saved in the database.

        Args:
            db_schema_doc: Schema document from the database.
            current_schema: Schema from the configuration.

        Returns:
            Required updates to each entity.
        """
        if db_schema_doc["schema"] == current_schema:
            return {}

        if db_schema_doc["version"] != SCHEMA_VERSION:
            self.log.info("Schema version changed, skipping detecting changes.")
            return {}

        updates = {}
        for entity, attributes in db_schema_doc["schema"].items():
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
        return updates

    def execute_updates(self, updates: dict[str, dict[str, dict[str, str]]]):
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

        configured_schema = self.construct_schema_doc()
        for delay in RECONNECT_DELAYS:
            if schema_doc["schema"] == configured_schema:
                break
            self.log.info("Schema mismatch, will await update by main worker in %s seconds", delay)
            time.sleep(delay)
            schema_doc = self.get_current_schema_doc()

        if schema_doc["schema"] != configured_schema:
            raise ValueError("Unable to establish matching schema")

        self.log.info("Schema OK!")
