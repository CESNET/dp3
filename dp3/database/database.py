import inspect
import logging
import threading
import time
from collections import defaultdict
from collections.abc import Generator, Iterator
from datetime import datetime
from typing import Callable, Optional

import pymongo
from event_count_logger import DummyEventGroup
from pydantic import BaseModel
from pymongo import ReplaceOne, UpdateMany, UpdateOne, WriteConcern
from pymongo.collection import Collection
from pymongo.command_cursor import CommandCursor
from pymongo.cursor import Cursor
from pymongo.database import Database
from pymongo.errors import OperationFailure
from pymongo.results import DeleteResult, UpdateResult

from dp3.common.attrspec import AttrType, timeseries_types
from dp3.common.config import HierarchicalDict, ModelSpec
from dp3.common.datapoint import DataPointBase
from dp3.common.datatype import AnyEidT
from dp3.common.scheduler import Scheduler
from dp3.common.task import HASH
from dp3.common.types import EventGroupType
from dp3.database.config import MongoConfig, MongoReplicaConfig, MongoStandaloneConfig
from dp3.database.encodings import get_codec_options
from dp3.database.exceptions import DatabaseError
from dp3.database.schema_cleaner import SchemaCleaner
from dp3.database.snapshots import SnapshotCollectionContainer


def get_caller_id():
    """Returns the name of the caller method's class, or function name if caller is not a method."""
    caller = inspect.stack()[2]
    if module := caller.frame.f_locals.get("self"):
        return module.__class__.__qualname__
    return caller.function


# number of seconds to wait for the i-th attempt to reconnect after error
RECONNECT_DELAYS = [1, 2, 5, 10, 30]


class EntityDatabase:
    """
    MongoDB database wrapper responsible for whole communication with database server.
    Initializes database schema based on database configuration.

    Args:
        config: configuration of database connection (content of database.yml)
        model_spec: ModelSpec object, configuration of data model (entities and attributes)
        process_index: index of worker process - used for sharding metadata
        num_processes: number of worker processes
    """

    def __init__(
        self,
        config: HierarchicalDict,
        model_spec: ModelSpec,
        num_processes: int,
        process_index: int = 0,
        elog: Optional[EventGroupType] = None,
    ) -> None:
        self.log = logging.getLogger("EntityDatabase")
        self.elog = elog or DummyEventGroup()

        db_config = MongoConfig.model_validate(config.get("database", {}))

        self.log.info("Connecting to database...")
        for attempt, delay in enumerate(RECONNECT_DELAYS):
            try:
                self._db = self.connect(db_config)
                # Check if connected
                self._db.admin.command("ping")
            except pymongo.errors.ConnectionFailure as e:
                if attempt + 1 == len(RECONNECT_DELAYS):
                    raise DatabaseError(
                        "Cannot connect to database with specified connection arguments."
                    ) from e
                else:
                    self.log.error(
                        "Cannot connect to database (attempt %d, retrying in %ds).",
                        attempt + 1,
                        delay,
                    )
                    time.sleep(delay)

        self._db_schema_config = model_spec
        self._num_processes = num_processes
        self._process_index = process_index

        # Init and switch to correct database
        codec_opts = get_codec_options()
        self._db = Database(self._db, db_config.db_name, codec_options=codec_opts)

        self.snapshots = SnapshotCollectionContainer(
            self._db, db_config, model_spec, config.get("snapshots", {})
        )
        self._snapshot_bucket_size = db_config.storage.snapshot_bucket_size

        if process_index == 0:
            self._init_database_schema()

        self.schema_cleaner = SchemaCleaner(
            self._db, self.get_module_cache("Schema"), self._db_schema_config, config, self.log
        )

        self._on_entity_delete_one = []
        self._on_entity_delete_many = []

        self._raw_buffer_locks = {etype: threading.Lock() for etype in model_spec.entities}
        self._raw_buffers = defaultdict(list)
        self._master_buffer_locks = {etype: threading.Lock() for etype in model_spec.entities}
        self._master_buffers = defaultdict(dict)

        self._sched = Scheduler()
        seconds = ",".join(
            f"{int(i)}" for i in range(60) if int(i - process_index) % min(num_processes, 3) == 0
        )
        self._sched.register(self._push_raw, second=seconds, misfire_grace_time=5)
        self._sched.register(self._push_master, second=seconds, misfire_grace_time=5)

        self.log.info("Database successfully initialized!")

    @staticmethod
    def connect(config: MongoConfig) -> pymongo.MongoClient:
        if isinstance(config.connection, MongoStandaloneConfig):
            host = config.connection.host
            return pymongo.MongoClient(
                f"mongodb://{config.username}:{config.password}@{host.address}:{host.port}/",
                connectTimeoutMS=3000,
                serverSelectionTimeoutMS=5000,
            )
        elif isinstance(config.connection, MongoReplicaConfig):
            uri = (
                f"mongodb://{config.username}:{config.password}@"
                + ",".join(f"{host.address}:{host.port}" for host in config.connection.hosts)
                + f"/?replicaSet={config.connection.replica_set}"
            )
            return pymongo.MongoClient(
                uri,
                replicaSet=config.connection.replica_set,
                connectTimeoutMS=3000,
                serverSelectionTimeoutMS=5000,
            )
        else:
            raise NotImplementedError()

    def start(self) -> None:
        """Starts the database sync of raw datapoint inserts."""
        self._sched.start()

    def stop(self) -> None:
        """Stops the database sync, push remaining datapoints."""
        self._sched.stop()
        self._push_raw()
        self._push_master()

    def register_on_entity_delete(
        self, f_one: Callable[[str, AnyEidT], None], f_many: Callable[[str, list[AnyEidT]], None]
    ):
        """Registers function to be called when entity is forcibly deleted."""
        self._on_entity_delete_one.append(f_one)
        self._on_entity_delete_many.append(f_many)

    def _master_col(self, entity: str, **kwargs) -> Collection:
        """Returns master collection for `entity`.

        **kwargs: additional arguments for `pymongo.Database.get_collection`.
        """
        return self._db.get_collection(f"{entity}#master", **kwargs)

    def _snapshot_col(self, entity: str, **kwargs) -> Collection:
        """Returns snapshots collection for `entity`.

        **kwargs: additional arguments for `pymongo.Database.get_collection`.
        """
        return self._db.get_collection(f"{entity}#snapshots", **kwargs)

    def _oversized_snapshots_col(self, entity: str, **kwargs) -> Collection:
        """Returns oversized snapshots collection for `entity`.

        **kwargs: additional arguments for `pymongo.Database.get_collection`.
        """
        return self._db.get_collection(f"{entity}#snapshots_oversized", **kwargs)

    @staticmethod
    def _raw_col_name(entity: str) -> str:
        """Returns name of raw data collection for `entity`."""
        return f"{entity}#raw"

    def _raw_col(self, entity: str, **kwargs) -> Collection:
        """Returns raw data collection for `entity`.

        **kwargs: additional arguments for `pymongo.Database.get_collection`.
        """
        return self._db.get_collection(f"{entity}#raw", **kwargs)

    @staticmethod
    def _get_new_archive_col_name(entity: str) -> str:
        """Returns name of new archive collection for `entity`."""
        return f"{entity}#archive_{datetime.utcnow().strftime('%Y_%m_%d_%H%M%S')}"

    def _archive_col_names(self, entity: str) -> list[str]:
        """Returns names of archive collections for `entity`."""
        return self._db.list_collection_names(filter={"name": {"$regex": f"{entity}#archive.*"}})

    def _archive_cols(self, entity: str, **kwargs) -> Generator[Collection, None, None]:
        """Returns archive collections for `entity`.

        **kwargs: additional arguments for `pymongo.Database.get_collection`.
        """
        for col_name in self._archive_col_names(entity):
            yield self._db.get_collection(col_name, **kwargs)

    def _init_database_schema(self) -> None:
        """Runs full check and update of database schema.

        Checks whether `db_name` database exists. If not, creates it.
        Checks whether all collections exist. If not, creates them.
        This all is done automatically in MongoDB.

        We need to create indexes, however.
        """
        for etype in self._db_schema_config.entities:
            # Snapshots index on `eid` and `_time_created` fields
            snapshot_col = self._oversized_snapshots_col(etype)
            snapshot_col.create_index("eid", background=True)
            snapshot_col.create_index("_time_created", background=True)

            snapshot_col = self._snapshot_col(etype)
            # To get the empty bucket for an entity without fetching all its buckets
            for ix in snapshot_col.list_indexes():
                if set(ix["key"].keys()) == {"_id", "count"}:
                    if ix.get("partialFilterExpression") is None:
                        snapshot_col.drop_index(ix["name"])
                        snapshot_col.create_index(
                            [("_id", pymongo.DESCENDING), ("count", pymongo.ASCENDING)],
                            partialFilterExpression={"count": {"$lt": self._snapshot_bucket_size}},
                            background=True,
                        )
                    break
            else:
                snapshot_col.create_index(
                    [("_id", pymongo.DESCENDING), ("count", pymongo.ASCENDING)],
                    partialFilterExpression={"count": {"$lt": self._snapshot_bucket_size}},
                    background=True,
                )

            # To fetch all the latest snapshots without relying on date
            snapshot_col.create_index(
                [("latest", pymongo.DESCENDING), ("_id", pymongo.ASCENDING)],
                partialFilterExpression={"latest": {"$eq": True}},
                background=True,
            )

            # To fetch the oversized entities only
            snapshot_col.create_index(
                [("oversized", pymongo.DESCENDING)],
                partialFilterExpression={"oversized": {"$eq": True}},
                background=True,
            )
            # For deleting old snapshots
            snapshot_col.create_index("_time_created", background=True)

        # Create a TTL index for metadata collection
        self._db["#metadata"].create_index(
            "#time_created", expireAfterSeconds=60 * 60 * 24 * 30, background=True
        )

        # Master indexes
        for etype in self._db_schema_config.entities:
            master_col = self._master_col(etype)
            history_attrs = set()
            plain_attrs = set()

            for attr, spec in self._db_schema_config.entity_attributes[etype].items():
                if spec.t == AttrType.PLAIN:
                    plain_attrs.add(attr)
                if spec.t in AttrType.TIMESERIES | AttrType.OBSERVATIONS:
                    history_attrs.add(attr)

            attrs = history_attrs | plain_attrs

            # Drop previous indexes
            create_wildcard = True
            for index in master_col.list_indexes():
                if index["name"] == "_id_":
                    continue
                attr_names = set(index["key"].keys())

                # Drop individual attribute indexes
                if attr_names & attrs:
                    self.log.info("Dropping index %s on %s", index["name"], etype)
                    master_col.drop_index(index["name"])

                # Drop wildcard indexes if there are missing attributes
                if "$**" in attr_names:
                    if index.get("wildcardProjection") is not None:
                        covered_attrs = {
                            it
                            for k in index["wildcardProjection"]
                            for it in k.rsplit(".", maxsplit=1)
                        }
                        covered_attrs -= {"ts_last_update", "#min_t2s"}
                        if "t2" not in covered_attrs and not attrs - covered_attrs:
                            create_wildcard = False
                            continue  # Index already covers all history attributes
                    self.log.info("Dropping wildcard index %s on %s", index["name"], etype)
                    master_col.drop_index(index["name"])

            # Create an index on the id hash
            master_col.create_index("#hash", background=True)

            if not create_wildcard:
                continue

            # Create wildcard index for attribute histories
            index_name = f"wildcard_{datetime.now().strftime('%Y%m%d%H%M%S')}"
            try:
                master_col.create_index(
                    [("$**", 1)],
                    name=index_name,
                    wildcardProjection={f"#min_t2s.{attr}": 1 for attr in history_attrs}
                    | {f"{attr}.ts_last_update": 1 for attr in plain_attrs},
                )
                self.log.info("Created wildcard index %s on %s", index_name, etype)
            except OperationFailure as why:
                self.log.error("Failed to create wildcard index: %s", why)

    def _assert_etype_exists(self, etype: str):
        """Asserts `etype` existence.

        If doesn't exist, raises `DatabaseError`.
        """
        if etype not in self._db_schema_config.entities:
            raise DatabaseError(f"Entity '{etype}' does not exist")

    def _assert_eid_correct_dtype(self, etype: str, eid: AnyEidT):
        """Asserts `etype` existence.

        If doesn't exist, raises `DatabaseError`.
        """
        entity_spec = self._db_schema_config.entities[etype]
        if not isinstance(eid, entity_spec.eid_type):
            raise DatabaseError(
                f"Invalid entity ID type: {type(eid)}, expected {entity_spec.eid_type}"
            )
        return entity_spec

    def update_schema(self):
        """
        Checks whether schema saved in database is up-to-date and updates it if necessary.

        Will NOT perform any changes in master collections on conflicting model changes.
        Any such changes must be performed via the CLI.

        As this method modifies the schema collection, it should be called only by the main worker.
        """
        try:
            self.schema_cleaner.safe_update_schema()
        except ValueError as e:
            raise DatabaseError("Schema update failed. Please run `dp3 schema-update`.") from e

    def await_updated_schema(self):
        """
        Checks whether schema saved in database is up-to-date and awaits its update
        by the main worker on mismatch.
        """
        self.schema_cleaner.await_updated()

    def insert_datapoints(
        self, eid: AnyEidT, dps: list[DataPointBase], new_entity: bool = False
    ) -> None:
        """Inserts datapoint to raw data collection and updates master record.

        Raises DatabaseError when insert or update fails.
        """
        if len(dps) == 0:
            return

        etype = dps[0].etype

        # Check `etype`
        self._assert_etype_exists(etype)
        self._assert_eid_correct_dtype(etype, eid)

        # Insert raw datapoints
        dps_dicts = [dp.model_dump(exclude={"attr_type"}) for dp in dps]
        with self._raw_buffer_locks[etype]:
            self._raw_buffers[etype].extend(dps_dicts)

        # Update master document
        master_changes = {"pushes": defaultdict(list), "$set": {}}
        for dp in dps:
            attr_spec = self._db_schema_config.attr(etype, dp.attr)

            if attr_spec.t in AttrType.PLAIN | AttrType.OBSERVATIONS and attr_spec.is_iterable:
                v = [elem.model_dump() if isinstance(elem, BaseModel) else elem for elem in dp.v]
            else:
                v = dp.v.model_dump() if isinstance(dp.v, BaseModel) else dp.v

            # Rewrite value of plain attribute
            if attr_spec.t == AttrType.PLAIN:
                master_changes["$set"][dp.attr] = {"v": v, "ts_last_update": datetime.now()}

            # Push new data of observation
            if attr_spec.t == AttrType.OBSERVATIONS:
                master_changes["pushes"][dp.attr].append(
                    {"t1": dp.t1, "t2": dp.t2, "v": v, "c": dp.c}
                )

            # Push new data of timeseries
            if attr_spec.t == AttrType.TIMESERIES:
                master_changes["pushes"][dp.attr].append({"t1": dp.t1, "t2": dp.t2, "v": v})

        if new_entity:
            master_changes["$set"]["#hash"] = HASH(f"{etype}:{eid}")
            master_changes["$set"]["#time_created"] = datetime.now()

        with self._master_buffer_locks[etype]:
            if eid in self._master_buffers[etype]:
                for attr, push_dps in master_changes["pushes"].items():
                    if attr in self._master_buffers[etype][eid]["pushes"]:
                        self._master_buffers[etype][eid]["pushes"][attr].extend(push_dps)
                    else:
                        self._master_buffers[etype][eid]["pushes"][attr] = push_dps
                self._master_buffers[etype][eid]["$set"].update(master_changes["$set"])
            else:
                self._master_buffers[etype][eid] = master_changes

    def _push_raw(self):
        """Pushes datapoints to raw collections."""
        for etype, lock in self._raw_buffer_locks.items():
            begin = time.time()
            with lock:
                locked = time.time()
                dps = self._raw_buffers[etype]
                self._raw_buffers[etype] = []
                if not dps:
                    continue
            try:
                self._raw_col(etype, write_concern=WriteConcern(w=0)).insert_many(
                    dps, ordered=False
                )
                end = time.time()
                self.log.debug(
                    "Inserted %s raw datapoints to %s in %.3fs, %.3fus lock, %.3fs insert",
                    len(dps),
                    etype,
                    end - begin,
                    (locked - begin) * 1000_000,
                    end - locked,
                )
            except Exception as e:
                raise DatabaseError(f"Insert of datapoints failed: {e}\n{dps}") from e

    def _push_master(self):
        """Push master changes to database."""
        for etype, lock in self._master_buffer_locks.items():
            master_col = self._master_col(etype, write_concern=WriteConcern(w=1))
            begin = time.time()
            with lock:
                locked = time.time()
                master_changes = self._master_buffers[etype]
                self._master_buffers[etype] = {}
            if not master_changes:
                continue
            try:
                updates = [
                    UpdateOne(
                        {"_id": eid},
                        (
                            {
                                "$push": {
                                    attr: push_dps[0] if len(push_dps) == 1 else {"$each": push_dps}
                                    for attr, push_dps in changes.get("pushes", {}).items()
                                }
                            }
                            if "pushes" in changes
                            else {}
                        )
                        | ({"$set": changes["$set"]} if "$set" in changes else {})
                        | ({"$max": changes["$max"]} if "$max" in changes else {}),
                        upsert=True,
                    )
                    for eid, changes in master_changes.items()
                ]
                updates_ready = time.time()
                res = master_col.bulk_write(updates, ordered=False)
                end = time.time()
                self.log.debug(
                    "Updated %s master records in %.3fs, "
                    "%.3fus lock, %.3fms update prep, %.3fs update",
                    len(master_changes),
                    end - begin,
                    (locked - begin) * 1000_000,
                    (updates_ready - locked) * 1000,
                    end - updates_ready,
                )
                for error in res.bulk_api_result.get("writeErrors", []):
                    self.log.error("Error in bulk write: %s", error)
            except Exception as e:
                raise DatabaseError(
                    f"Update of master records failed: {e}\n{master_changes}"
                ) from e

    def update_master_records(self, etype: str, eids: list[AnyEidT], records: list[dict]) -> None:
        """Replace master records of `etype`:`eid` with the provided `records`.

        Raises DatabaseError when update fails.
        """
        master_col = self._master_col(etype)
        try:
            res = master_col.bulk_write(
                [
                    ReplaceOne({"_id": eid}, record, upsert=True)
                    for eid, record in zip(eids, records)
                ],
                ordered=False,
            )
            self.log.debug("Updated master records of %s (%s).", etype, len(eids))
            for error in res.bulk_api_result.get("writeErrors", []):
                self.log.error("Error in bulk write: %s", error)
        except Exception as e:
            raise DatabaseError(f"Update of master records failed: {e}\n{records}") from e

    def extend_ttl(self, etype: str, eid: AnyEidT, ttl_tokens: dict[str, datetime]):
        """Extends TTL of given `etype`:`eid` by `ttl_tokens`."""
        extensions = {
            f"#ttl.{token_name}": token_value for token_name, token_value in ttl_tokens.items()
        }
        with self._master_buffer_locks[etype]:
            buf = self._master_buffers[etype]
            if eid in buf:
                if "$max" in buf[eid]:
                    for ttl_name, this_val in extensions.items():
                        curr_val = buf[eid]["$max"].get(ttl_name, datetime.min)
                        buf[eid]["$max"][ttl_name] = max(curr_val, this_val)
                else:
                    self._master_buffers[etype][eid]["$max"] = extensions
            else:
                self._master_buffers[etype][eid] = {"$max": extensions}

    def remove_expired_ttls(self, etype: str, expired_eid_ttls: dict[AnyEidT, list[str]]):
        """Removes expired TTL of given `etype`:`eid`."""
        master_col = self._master_col(etype)
        try:
            res = master_col.bulk_write(
                [
                    UpdateOne(
                        {"_id": eid},
                        {"$unset": {f"#ttl.{token_name}": "" for token_name in expired_ttls}},
                    )
                    for eid, expired_ttls in expired_eid_ttls.items()
                ]
            )
            self.log.debug(
                "Removed expired TTL of %s: (%s, modified %s).",
                etype,
                len(expired_eid_ttls),
                res.modified_count,
            )
        except Exception as e:
            raise DatabaseError(f"TTL update failed: {e}") from e

    def delete_eids(self, etype: str, eids: list[AnyEidT]):
        """Delete master record and all snapshots of `etype`:`eids`."""
        master_col = self._master_col(etype)
        with self._master_buffer_locks[etype]:
            for eid in eids:
                if eid in self._master_buffers[etype]:
                    del self._master_buffers[etype][eid]
        try:
            res = master_col.delete_many({"_id": {"$in": eids}})
            self.log.debug(
                "Deleted %s master records of %s (%s).", res.deleted_count, etype, len(eids)
            )
            self.elog.log("record_removed", count=res.deleted_count)
        except Exception as e:
            raise DatabaseError(f"Delete of master record failed: {e}\n{eids}") from e
        self.snapshots.delete_eids(etype, eids)

        for f in self._on_entity_delete_many:
            try:
                f(etype, eids)
            except Exception as e:
                self.log.exception("Error in on_entity_delete_many callback %s: %s", f, e)

    def delete_eid(self, etype: str, eid: AnyEidT):
        """Delete master record and all snapshots of `etype`:`eid`."""
        master_col = self._master_col(etype)
        with self._master_buffer_locks[etype]:
            if eid in self._master_buffers[etype]:
                del self._master_buffers[etype][eid]
        try:
            master_col.delete_one({"_id": eid})
            self.log.debug("Deleted master record of %s/%s.", etype, eid)
            self.elog.log("record_removed")
        except Exception as e:
            raise DatabaseError(f"Delete of master record failed: {e}\n{eid}") from e
        self.snapshots.delete_eid(etype, eid)

        for f in self._on_entity_delete_one:
            try:
                f(etype, eid)
            except Exception as e:
                self.log.exception("Error in on_entity_delete_one callback %s: %s", f, e)

    def mark_all_entity_dps_t2(self, etype: str, attrs: list[str]) -> UpdateResult:
        """
        Updates the `min_t2s` of the master records of `etype` for all records.

        Periodically called for all `etype`s from HistoryManager.
        """
        master_col = self._master_col(etype)
        try:
            return master_col.update_many(
                {},
                [
                    {
                        "$set": {
                            attr_name: {
                                "$cond": {
                                    "if": {
                                        "$eq": [
                                            {"$size": {"$ifNull": [f"${attr_name}", []]}},
                                            0,
                                        ]
                                    },
                                    "then": "$$REMOVE",
                                    "else": f"${attr_name}",
                                }
                            }
                            for attr_name in attrs
                        }
                        | {
                            f"#min_t2s.{attr_name}": {
                                "$cond": {
                                    "if": {
                                        "$eq": [
                                            {"$size": {"$ifNull": [f"${attr_name}", []]}},
                                            0,
                                        ]
                                    },
                                    "then": "$$REMOVE",
                                    "else": {"$min": f"${attr_name}.t2"},
                                }
                            }
                            for attr_name in attrs
                        }
                    }
                ],
            )
        except Exception as e:
            raise DatabaseError(f"Update of min_t2s failed: {e}") from e

    def delete_old_dps(self, etype: str, attr_name: str, t_old: datetime) -> UpdateResult:
        """Delete old datapoints from master collection.

        Periodically called for all `etype`s from HistoryManager.
        """
        master_col = self._master_col(etype, write_concern=WriteConcern(w=1))
        try:
            return master_col.update_many(
                {f"#min_t2s.{attr_name}": {"$lt": t_old}},
                [
                    {
                        "$set": {
                            attr_name: {
                                "$filter": {
                                    "input": f"${attr_name}",
                                    "cond": {"$gte": ["$$this.t2", t_old]},
                                }
                            }
                        }
                    },
                    {
                        "$set": {
                            f"#min_t2s.{attr_name}": {
                                "$cond": {
                                    "if": {
                                        "$eq": [
                                            {"$size": {"$ifNull": [f"${attr_name}", []]}},
                                            0,
                                        ]
                                    },
                                    "then": "$$REMOVE",
                                    "else": {"$min": f"${attr_name}.t2"},
                                }
                            }
                        },
                    },
                ],
            )
        except Exception as e:
            raise DatabaseError(f"Delete of old datapoints failed: {e}") from e

    def delete_link_dps(
        self, etype: str, affected_eids: list[AnyEidT], attr_name: str, eid_to: AnyEidT
    ) -> None:
        """Delete link datapoints from master collection.

        Called from LinkManager for deleted entities.
        """
        master_col = self._master_col(etype)
        attr_type = self._db_schema_config.attr(etype, attr_name).t
        filter_cond = {"_id": {"$in": affected_eids}}
        try:
            if attr_type == AttrType.OBSERVATIONS:
                update_pull = {"$pull": {attr_name: {"v.eid": eid_to}}}
                master_col.update_many(filter_cond, update_pull)
            elif attr_type == AttrType.PLAIN:
                update_unset = {"$unset": {attr_name: ""}}
                master_col.update_many(filter_cond, update_unset)
            else:
                raise ValueError(f"Unsupported attribute type: {attr_type}")
        except Exception as e:
            raise DatabaseError(f"Delete of link datapoints failed: {e}") from e

    def delete_many_link_dps(
        self,
        etypes: list[str],
        affected_eids: list[list[AnyEidT]],
        attr_names: list[str],
        eids_to: list[list[AnyEidT]],
    ) -> None:
        """Delete link datapoints from master collection.

        Called from LinkManager for deleted entities, when deleting multiple entities.
        """
        try:
            updates = []
            for etype, affected_eid_list, attr_name, eid_to_list in zip(
                etypes, affected_eids, attr_names, eids_to
            ):
                master_col = self._master_col(etype)
                attr_type = self._db_schema_config.attr(etype, attr_name).t
                filter_cond = {"_id": {"$in": affected_eid_list}}
                if attr_type == AttrType.OBSERVATIONS:
                    update_pull = {"$pull": {attr_name: {"v.eid": {"$in": eid_to_list}}}}
                    updates.append(UpdateMany(filter_cond, update_pull))
                elif attr_type == AttrType.PLAIN:
                    update_unset = {"$unset": {attr_name: ""}}
                    updates.append(UpdateMany(filter_cond, update_unset))
                else:
                    raise ValueError(f"Unsupported attribute type: {attr_type}")
                master_col.bulk_write(updates)
        except Exception as e:
            raise DatabaseError(f"Delete of link datapoints failed: {e}") from e

    def get_master_record(self, etype: str, eid: AnyEidT, **kwargs) -> dict:
        """Get current master record for etype/eid.

        If doesn't exist, returns {}.
        """
        self._assert_etype_exists(etype)
        self._assert_eid_correct_dtype(etype, eid)

        master_col = self._master_col(etype)

        return master_col.find_one({"_id": eid}, **kwargs) or {}

    def ekey_exists(self, etype: str, eid: AnyEidT) -> bool:
        """Checks whether master record for etype/eid exists"""
        with self._master_buffer_locks[etype]:
            in_cache = eid in self._master_buffers[etype]
        return in_cache or bool(self.get_master_record(etype, eid, projection={"_id": 1}))

    def get_master_records(self, etype: str, **kwargs) -> Cursor:
        """Get cursor to current master records of etype."""
        self._assert_etype_exists(etype)

        master_col = self._master_col(etype)
        return master_col.find({}, **kwargs)

    def get_worker_master_records(
        self, worker_index: int, worker_cnt: int, etype: str, query_filter: dict = None, **kwargs
    ) -> Cursor:
        """Get cursor to current master records of etype."""
        if etype not in self._db_schema_config.entities:
            raise DatabaseError(f"Entity '{etype}' does not exist")

        query_filter = {} if query_filter is None else query_filter
        master_col = self._master_col(etype)
        return master_col.find(
            {"#hash": {"$mod": [worker_cnt, worker_index]}, **query_filter}, **kwargs
        )

    def get_value_or_history(
        self,
        etype: str,
        attr_name: str,
        eid: AnyEidT,
        t1: Optional[datetime] = None,
        t2: Optional[datetime] = None,
    ) -> dict:
        """Gets current value and/or history of attribute for given `eid`.

        Depends on attribute type:
        - plain: just (current) value
        - observations: (current) value and history stored in master record (optionally filtered)
        - timeseries: just history stored in master record (optionally filtered)

        Returns dict with two keys: `current_value` and `history` (list of values).
        """
        self._assert_etype_exists(etype)
        self._assert_eid_correct_dtype(etype, eid)

        attr_spec = self._db_schema_config.attr(etype, attr_name)

        result = {"current_value": None, "history": []}

        # Add current value to the result
        if attr_spec.t == AttrType.PLAIN:
            result["current_value"] = (
                self.get_master_record(etype, eid).get(attr_name, {}).get("v", None)
            )
        elif attr_spec.t == AttrType.OBSERVATIONS:
            result["current_value"] = self.snapshots.get_latest_one(etype, eid).get(attr_name, None)

        # Add history
        if attr_spec.t == AttrType.OBSERVATIONS:
            result["history"] = self.get_observation_history(etype, attr_name, eid, t1, t2)
        elif attr_spec.t == AttrType.TIMESERIES:
            result["history"] = self.get_timeseries_history(etype, attr_name, eid, t1, t2)

        return result

    def estimate_count_eids(self, etype: str) -> int:
        """Estimates count of `eid`s in given `etype`"""
        self._assert_etype_exists(etype)

        master_col = self._master_col(etype)
        return master_col.estimated_document_count({})

    def _get_metadata_id(self, module: str, time: datetime, worker_id: Optional[int] = None) -> str:
        """Generates unique metadata id based on `module`, `time` and the worker index."""
        worker_id = self._process_index if worker_id is None else worker_id
        return f"{module}{time.strftime('%Y-%m-%dT%H:%M:%S.%fZ')}w{worker_id}"

    def save_metadata(self, time: datetime, metadata: dict, worker_id: Optional[int] = None):
        """Saves metadata dict under the caller module and passed timestamp."""
        module = get_caller_id()
        metadata["_id"] = self._get_metadata_id(module, time, worker_id)
        metadata["#module"] = module
        metadata["#time_created"] = time
        metadata["#last_update"] = datetime.now()
        try:
            self._db["#metadata"].insert_one(metadata)
            self.log.debug("Inserted metadata %s: %s", metadata["_id"], metadata)
        except Exception as e:
            raise DatabaseError(f"Insert of metadata failed: {e}\n{metadata}") from e

    def update_metadata(
        self, time: datetime, metadata: dict, increase: dict = None, worker_id: Optional[int] = None
    ):
        """Updates existing metadata of caller module and passed timestamp."""
        module = get_caller_id()
        metadata_id = self._get_metadata_id(module, time, worker_id)
        metadata["#last_update"] = datetime.now()

        changes = {"$set": metadata} if increase is None else {"$set": metadata, "$inc": increase}

        try:
            res = self._db["#metadata"].update_one({"_id": metadata_id}, changes, upsert=True)
            self.log.debug(
                "Updated metadata %s, changes: %s, result: %s", metadata_id, changes, res.raw_result
            )
        except Exception as e:
            raise DatabaseError(f"Update of metadata failed: {e}\n{metadata_id}, {changes}") from e

    def get_observation_history(
        self,
        etype: str,
        attr_name: str,
        eid: AnyEidT,
        t1: datetime = None,
        t2: datetime = None,
        sort: int = None,
    ) -> list[dict]:
        """Get full (or filtered) history of observation attribute.

        This method is useful for displaying `eid`'s history on web.
        Also used to feed data into `get_timeseries_history()`.

        Args:
            etype: entity type
            attr_name: name of attribute
            eid: id of entity, to which data-points correspond
            t1: left value of time interval (inclusive)
            t2: right value of time interval (inclusive)
            sort: sort by timestamps - 0: ascending order by t1, 1: descending order by t2,
                None: don't sort
        Returns:
            list of dicts (reduced datapoints)
        """
        t1 = datetime.fromtimestamp(0) if t1 is None else t1.replace(tzinfo=None)
        t2 = datetime.now() if t2 is None else t2.replace(tzinfo=None)

        # Get attribute history
        mr = self.get_master_record(etype, eid)
        attr_history = mr.get(attr_name, [])

        # Filter
        attr_history_filtered = [row for row in attr_history if row["t1"] <= t2 and row["t2"] >= t1]

        # Sort
        if sort == 1:
            attr_history_filtered.sort(key=lambda row: row["t1"])
        elif sort == 2:
            attr_history_filtered.sort(key=lambda row: row["t2"], reverse=True)

        return attr_history_filtered

    def get_timeseries_history(
        self,
        etype: str,
        attr_name: str,
        eid: AnyEidT,
        t1: datetime = None,
        t2: datetime = None,
        sort: int = None,
    ) -> list[dict]:
        """Get full (or filtered) history of timeseries attribute.
        Outputs them in format:
        ```
            [
                {
                    "t1": ...,
                    "t2": ...,
                    "v": {
                        "series1": ...,
                        "series2": ...
                    }
                },
                ...
            ]
        ```
        This method is useful for displaying `eid`'s history on web.

        Args:
            etype: entity type
            attr_name: name of attribute
            eid: id of entity, to which data-points correspond
            t1: left value of time interval (inclusive)
            t2: right value of time interval (inclusive)
            sort: sort by timestamps - `0`: ascending order by `t1`, `1`: descending order by `t2`,
                `None`: don't sort
        Returns:
             list of dicts (reduced datapoints) - each represents just one point at time
        """
        t1 = datetime.fromtimestamp(0) if t1 is None else t1.replace(tzinfo=None)
        t2 = datetime.now() if t2 is None else t2.replace(tzinfo=None)

        attr_history = self.get_observation_history(etype, attr_name, eid, t1, t2, sort)
        if not attr_history:
            return []

        attr_history_split = self._split_timeseries_dps(etype, attr_name, attr_history)

        # Filter out rows outside [t1, t2] interval
        attr_history_filtered = [
            row for row in attr_history_split if row["t1"] <= t2 and row["t2"] >= t1
        ]

        return attr_history_filtered

    def _split_timeseries_dps(
        self, etype: str, attr_name: str, attr_history: list[dict]
    ) -> list[dict]:
        """Helper to split "datapoints" (rows) of timeseries to "datapoints"
        containing just one value per series."""
        attrib_conf = self._db_schema_config.attr(etype, attr_name)
        timeseries_type = attrib_conf.timeseries_type

        result = []

        # Should be ["time"] or ["time_first", "time_last"] for irregular
        # timeseries and [] for regular timeseries
        time_series = list(timeseries_types[timeseries_type]["default_series"].keys())

        # User-defined series
        user_series = list(set(attrib_conf.series.keys()) - set(time_series))

        if not user_series:
            return []

        if timeseries_type == "regular":
            time_step = attrib_conf.timeseries_params.time_step

            for row in attr_history:
                # Length of all series arrays/lists in this row
                values_len = len(row["v"][user_series[0]])

                for i in range(values_len):
                    row_t1 = row["t1"] + i * time_step
                    row_t2 = row_t1 + time_step

                    row_new = {"t1": row_t1, "t2": row_t2, "v": {}}

                    for s in user_series:
                        row_new["v"][s] = row["v"][s][i]

                    result.append(row_new)
        else:
            for row in attr_history:
                # Length of all series arrays/lists in this row
                values_len = len(row["v"][user_series[0]])

                for i in range(values_len):
                    row_t1 = row["v"].get(time_series[0])[i] if len(time_series) >= 1 else None
                    row_t2 = row["v"].get(time_series[1])[i] if len(time_series) >= 2 else row_t1

                    row_new = {"t1": row_t1, "t2": row_t2, "v": {}}

                    for s in user_series:
                        row_new["v"][s] = row["v"][s][i]

                    result.append(row_new)

        return result

    def move_raw_to_archive(self, etype: str):
        """Rename the current raw collection to archive collection.

        Multiple archive collections can exist for one entity type,
        though they are exported and dropped over time.
        """
        raw_col_name = self._raw_col_name(etype)
        archive_col_name = self._get_new_archive_col_name(etype)
        try:
            if self._db.list_collection_names(filter={"name": raw_col_name}):
                self._db[raw_col_name].rename(archive_col_name)
                return archive_col_name
            return None
        except Exception as e:
            raise DatabaseError(f"Move of raw collection failed: {e}") from e

    def get_archive_summary(self, etype: str, before: datetime) -> Optional[dict]:
        collection_summaries = []
        for archive_col_name in self._archive_col_names(etype):
            result_cursor = self._get_archive_summary(archive_col_name, before=before)
            for summary in result_cursor:
                collection_summaries.append(summary)
        if not collection_summaries:
            return None
        min_date = min(x["earliest"] for x in collection_summaries)
        max_date = max(x["latest"] for x in collection_summaries)
        total_dps = sum(x["count"] for x in collection_summaries)
        return {"earliest": min_date, "latest": max_date, "count": total_dps}

    def _get_archive_summary(self, archive_col_name: str, before: datetime) -> CommandCursor:
        try:
            return self._db[archive_col_name].aggregate(
                [
                    {"$match": {"t1": {"$lt": before}}},
                    {
                        "$group": {
                            "_id": "date_range",
                            "earliest": {"$min": "$t1"},
                            "latest": {"$max": "$t1"},
                            "count": {"$sum": 1},
                        }
                    },
                ]
            )
        except Exception as e:
            raise DatabaseError(f"Archive collection {archive_col_name} fetch failed: {e}") from e

    def get_archive(self, etype: str, after: datetime, before: datetime) -> Iterator[Cursor]:
        """Get archived raw datapoints where `t1` is in <`after`, `before`).

        All plain datapoints will be returned (default).
        """
        query_filter = {"$or": [{"t1": {"$gte": after, "$lt": before}}, {"t1": None}]}
        for archive_col in self._archive_cols(etype):
            try:
                yield archive_col.find(query_filter)
            except Exception as e:
                raise DatabaseError(f"Archive collection {archive_col} fetch failed: {e}") from e

    def delete_old_archived_dps(self, etype: str, before: datetime) -> Iterator[DeleteResult]:
        """Delete archived raw datapoints older than `before`.

        Deletes all plain datapoints if `plain` is `True` (default).
        """
        query_filter = {"$or": [{"t1": {"$lt": before}}, {"t1": None}]}
        for archive_col in self._archive_cols(etype):
            try:
                yield archive_col.delete_many(query_filter)
            except Exception as e:
                raise DatabaseError(f"Delete of old datapoints failed: {e}") from e

    def drop_empty_archives(self, etype: str) -> int:
        """Drop empty archive collections."""
        dropped_count = 0
        for col in self._archive_cols(etype):
            try:
                if col.estimated_document_count() < 1000 and col.count_documents({}) == 0:
                    col.drop()
                    dropped_count += 1
            except Exception as e:
                raise DatabaseError(f"Drop of empty archive failed: {e}") from e
        return dropped_count

    def get_module_cache(self, override_called_id: Optional[str] = None):
        """Return a persistent cache collection for given module name.

        Module name is determined automatically, but you can override it.
        """
        module = override_called_id or get_caller_id()
        self.log.debug("Cache collection access: %s", module)
        return self._db[f"#cache#{module}"]

    def get_estimated_entity_count(self, entity_type: str) -> int:
        """Get count of entities of given type."""
        return self._master_col(entity_type).estimated_document_count()
