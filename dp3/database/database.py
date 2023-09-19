import inspect
import logging
import time
import urllib
from datetime import datetime
from typing import Literal, Optional, Union

import pymongo
from pydantic import BaseModel, Field, validator
from pymongo import ReplaceOne
from pymongo.errors import OperationFailure

from dp3.common.attrspec import AttrType, timeseries_types
from dp3.common.config import HierarchicalDict, ModelSpec
from dp3.common.datapoint import DataPointBase
from dp3.task_processing.task_queue import HASH


class DatabaseError(Exception):
    pass


class MissingTableError(DatabaseError):
    pass


class MongoHostConfig(BaseModel, extra="forbid"):
    """MongoDB host."""

    address: str = "localhost"
    port: int = 27017


class MongoStandaloneConfig(BaseModel, extra="forbid"):
    """MongoDB standalone configuration."""

    mode: Literal["standalone"]
    host: MongoHostConfig = MongoHostConfig()


class MongoReplicaConfig(BaseModel, extra="forbid"):
    """MongoDB replica set configuration."""

    mode: Literal["replica"]
    replica_set: str = "dp3"
    hosts: list[MongoHostConfig]


class MongoConfig(BaseModel, extra="forbid"):
    """Database configuration."""

    db_name: str = "dp3"
    username: str = "dp3"
    password: str = "dp3"
    connection: Union[MongoStandaloneConfig, MongoReplicaConfig] = Field(..., discriminator="mode")

    @validator("username", "password")
    def url_safety(cls, v):
        return urllib.parse.quote_plus(v)


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

    db_conf - configuration of database connection (content of database.yml)
    model_spec - ModelSpec object, configuration of data model (entities and attributes)
    """

    def __init__(
        self,
        db_conf: HierarchicalDict,
        model_spec: ModelSpec,
    ) -> None:
        self.log = logging.getLogger("EntityDatabase")

        config = MongoConfig.parse_obj(db_conf)

        self.log.info("Connecting to database...")
        for attempt, delay in enumerate(RECONNECT_DELAYS):
            try:
                self._db = self.connect(config)
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

        # Init and switch to correct database
        self._db = self._db[config.db_name]
        self._init_database_schema(config.db_name)

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

    @staticmethod
    def _master_col_name(entity: str) -> str:
        """Returns name of master collection for `entity`."""
        return f"{entity}#master"

    @staticmethod
    def _snapshots_col_name(entity: str) -> str:
        """Returns name of snapshots collection for `entity`."""
        return f"{entity}#snapshots"

    @staticmethod
    def _raw_col_name(entity: str) -> str:
        """Returns name of raw data collection for `entity`."""
        return f"{entity}#raw"

    def _init_database_schema(self, db_name) -> None:
        """Runs full check and update of database schema.

        Checks whether `db_name` database exists. If not, creates it.
        Checks whether all collections exist. If not, creates them.
        This all is done automatically in MongoDB.

        We need to create indexes, however.
        """
        for etype in self._db_schema_config.entities:
            # Snapshots index on `eid` and `_time_created` fields
            snapshot_col = self._snapshots_col_name(etype)
            self._db[snapshot_col].create_index("eid", background=True)
            self._db[snapshot_col].create_index("_time_created", background=True)

            # Existence indexes for each attribute in master collection
            master_col = self._master_col_name(etype)
            for attr, _spec in self._db_schema_config.entity_attributes[etype].items():
                self._db[master_col].create_index(
                    attr, background=True, partialFilterExpression={attr: {"$exists": True}}
                )

    def _assert_etype_exists(self, etype: str):
        """Asserts `etype` existence.

        If doesn't exist, raises `DatabaseError`.
        """
        if etype not in self._db_schema_config.entities:
            raise DatabaseError(f"Entity '{etype}' does not exist")

    def insert_datapoints(
        self, etype: str, eid: str, dps: list[DataPointBase], new_entity: bool = False
    ) -> None:
        """Inserts datapoint to raw data collection and updates master record.

        Raises DatabaseError when insert or update fails.
        """
        if len(dps) == 0:
            return

        etype = dps[0].etype

        # Check `etype`
        self._assert_etype_exists(etype)

        # Insert raw datapoints
        raw_col = self._raw_col_name(etype)
        dps_dicts = [dp.dict(exclude={"attr_type"}) for dp in dps]
        try:
            self._db[raw_col].insert_many(dps_dicts)
            self.log.debug(f"Inserted datapoints to raw collection:\n{dps}")
        except Exception as e:
            raise DatabaseError(f"Insert of datapoints failed: {e}\n{dps}") from e

        # Update master document
        master_changes = {"$push": {}, "$set": {}}
        for dp in dps:
            attr_spec = self._db_schema_config.attr(etype, dp.attr)

            v = dp.v.dict() if isinstance(dp.v, BaseModel) else dp.v

            # Rewrite value of plain attribute
            if attr_spec.t == AttrType.PLAIN:
                master_changes["$set"][dp.attr] = {"v": v, "ts_last_update": datetime.now()}

            # Push new data of observation
            if attr_spec.t == AttrType.OBSERVATIONS:
                if dp.attr in master_changes["$push"]:
                    # Support multiple datapoints being pushed in the same request
                    if "$each" not in master_changes["$push"][dp.attr]:
                        saved_dp = master_changes["$push"][dp.attr]
                        master_changes["$push"][dp.attr] = {"$each": [saved_dp]}
                    master_changes["$push"][dp.attr]["$each"].append(
                        {"t1": dp.t1, "t2": dp.t2, "v": v, "c": dp.c}
                    )
                else:
                    # Otherwise just push one datapoint
                    master_changes["$push"][dp.attr] = {"t1": dp.t1, "t2": dp.t2, "v": v, "c": dp.c}

            # Push new data of timeseries
            if attr_spec.t == AttrType.TIMESERIES:
                if dp.attr in master_changes["$push"]:
                    # Support multiple datapoints being pushed in the same request
                    if "$each" not in master_changes["$push"][dp.attr]:
                        saved_dp = master_changes["$push"][dp.attr]
                        master_changes["$push"][dp.attr] = {"$each": [saved_dp]}
                    master_changes["$push"][dp.attr]["$each"].append(
                        {"t1": dp.t1, "t2": dp.t2, "v": v}
                    )
                else:
                    # Otherwise just push one datapoint
                    master_changes["$push"][dp.attr] = {"t1": dp.t1, "t2": dp.t2, "v": v}

        if new_entity:
            master_changes["$set"]["#hash"] = HASH(f"{etype}:{eid}")

        master_col = self._master_col_name(etype)
        try:
            self._db[master_col].update_one({"_id": eid}, master_changes, upsert=True)
            self.log.debug(f"Updated master record of {etype} {eid}: {master_changes}")
        except Exception as e:
            raise DatabaseError(f"Update of master record failed: {e}\n{dps}") from e

    def update_master_records(self, etype: str, eids: list[str], records: list[dict]) -> None:
        """Replace master records of `etype`:`eid` with the provided `records`.

        Raises DatabaseError when update fails.
        """
        master_col = self._master_col_name(etype)
        try:
            self._db[master_col].bulk_write(
                [
                    ReplaceOne({"_id": eid}, record, upsert=True)
                    for eid, record in zip(eids, records)
                ]
            )
            self.log.debug("Updated master records of %s: %s.", eids, eids)
        except Exception as e:
            raise DatabaseError(f"Update of master records failed: {e}\n{records}") from e

    def delete_old_dps(self, etype: str, attr_name: str, t_old: datetime) -> None:
        """Delete old datapoints from master collection.

        Periodically called for all `etype`s from HistoryManager.
        """
        master_col = self._master_col_name(etype)
        try:
            self._db[master_col].update_many({}, {"$pull": {attr_name: {"t2": {"$lt": t_old}}}})
        except Exception as e:
            raise DatabaseError(f"Delete of old datapoints failed: {e}") from e

    def get_master_record(self, etype: str, eid: str, **kwargs) -> dict:
        """Get current master record for etype/eid.

        If doesn't exist, returns {}.
        """
        # Check `etype`
        self._assert_etype_exists(etype)

        master_col = self._master_col_name(etype)
        return self._db[master_col].find_one({"_id": eid}, **kwargs) or {}

    def ekey_exists(self, etype: str, eid: str) -> bool:
        """Checks whether master record for etype/eid exists"""
        return bool(self.get_master_record(etype, eid))

    def get_master_records(self, etype: str, **kwargs) -> pymongo.cursor.Cursor:
        """Get cursor to current master records of etype."""
        # Check `etype`
        self._assert_etype_exists(etype)

        master_col = self._master_col_name(etype)
        return self._db[master_col].find({}, **kwargs)

    def get_worker_master_records(
        self, worker_index: int, worker_cnt: int, etype: str, **kwargs
    ) -> pymongo.cursor.Cursor:
        """Get cursor to current master records of etype."""
        if etype not in self._db_schema_config.entities:
            raise DatabaseError(f"Entity '{etype}' does not exist")

        master_col = self._master_col_name(etype)
        return self._db[master_col].find({"#hash": {"$mod": [worker_cnt, worker_index]}}, **kwargs)

    def get_latest_snapshot(self, etype: str, eid: str) -> dict:
        """Get latest snapshot of given etype/eid.

        If doesn't exist, returns {}.
        """
        # Check `etype`
        self._assert_etype_exists(etype)

        snapshot_col = self._snapshots_col_name(etype)
        return self._db[snapshot_col].find_one({"eid": eid}, sort=[("_id", -1)]) or {}

    def get_latest_snapshots(
        self, etype: str, eid_filter: str = ""
    ) -> tuple[pymongo.cursor.Cursor, int]:
        """Get latest snapshots of given `etype`.

        This method is useful for displaying data on web.

        If `eid_filter` is not empty, returns only `eid`s containing substring `eid_filter`.

        Also returns total document count (after filtering).

        May raise `DatabaseError` if query is invalid.
        """
        # Check `etype`
        self._assert_etype_exists(etype)

        snapshot_col = self._snapshots_col_name(etype)
        latest_snapshot = self._db[snapshot_col].find_one({}, sort=[("_id", -1)])
        if latest_snapshot is None:
            return self._db[snapshot_col].find(), self._db[snapshot_col].count_documents({})

        latest_snapshot_date = latest_snapshot["_time_created"]
        query = {"_time_created": latest_snapshot_date}
        if eid_filter != "":
            query["eid"] = {"$regex": eid_filter}

        try:
            return self._db[snapshot_col].find(query), self._db[snapshot_col].count_documents(query)
        except OperationFailure as e:
            raise DatabaseError("Invalid query") from e

    def get_snapshots(
        self, etype: str, eid: str, t1: Optional[datetime] = None, t2: Optional[datetime] = None
    ) -> pymongo.cursor.Cursor:
        """Get all (or filtered) snapshots of given `eid`.

        This method is useful for displaying `eid`'s history on web.

        Args:
            etype: entity type
            eid: id of entity, to which data-points correspond
            t1: left value of time interval (inclusive)
            t2: right value of time interval (inclusive)
        """
        # Check `etype`
        self._assert_etype_exists(etype)

        snapshot_col = self._snapshots_col_name(etype)
        query = {"eid": eid, "_time_created": {}}

        # Filter by date
        if t1:
            query["_time_created"]["$gte"] = t1
        if t2:
            query["_time_created"]["$lte"] = t2

        # Unset if empty
        if not query["_time_created"]:
            del query["_time_created"]

        return self._db[snapshot_col].find(query).sort([("_time_created", pymongo.ASCENDING)])

    def get_value_or_history(
        self,
        etype: str,
        attr_name: str,
        eid: str,
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
        # Check `etype`
        self._assert_etype_exists(etype)

        attr_spec = self._db_schema_config.attr(etype, attr_name)

        result = {"current_value": None, "history": []}

        # Add current value to the result
        if attr_spec.t == AttrType.PLAIN:
            result["current_value"] = (
                self.get_master_record(etype, eid).get(attr_name, {}).get("v", None)
            )
        elif attr_spec.t == AttrType.OBSERVATIONS:
            result["current_value"] = self.get_latest_snapshot(etype, eid).get(attr_name, None)

        # Add history
        if attr_spec.t == AttrType.OBSERVATIONS:
            result["history"] = self.get_observation_history(etype, attr_name, eid, t1, t2)
        elif attr_spec.t == AttrType.TIMESERIES:
            result["history"] = self.get_timeseries_history(etype, attr_name, eid, t1, t2)

        return result

    def estimate_count_eids(self, etype: str) -> int:
        """Estimates count of `eid`s in given `etype`"""
        # Check `etype`
        self._assert_etype_exists(etype)

        master_col = self._master_col_name(etype)
        return self._db[master_col].estimated_document_count({})

    def save_snapshot(self, etype: str, snapshot: dict, time: datetime):
        """Saves snapshot to specified entity of current master document."""
        # Check `etype`
        self._assert_etype_exists(etype)

        snapshot["_time_created"] = time

        snapshot_col = self._snapshots_col_name(etype)
        try:
            self._db[snapshot_col].insert_one(snapshot)
            self.log.debug(f"Inserted snapshot: {snapshot}")
        except Exception as e:
            raise DatabaseError(f"Insert of snapshot failed: {e}\n{snapshot}") from e

    def save_snapshots(self, etype: str, snapshots: list[dict], time: datetime):
        """
        Saves a list of snapshots of current master documents.

        All snapshots must belong to same entity type.
        """
        # Check `etype`
        self._assert_etype_exists(etype)

        for snapshot in snapshots:
            snapshot["_time_created"] = time

        snapshot_col = self._snapshots_col_name(etype)
        try:
            self._db[snapshot_col].insert_many(snapshots)
            self.log.debug(f"Inserted snapshots: {snapshots}")
        except Exception as e:
            raise DatabaseError(f"Insert of snapshots failed: {e}\n{snapshots}") from e

    def save_metadata(self, time: datetime, metadata: dict):
        """Saves snapshot to specified entity of current master document."""
        module = get_caller_id()
        metadata["_id"] = module + time.strftime("%Y-%m-%dT%H:%M:%S.%fZ")[:-4]
        metadata["#module"] = module
        metadata["#time_created"] = time
        metadata["#last_update"] = datetime.now()
        try:
            self._db["#metadata"].insert_one(metadata)
            self.log.debug("Inserted metadata %s: %s", metadata["_id"], metadata)
        except Exception as e:
            raise DatabaseError(f"Insert of metadata failed: {e}\n{metadata}") from e

    def update_metadata(self, time: datetime, metadata: dict, increase: dict = None):
        module = get_caller_id()
        metadata_id = module + time.strftime("%Y-%m-%dT%H:%M:%S.%fZ")[:-4]
        metadata["#last_update"] = datetime.now()

        changes = {"$set": metadata} if increase is None else {"$set": metadata, "$inc": increase}

        try:
            res = self._db["#metadata"].update_one({"_id": metadata_id}, changes)
            self.log.debug(
                "Updated metadata %s, changes: %s, result: %s", metadata_id, changes, res.raw_result
            )
        except Exception as e:
            raise DatabaseError(f"Update of metadata failed: {e}\n{metadata_id}, {changes}") from e

    def get_observation_history(
        self,
        etype: str,
        attr_name: str,
        eid: str,
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
        eid: str,
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

    def get_raw_summary(self, etype: str, before: datetime) -> pymongo.cursor:
        raw_col_name = self._raw_col_name(etype)
        try:
            return self._db[raw_col_name].aggregate(
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
            raise DatabaseError(f"Raw collection {raw_col_name} fetch failed: {e}") from e

    def get_raw(
        self, etype: str, after: datetime, before: datetime, plain: bool = True
    ) -> pymongo.cursor:
        """Get raw datapoints where `t1` is in <`after`, `before`).

        If `plain` is `True`, then all plain datapoints will be returned (default).
        """
        raw_col_name = self._raw_col_name(etype)
        query_filter = {"$or": [{"t1": {"$gte": after, "$lt": before}}]}
        if plain:
            query_filter["$or"].append({"t1": {"$exists": False}})
        return self._db[raw_col_name].find(query_filter)

    def delete_old_raw_dps(self, etype: str, before: datetime, plain: bool = True):
        """Delete raw datapoints older than `before`.

        Deletes all plain datapoints if `plain` is `True` (default).
        """
        raw_col_name = self._raw_col_name(etype)
        query_filter = {"$or": [{"t1": {"$lt": before}}]}
        if plain:
            query_filter["$or"].append({"t1": {"$exists": False}})
        try:
            return self._db[raw_col_name].delete_many(query_filter)
        except Exception as e:
            raise DatabaseError(f"Delete of old datapoints failed: {e}") from e

    def delete_old_snapshots(self, etype: str, t_old: datetime):
        """Delete old snapshots.

        Periodically called for all `etype`s from HistoryManager.
        """
        snapshot_col_name = self._snapshots_col_name(etype)
        try:
            return self._db[snapshot_col_name].delete_many({"_time_created": {"$lt": t_old}})
        except Exception as e:
            raise DatabaseError(f"Delete of olds snapshots failed: {e}") from e

    def get_module_cache(self):
        """Return a persistent cache collection for given module name."""
        module = get_caller_id()
        self.log.debug("Module %s is accessing its cache collection", module)
        return self._db[f"#cache#{module}"]
