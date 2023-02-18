import logging
import urllib
from datetime import datetime

import pymongo

from dp3.common.attrspec import AttrType, timeseries_types
from dp3.common.config import HierarchicalDict, ModelSpec
from dp3.common.datapoint import DataPointBase


class DatabaseError(Exception):
    pass


class MissingTableError(DatabaseError):
    pass


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

        connection_conf = db_conf.get("connection", {})
        username = urllib.parse.quote_plus(connection_conf.get("username", "dp3"))
        password = urllib.parse.quote_plus(connection_conf.get("password", "dp3"))
        address = connection_conf.get("address", "localhost")
        port = str(connection_conf.get("port", 27017))
        db_name = connection_conf.get("db_name", "dp3")

        self.log.info("Connecting to database...")
        self._db = pymongo.MongoClient(
            f"mongodb://{username}:{password}@{address}:{port}/",
            connectTimeoutMS=3000,
            serverSelectionTimeoutMS=5000,
        )

        # Check if connected
        try:
            self._db.admin.command("ping")
        except pymongo.errors.ConnectionFailure as e:
            raise DatabaseError(
                "Cannot connect to database with specified connection arguments."
            ) from e

        self._db_schema_config = model_spec

        # Init and switch to correct database
        self._init_database_schema(db_name)
        self._db = self._db[db_name]

        self.log.info("Database successfully initialized!")

    def _init_database_schema(self, db_name) -> None:
        """Runs full check and update of database schema.

        Checks whether `db_name` database exists. If not, creates it.
        Checks whether all collections exist. If not, creates them.

        This all is done automatically in MongoDB, so this function doesn't do
        anything.
        """

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

    def insert_datapoints(self, etype: str, ekey: str, dps: list[DataPointBase]) -> None:
        """Inserts datapoint to raw data collection and updates master record.

        Raises DatabaseError when insert or update fails.
        """
        if len(dps) == 0:
            return

        etype = dps[0].etype

        if etype not in self._db_schema_config.entities:
            raise DatabaseError(f"Entity '{etype}' does not exist")

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
            # Rewrite value of plain attribute
            if attr_spec.t == AttrType.PLAIN:
                master_changes["$set"][dp.attr] = {"v": dp.v, "ts_last_update": datetime.now()}

            # Push new data of observation
            if attr_spec.t == AttrType.OBSERVATIONS:
                master_changes["$push"][dp.attr] = {"t1": dp.t1, "t2": dp.t2, "v": dp.v, "c": dp.c}

            # Push new data of timeseries
            if attr_spec.t == AttrType.TIMESERIES:
                master_changes["$push"][dp.attr] = {"t1": dp.t1, "t2": dp.t2, "v": dp.v.dict()}

        master_col = self._master_col_name(etype)
        try:
            self._db[master_col].update_one({"_id": ekey}, master_changes, upsert=True)
            self.log.debug(f"Updated master record of {etype} {ekey}: {master_changes}")
        except Exception as e:
            raise DatabaseError(f"Update of master record failed: {e}\n{dps}") from e

    def delete_old_dps(self, etype: str, attr_name: str, t_old: datetime) -> None:
        """Delete old datapoints from master collection.

        Periodically called for all `etype`s from HistoryManager.
        """
        master_col = self._master_col_name(etype)
        try:
            self._db[master_col].update_many({}, {"$pull": {attr_name: {"t2": {"$lt": t_old}}}})
        except Exception as e:
            raise DatabaseError(f"Delete of old datapoints failed: {e}") from e

    def get_master_record(self, etype: str, ekey: str) -> dict:
        """Get current master record for etype/ekey.

        If doesn't exist, returns {}.
        """
        if etype not in self._db_schema_config.entities:
            raise DatabaseError(f"Entity '{etype}' does not exist")

        master_col = self._master_col_name(etype)
        return self._db[master_col].find_one({"_id": ekey}) or {}

    def ekey_exists(self, etype: str, ekey: str) -> bool:
        """Checks whether master record for etype/ekey exists"""
        return bool(self.get_master_record(etype, ekey))

    def get_master_records(self, etype: str) -> pymongo.cursor.Cursor:
        """Get cursor to current master records of etype."""
        if etype not in self._db_schema_config.entities:
            raise DatabaseError(f"Entity '{etype}' does not exist")

        master_col = self._master_col_name(etype)
        return self._db[master_col].find({})

    def save_snapshot(self, etype: str, snapshot: dict):
        """Saves snapshot to specified entity of current master document."""
        if etype not in self._db_schema_config.entities:
            raise DatabaseError(f"Entity '{etype}' does not exist")

        snapshot["_time_created"] = datetime.now()

        snapshot_col = self._snapshots_col_name(etype)
        try:
            self._db[snapshot_col].insert_one(snapshot)
            self.log.debug(f"Inserted snapshot:\n{snapshot}")
        except Exception as e:
            raise DatabaseError(f"Insert of snaphsot failed: {e}\n{snapshot}") from e

    def get_latest_snapshot(self):
        """Get latest snapshot of given `ekey`.

        This method is useful for displaying data on web.
        """

    def get_snapshots(self):
        """Get all (or filtered) snapshots of given `ekey`.

        This method is useful for displaying `ekey`'s history on web.
        """

    def get_observation_history(
        self,
        etype: str,
        attr_name: str,
        ekey: str,
        t1: datetime = None,
        t2: datetime = None,
        sort: int = None,
    ) -> list[dict]:
        """Get full (or filtered) history of observation attribute.

        This method is useful for displaying `ekey`'s history on web.
        Also used to feed data into `get_timeseries_history()`.

        Args:
            etype: entity type
            attr_name: name of attribute
            ekey: id of entity, to which data-points correspond
            t1: left value of time interval (inclusive)
            t2: right value of time interval (inclusive)
            sort: sort by timestamps - 0: ascending order by t1, 1: descending order by t2,
                None: don't sort
        Returns:
            list of dicts (reduced datapoints)
        """
        t1 = datetime.fromtimestamp(0) if t1 is None else t1
        t2 = datetime.now() if t2 is None else t2

        # Get attribute history
        mr = self.get_master_record(etype, ekey)
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
        ekey: str,
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
        This method is useful for displaying `ekey`'s history on web.

        Args:
            etype: entity type
            attr_name: name of attribute
            ekey: id of entity, to which data-points correspond
            t1: left value of time interval (inclusive)
            t2: right value of time interval (inclusive)
            sort: sort by timestamps - `0`: ascending order by `t1`, `1`: descending order by `t2`,
                `None`: don't sort
        Returns:
             list of dicts (reduced datapoints) - each represents just one point at time
        """
        t1 = datetime.fromtimestamp(0) if t1 is None else t1
        t2 = datetime.now() if t2 is None else t2

        attr_history = self.get_observation_history(etype, attr_name, ekey, t1, t2, sort)
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
