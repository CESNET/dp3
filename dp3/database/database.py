from datetime import datetime, timedelta
from typing import Union
import logging
import urllib

import pymongo

from dp3.common.attrspec import AttrSpecGeneric, AttrType
from dp3.common.config import HierarchicalDict
from dp3.common.datapoint import DataPoint
from dp3.common.entityspec import EntitySpec


class DatabaseError(Exception):
    pass


class MissingTableError(DatabaseError):
    pass


class EntityDatabase:
    """
    MongoDB database wrapper responsible for whole communication with database server. Initializes database schema
    based on database configuration.

    db_conf - configuration of database connection (content of database.yml)
    attr_spec - configuration of data model (entities and attributes, result of config.load_attr_spec function)
    """

    def __init__(self, db_conf: HierarchicalDict,
                 attr_spec: dict[str, dict[str, Union[EntitySpec, dict[str, AttrSpecGeneric]]]]) -> None:
        self.log = logging.getLogger("EntityDatabase")
        self.log.setLevel("DEBUG")

        connection_conf = db_conf.get("connection", {})
        username = urllib.parse.quote_plus(connection_conf.get("username", "dp3"))
        password = urllib.parse.quote_plus(connection_conf.get("password", "dp3"))
        address = connection_conf.get("address", "localhost")
        port = str(connection_conf.get("port", 27017))
        db_name = connection_conf.get("db_name", "dp3")

        try:
            self._db = pymongo.MongoClient(f"mongodb://{username}:{password}@{address}:{port}/")
        except pymongo.errors.ConnectionFailure as e:
            raise DatabaseError(f"Cannot connect to database with specified connection arguments.") from e

        self._db_schema_config = attr_spec

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
        pass

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

    def insert_datapoints(self, etype: str, ekey: str, dps: list[DataPoint]) -> None:
        """Inserts datapoint to raw data collection and updates master collection."""
        if len(dps) == 0:
            return

        etype = dps[0].etype

        # Insert raw datapoints
        raw_col = self._raw_col_name(etype)
        dps_dicts = [dp.dict(exclude={"attr_type"}) for dp in dps]
        try:
            self._db[raw_col].insert_many(dps_dicts)
            self.log.debug(f"Inserted datapoints to raw collection:\n{dps}")
        except Exception as e:
            raise DatabaseError(f"Couldn't insert datapoints: {e}\n{dps}") from e

        # Update master document
        master_changes = {"$push": {}, "$set": {}}
        for dp in dps:
            # Rewrite value of plain attribute
            if dp.attr_type == AttrType.PLAIN:
                master_changes["$set"][dp.attr] = {
                    "v": dp.v,
                    "ts_last_update": dp.t1
                }

            # Push new data of observation
            if dp.attr_type == AttrType.OBSERVATIONS:
                master_changes["$push"][dp.attr] = {
                    "t1": dp.t1,
                    "t2": dp.t2,
                    "v": dp.v,
                    "c": dp.c
                }

            # Push new data of timeseries
            if dp.attr_type == AttrType.TIMESERIES:
                master_changes["$push"][dp.attr] = {
                    "t1": dp.t1,
                    "t2": dp.t2,
                    "v": dp.v
                }

        master_col = self._master_col_name(etype)
        try:
            self._db[master_col].update_one({"_id": ekey}, master_changes, upsert=True)
            self.log.debug(f"Updated master collection of {etype} {ekey}: {master_changes}")
        except Exception as e:
            raise DatabaseError(f"Couldn't update master collection: {e}\n{dps}") from e

    def _get_master_record(self, etype: str, ekey: str) -> dict:
        """Get current master record for etype/ekey.

        If doesn't exist, returns {}.
        """
        if etype not in self._db_schema_config:
            raise DatabaseError(f"Entity '{etype}' does not exist")

        master_col = self._master_col_name(etype)
        return self._db[master_col].find_one({"_id": ekey}) or {}

    def take_snapshot(self):
        """Takes snapshot of current master document."""
        pass

    def get_latest_snapshot(self):
        """Get latest snapshot of given `eid`.

        This method is useful for displaying data on web.
        """
        pass

    def get_snapshots(self):
        """Get all (or filtered) snapshots of given `eid`.

        This method is useful for displaying `eid`'s history on web.
        """
        pass

    def get_observation_history(self):
        """Get all (or filtered) history of observation attribute.

        This method is useful for displaying `eid`'s history on web.
        """
        pass

    def get_timeseries_history(self):
        """Get all (or filtered) history of observation attribute.

        This method is useful for displaying `eid`'s history on web.
        """
        pass
