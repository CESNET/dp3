import logging
import time
from copy import deepcopy
from datetime import datetime, timedelta
from typing import Union

from dp3.common.attrspec import AttrSpec
from dp3.common.config import HierarchicalDict
from dp3.common.entityspec import EntitySpec

# Static preconfiguration of attribute's history table
HISTORY_ATTRIBS_CONF = {
    "id": AttrSpec("id", {"name": "id", "type": "plain", "data_type": "int"}),
    "eid": AttrSpec("eid", {"name": "eid", "type": "plain", "data_type": "string"}),
    # "value" is inserted manually, because it depends on the attribute
    "t1": AttrSpec("t1", {"name": "t1", "type": "plain", "data_type": "time"}),
    "t2": AttrSpec("t2", {"name": "t2", "type": "plain", "data_type": "time"}),
    "c": AttrSpec("c", {"name": "c", "type": "plain", "data_type": "float"}),
    "src": AttrSpec("src", {"name": "src", "type": "plain", "data_type": "string"}),

    "tag": AttrSpec("tag", {"name": "tag", "type": "plain", "data_type": "int"})
}

# Preconfigured attributes all tables (records) should have
TABLE_MANDATORY_ATTRIBS = {
    "ts_added": AttrSpec("ts_added", {"name": "timestamp of record creation", "type": "plain", "data_type": "time"}),
    "ts_last_update": AttrSpec("ts_last_update", {"name": "timestamp of record last update", "type": "plain", "data_type": "time"}),
}

# Preconfiguration of main entity tables
EID_CONF = {"eid": AttrSpec("eid", {"name": "entity id", "type": "plain", "data_type": "string"})}

# number of seconds to wait for the i-th attempt to reconnect after error
RECONNECT_DELAYS = [1, 2, 5, 10, 30]

# Custom exceptions
# class DatabaseConfigMismatchError(Exception):
#     def __init__(self, message=None):
#         if message is None:
#             message = "Database configuration is not valid, current configuration is not the same with current database " \
#                       "server schema configuration. Migration is not supported yet, drop all the updated tables" \
#                       " first, if you want to apply your configuration!"
#         super().__init__(message)

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
                 attr_spec: dict[str, dict[str, Union[EntitySpec, dict[str, AttrSpec]]]]) -> None:
        self.log = logging.getLogger("EntityDatabase")
        # self.log.setLevel("DEBUG")

        connection_conf = db_conf.get("connection", {})
        username = connection_conf.get("username", "dp3")
        password = connection_conf.get("password", "dp3")
        address = connection_conf.get("address", "localhost")
        port = str(connection_conf.get("port", 27017))
        db_name = connection_conf.get("db_name", "dp3")
