import logging
import time
from copy import deepcopy
from datetime import datetime, timedelta
from typing import Union

from dp3.common.attrspec import AttrSpec
from dp3.common.config import HierarchicalDict
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
                 attr_spec: dict[str, dict[str, Union[EntitySpec, dict[str, AttrSpec]]]]) -> None:
        self.log = logging.getLogger("EntityDatabase")
        # self.log.setLevel("DEBUG")

        connection_conf = db_conf.get("connection", {})
        username = connection_conf.get("username", "dp3")
        password = connection_conf.get("password", "dp3")
        address = connection_conf.get("address", "localhost")
        port = str(connection_conf.get("port", 27017))
        db_name = connection_conf.get("db_name", "dp3")
