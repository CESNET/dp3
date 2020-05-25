import logging
from typing import List
from copy import deepcopy

from sqlalchemy import create_engine, Table, Column, MetaData
from sqlalchemy.dialects.postgresql import VARCHAR, TIMESTAMP, BOOLEAN, INTEGER, ARRAY, FLOAT
from sqlalchemy.sql import text, select

from common.config import load_attr_spec
from common.attrspec import AttrSpec
import g

# map supported data types to Postgres SQL data types
ATTR_TYPE_MAPPING = {
    'tag': BOOLEAN,
    'binary': BOOLEAN,
    'category': VARCHAR,
    'string': VARCHAR,
    'int': INTEGER,
    'float': FLOAT,
    'time': TIMESTAMP,
    'ipv4': VARCHAR,
    'ipv6': VARCHAR,
    'mac': VARCHAR,
    # TODO probably VARCHAR?
    'link': None,
    'array': ARRAY,
    'set': ARRAY,
    # TODO
    'special': None
}

# static preconfiguration of attribute's history table
HISTORY_ATTRIBS_CONF = {
    'id': AttrSpec("id", {'name': "id", 'data_type': "int"}),
    'eid': AttrSpec("eid", {'name': "eid", 'data_type': "string"}),
    # 'value' is inserted manually, because it depends on the attribute
    't1': AttrSpec("t1", {'name': "t1", 'data_type': "time"}),
    't2': AttrSpec("t2", {'name': "t2", 'data_type': "time"}),
    'c': AttrSpec("c", {'name': "c", 'data_type': "float"}),
    'src': AttrSpec("src", {'name': "src", 'data_type': "string"}),
}

# preconfigured attributes all tables (records) should have
TABLE_MANDATORY_ATTRIBS = {
    'ts_added': AttrSpec("ts_added", {'name': "timestamp of record creation", 'data_type': "time"}),
    'ts_last_update': AttrSpec("ts_last_update", {'name': "timestamp of record last update", 'data_type': "time"}),
}
EID_CONF = {'eid': AttrSpec("eid", {'name': "entity id", 'data_type': "string"})}


class DatabaseConfigMismatchError(Exception):
    def __init__(self, message=None):
        if message is None:
            message = "Database configuration is not valid, current configuration is not the same with current database " \
                      "server schema configuration. Migration is not supported yet, drop all the updated tables" \
                      " first, if you want to apply your configuration!"
        super().__init__(message)


class EntityDatabase:
    """
    PostgreSQL database wrapper responsible for whole communication with database server. Initializes database schema
    based on database configuration.
    """
    def __init__(self, conf=None):
        self.log = logging.getLogger("EntityDatabase")

        if conf is None:
            conf = {}

        connection_conf = conf['database'].get('connection', {})
        username = connection_conf.get('username', "adict")
        password = connection_conf.get('password', "adict")
        address = connection_conf.get('address', "localhost")
        port = str(connection_conf.get('port', 5432))
        db_name = connection_conf.get('db_name', "adict")
        database_url = "postgresql://" + username + ":" + password + "@" + address + ":" + port + "/" + db_name

        db_engine = create_engine(database_url)

        try:
            self._db = db_engine.connect()
        except Exception:
            # TODO How to exit properly?
            self.log.error("Cannot connect to database with specified connection arguments!")

        # internal structure for storing table objects
        self._tables = {}
        # for type check purposes let reflect as True, later with migration support may be solved in some other way
        self._db_metadata = MetaData(bind=self._db, reflect=True)

        self._db_schema_config = load_attr_spec(g.config['db_entities'])
        self.init_database_schema()
        self.log.info("Database successfully initialized!")

    @staticmethod
    def are_tables_identical(first_table: Table, second_table: Table) -> bool:
        """
        Check table columns names and their types, if table does not exist yet, just create it.
        :param first_table: first table to compare with
        :param second_table: second table to compare with
        :return: True if tables are identical, False otherwise
        """
        db_table_metadata = set([(col.key, col.type.python_type) for col in first_table.columns])
        config_table_metadata = set([(col.key, col.type.python_type) for col in second_table.columns])
        return db_table_metadata == config_table_metadata

    @staticmethod
    def init_table_columns(table_attribs: dict) -> List[Column]:
        """
        Instantiate Columns based on attributes configuration.
        :param table_attribs: dictionary of AttrSpecs
        :return: list of Columns
        """
        columns = []
        for attrib_id, attrib_conf in table_attribs.items():
            if attrib_id == "id":
                # "id" is primary key column
                columns.append(Column(attrib_id, ATTR_TYPE_MAPPING[attrib_conf.data_type], primary_key=True))
            else:
                if attrib_conf.data_type.startswith(("array", "set")):
                    # e.g. "array<int>" --> ["array", "int>"] --> "int"
                    data_type = attrib_conf.data_type.split('<')[1][:-1]
                    columns.append(Column(attrib_id, ARRAY(ATTR_TYPE_MAPPING[data_type])))
                else:
                    columns.append(Column(attrib_id, ATTR_TYPE_MAPPING[attrib_conf.data_type]))
        return columns

    def create_table(self, table_name: str, table_conf: dict, db_current_state: MetaData, history=False):
        """
        Creates new table in database schema.
        :param table_name: name of new table
        :param table_conf: configuration of new table
        :param db_current_state: current state of database (schema) to check if table does not already exist
        :param history: boolean flag determining if new table is history table or not
        :return:
        """
        # add MANDATORY ATTRIBS to table configuration
        attribs_conf = table_conf['attribs']
        attribs_conf.update(TABLE_MANDATORY_ATTRIBS)
        # if table is not history, add 'eid' attrib configuration
        if not history:
            attribs_conf.update(EID_CONF)
        columns = self.init_table_columns(attribs_conf)
        entity_table = Table(table_name, self._db_metadata, *columns, extend_existing=True)
        try:
            # check if table exists
            current_table = db_current_state.tables[table_name]
        except KeyError:
            self.log.info(f"Entity table {table_name} does not exist in the database yet, creating it now.")
            entity_table.create()
            self._tables[table_name] = entity_table
            return

        # if table exists, check if the definition is the same
        if not EntityDatabase.are_tables_identical(current_table, entity_table):
            raise DatabaseConfigMismatchError(f"Table {table_name} already exists, but has different settings and "
                                              f"migration is not supported yet!")
        self._tables[table_name] = entity_table

    def init_history_tables(self, table_attribs: dict, db_current_state: MetaData):
        """
        Initialize all history tables in database schema.
        :param table_attribs: configuration of table's attributes, some of them may contain history=True
        :param db_current_state: current state of database (needed to check if table already exists and if it is identical)
        :return: None
        """
        # TODO How to handle history_params (max_age, expire_time, etc.)? It will be probably handled by secondary
        #  modules.
        for _, attrib_conf in table_attribs.items():
            if attrib_conf.history:
                history_conf = deepcopy(HISTORY_ATTRIBS_CONF)
                history_conf['value'] = AttrSpec("value", {'name': "value", 'data_type': attrib_conf.data_type})
                self.create_table(attrib_conf.id, {'attribs': history_conf}, db_current_state)

    def init_database_schema(self):
        """
        Check if database configuration is same as in database server or only new tables were added. If any already
        existing table has updated configuration, cannot continue.
        :return: True if configuration db schema is same with database server (except new tables), otherwise False
        :raise: DatabaseConfigMismatchError if db config is not the same with database (except new tables)
        """
        # TODO database schema migration will be done later -> probably via Alembic
        db_current_state = MetaData(bind=self._db, reflect=True)

        for entity_name, entity_conf in self._db_schema_config.items():
            self.create_table(entity_name, entity_conf, db_current_state)
            self.init_history_tables(entity_conf['attribs'], db_current_state)

    def exists(self, table_name: str, key_to_record: str):
        """
        Check if record exists in table.
        :param table_name: name of database table
        :param key_to_record: key under which should be the record stored or just attribute you want to check
        :return: True if record is present in table, False otherwise
        """
        if table_name not in self._tables.keys():
            return False

        # this query should have best performance for exist check
        exist_query = text(f"SELECT 1 FROM {table_name} WHERE eid=:eid")
        result = self._db.execute(exist_query, eid=key_to_record)
        # if row count is not zero, the the record exists
        return result.rowcount != 0

    @staticmethod
    def get_id_condition(table: Table, key: str):
        """
        Lot of times SQL queries ask on id of table, but entity tables use column 'eid' and history tables use 'id',
        which needs to be distinguished. So depending on table type return correct identifier
        :param table: table to get identifier from
        :param key: second part of condition - the value of identifier
        :return: None
        """
        if "eid" in table.c:
            return table.c.eid == key
        else:
            return table.c.id == key

    def get_attrib(self, table_name, key, attrib_name):
        """
        Queries one attribute of some record in table.
        :param table_name: name of table, from which will be attribute selected from
        :param key: key to selected record
        :param attrib_name: name of selected attribute
        :return: value of the selected attribute (in attribute's data_type --> may be string, int, list, ...) or None
            if some error occurs
        """
        try:
            record_table = self._tables[table_name]
        except KeyError:
            self.log.error(f"Cannot get attribute from Record {key}, because table {table_name} does not exist!")
            return None
        if attrib_name not in record_table.c:
            self.log.error(f"Cannot get attribute's value of Record {key} of table {table_name}, because such attribute"
                           f" does not exist!")
            return None
        # getattr(record_table.c, attrib_name) gets instance of Column of table, which specifies to Select only this
        # column from the record
        select_statement = select([getattr(record_table.c, attrib_name)]).where(self.get_id_condition(record_table, key))
        try:
            q_result = self._db.execute(select_statement)
        except Exception:
            self.log.error(f"Something went wrong while selecting record {key} from {table_name} table in database!")
            return
        return q_result.fetchone()[attrib_name]

    def update_record(self, table_name: str, key: str, updates: dict):
        """
        Updates all requested values of some record in table.
        :param table_name: name of updated table
        :param key: key to updated record
        :param updates: dictionary of changes - attributes and its new values
        :return: None
        """
        try:
            record_table = self._tables[table_name]
        except KeyError:
            self.log.error(f"Record {key} cannot be updated, because table {table_name} does not exist!")
            return
        update_statement = record_table.update().where(self.get_id_condition(record_table, key)).values(**updates)
        try:
            self._db.execute(update_statement)
        except Exception as e:
            self.log.error(f"Something went wrong while updating record {key} of {table_name} table in database!")
            self.log.error(e)

    def create_record(self, table_name: str, key: str, data: dict):
        """
        Creates new record in table.
        :param table_name: name of updated table
        :param key: key to newly created record
        :param data: data to insert (including eid/id)
        :return: True if record was successfully created, False if some error occurred
        """
        try:
            record_table = self._tables[table_name]
        except KeyError:
            self.log.error(f"New record {key} cannot be created, because table {table_name} does not exist!")
            return False
        insert_statement = record_table.insert().values(**data)
        try:
            self._db.execute(insert_statement)
        except Exception as e:
            self.log.error(f"Something went wrong while inserting new record {key} of {table_name} table into database!")
            self.log.error(e)
            return False
        return True

    def delete_record(self, table_name: str, key: str):
        """
        Deletes whole record from database table.
        :param table_name: name of updated table
        :param key: key to record, which will get deleted
        :return:None
        """
        try:
            record_table = self._tables[table_name]
        except KeyError:
            self.log.error(f"Record {key} of {table_name} table cannot be deleted, because {table_name} table does not exist!")
            return

        del_statement = record_table.delete().where(self.get_id_condition(record_table, key))
        try:
            self._db.execute(del_statement)
        except Exception as e:
            self.log.error(f"Something went wrong while deleting record {key} of {table_name} table from database!")
            self.log.error(e)

    def delete_attribute(self, table_name: str, key: str, attrib_name: str):
        """
        Delete attrib by updating it to None (null).
        :param table_name: name of table, where is the attrib value going to be deleted from
        :param key: key to the updated record
        :param attrib_name: name of attribute, which value will be deleted
        :return: None
        """
        self.update_record(table_name, key, {attrib_name: None})


