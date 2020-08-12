import logging
from typing import List
from copy import deepcopy
from datetime import datetime

from sqlalchemy import create_engine, Table, Column, MetaData
from sqlalchemy.dialects.postgresql import VARCHAR, TIMESTAMP, BOOLEAN, INTEGER, BIGINT, ARRAY, FLOAT
from sqlalchemy.sql import text, select, func, and_

from common.config import load_attr_spec
from common.attrspec import AttrSpec

# TODO:
#  - create index on "eid" (for history tables)
#  - set "eid" as primary key (for main tables)
#  - name history tables as <entity_name><delimiter><attr_name> (so different entities can have the same attribute)

# map supported data types to Postgres SQL data types
ATTR_TYPE_MAPPING = {
    'tag': BOOLEAN,
    'binary': BOOLEAN,
    'category': VARCHAR,
    'string': VARCHAR,
    'int': INTEGER,
    'int64': BIGINT,
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

    db_conf - configuration of database connection (content of database.yml)
    attr_spec - configuration of data model (entities and attributes, result of config.load_attr_spec function)
    """
    def __init__(self, db_conf, attr_spec):
        self.log = logging.getLogger("EntityDatabase")
        #self.log.setLevel("DEBUG")

        connection_conf = db_conf.get('connection', {})
        username = connection_conf.get('username', "adict")
        password = connection_conf.get('password', "adict")
        address = connection_conf.get('address', "localhost")
        port = str(connection_conf.get('port', 5432))
        db_name = connection_conf.get('db_name', "adict")
        database_url = "postgresql://" + username + ":" + password + "@" + address + ":" + port + "/" + db_name
        self.log.debug(f"Connection URL: {database_url}")
        db_engine = create_engine(database_url)

        try:
            self._db = db_engine.connect()
        except Exception as e:
            # TODO How to exit properly?
            self.log.error("Cannot connect to database with specified connection arguments!")
            self.log.error(e)

        # internal structure for storing table objects
        self._tables = {}
        # for type check purposes let reflect as True, later with migration support may be solved in some other way
        self._db_metadata = MetaData(bind=self._db, reflect=True)

        self._db_schema_config = attr_spec
        self.init_database_schema()
        self.log.info("Database successfully initialized!")

    @staticmethod
    def are_tables_identical(first_table: Table, second_table: Table) -> bool:
        """
        Check table columns names and their types.
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
                if attrib_conf.confidence:
                    # create confidence column if required from configuration
                    columns.append(Column(attrib_id + ":c", FLOAT))
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
        attribs_conf = table_conf['attribs']
        # if table is not history table, add 'eid' column configuration
        if not history:
            # this could be done just by 'attribs_conf.update(EID_CONF)' but this way is 'eid' first column in database,
            # which is more intuitive when viewing data in database
            attr_conf = deepcopy(EID_CONF)
            attr_conf.update(attribs_conf)
            attribs_conf = attr_conf
            attribs_conf.update(TABLE_MANDATORY_ATTRIBS)
        else:
            tmp_mandatory_attribs = deepcopy(TABLE_MANDATORY_ATTRIBS)
            # not needed to save tl_last_update in data-points, they should not be updated
            tmp_mandatory_attribs.pop('ts_last_update')
            attribs_conf.update(tmp_mandatory_attribs)
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
                history_conf['v'] = AttrSpec("v", {'name': "value", 'data_type': attrib_conf.data_type})
                self.create_table(attrib_conf.id, {'attribs': history_conf}, db_current_state, True)

    def init_database_schema(self):
        """
        Check if database configuration is same as in database server or only new tables were added. If any already
        existing table has updated configuration, cannot continue. If db schema is empty, whole db schema will be
        created.
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
        if "id" in table.c:
            return table.c.id == key
        else:
            return table.c.eid == key

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

    def create_multiple_records(self, table_name: str, data: list):
        """
        Creates multiple records in table.
        :param table_name: name of updated table
        :param data: list of dictionaries, which contains data of each record to save
        :return: True if records were successfully created, False if some error occurred
        """
        try:
            record_table = self._tables[table_name]
        except KeyError:
            self.log.error(f"New record cannot be created, because table {table_name} does not exist!")
            return False

        try:
            self._db.execute(record_table.insert(), data)
        except Exception as e:
            self.log.error(f"Multiple insert into {table_name} table failed!")
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

    def delete_multiple_records(self, table_name: str, list_of_ids: list):
        """
        Deletes all records of specified ids from table.
        :param table_name: name of updated table
        :param list_of_ids: list of record ids to delete
        :return: None
        """
        try:
            table = self._tables[table_name]
        except KeyError:
            self.log.error(f"Cannot delete records from {table_name} table, because table does not exist!")
            return
        if "id" in table.c:
            delete_statement = table.delete().where(getattr(table.c, "id").in_(list_of_ids))
        else:
            delete_statement = table.delete().where(getattr(table.c, "eid").in_(list_of_ids))
        try:
            self._db.execute(delete_statement)
        except Exception as e:
            self.log.error(f"Deletion of multiple records of ids {list_of_ids} in {table_name} table failed!")
            self.log.error(e)

    def create_datapoint(self, etype: str, attr_name: str, datapoint_body: dict):
        """
        Creates new data-point in attribute history table and saves the oldest value of attribute's data-points as
        actual value.
        :param etype: name of entity, which
        :param attr_name: name of history attribute (table name)
        :param datapoint_body: data of datapoint to save
        :return: True if datapoint was successfully processed, False otherwise
        """
        try:
            datapoint_table = self._tables[attr_name]
        except KeyError:
            self.log.error(f"Cannot create datapoint of attribute {attr_name}, because such history table does not exist!")
            return False
        try:
            datapoint_body.update({'ts_added': datetime.utcnow()})
            self.create_record(attr_name, datapoint_body['eid'], datapoint_body) #TODO toto v pripade chyby vraci False, nevyhazuje to vyjimku!
            self.log.debug(f"Data-point stored: {datapoint_body}")
            # TODO shouldn't this be easier to do using "SELECT v FROM... ORDER BY t2 DESC LIMIT 1" - it would require just one query
            # get maximum of 't2' of attribute's datapoints
            select_max_t2 = select([func.max(getattr(datapoint_table.c, "t2"))]).where(
                getattr(datapoint_table.c, "eid") == datapoint_body['eid'])
            result = self._db.execute(select_max_t2)
            max_val = result.fetchone()[0]
            # and get the 'v' of that record, where t2 is the maximum
            select_val = select([getattr(datapoint_table.c, "v")]).where(and_(
                getattr(datapoint_table.c, "t2") == max_val,
                getattr(datapoint_table.c, "eid") == datapoint_body['eid']
            ))
            result = self._db.execute(select_val)
            actual_val = result.fetchone()[0]
            # and insert that value as actual value of entity attribute
            self.update_record(etype, datapoint_body['eid'], {attr_name: actual_val}) # TODO Stejne tak asi toto!
            self.log.debug(f"Current value of '{attr_name}' updated to '{actual_val}'")
            return True
        except Exception as e:
            self.log.error(f"Handling of creating new datapoint of {etype} entity and {attr_name} attribute failed!")
            self.log.error(e)
            return False

    def search(self, table_name, data):
        raise NotImplementedError("search method not implemented yet")

    @staticmethod
    def get_object_from_db_record(table: Table, db_record):
        """
        Loads correct object from database query (one row)
        :param table: row Table instance to correctly match columns to query row
        :param db_record: row of database from SQL query
        :return: correct object loaded query row
        """
        record_object = {}
        for i, column in enumerate(table.c):
            record_object[column.description] = db_record[i]
        return record_object

    def get_datapoints_range(self, attr_name: str, eid: str, t1: str, t2: str):
        """
        Gets data-points of certain time interval between t1 and t2 from database.
        :param attr_name: name of attribute
        :param eid: id of entity, to which data-points corresponds
        :param t1: left value of time interval
        :param t2: right value of time interval
        :return: list of data-point objects
        """
        try:
            data_point_table = self._tables[attr_name]
        except KeyError:
            self.log.error(f"Cannot get data-points range, because history table of {attr_name} does not exist!")
            return
        # wanted datapoints values must have their 't2' > t1 and 't1' < t2 (where t without '' are arguments from this
        # method)
        select_statement = select([data_point_table]).where(and_(getattr(data_point_table.c, "eid") == eid,
                                                                 getattr(data_point_table.c, "t2") > t1,
                                                                 getattr(data_point_table.c, "t1") < t2))
        try:
            result = self._db.execute(select_statement)
        except Exception as e:
            self.log.error(f"Select of data-point range from {t1} to {t2} failed!")
            self.log.error(e)
            return None
        data_points_result = []
        for row in result:
            data_points_result.append(self.get_object_from_db_record(data_point_table, row))
        return data_points_result

    def rewrite_data_points(self, attr_name: str, list_of_ids_to_delete: list, new_data_points: list):
        """
        Deletes data-points of specified ids and inserts new data-points.
        :param attr_name: name of data-point attribute
        :param list_of_ids_to_delete: ids of data-points, which will be deleted from database
        :param new_data_points: list of dictionaries, which contains data of new data points
        :return: None
        """
        # TODO: do in a trasnsaction (that is probably needed on other places as well)
        self.delete_multiple_records(attr_name, list_of_ids_to_delete)
        self.create_multiple_records(attr_name, new_data_points)
