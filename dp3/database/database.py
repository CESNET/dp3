import logging
from typing import List
from copy import deepcopy
from datetime import datetime, timedelta

from sqlalchemy import create_engine, Table, Column, MetaData, func
from sqlalchemy.dialects.postgresql import VARCHAR, TIMESTAMP, BOOLEAN, INTEGER, BIGINT, ARRAY, REAL, JSON
from sqlalchemy.sql import text, select, delete, func, and_, desc, asc

from ..common.config import load_attr_spec
from ..common.attrspec import AttrSpec, validators, timeseries_types
from ..common.utils import parse_rfc_time
from ..history_management.constants import *

# map supported data types to Postgres SQL data types
ATTR_TYPE_MAPPING = {
    'tag': BOOLEAN,
    'binary': BOOLEAN,
    'category': VARCHAR,
    'string': VARCHAR,
    'int': INTEGER,
    'int64': BIGINT,
    'float': REAL,
    'time': TIMESTAMP,
    'ipv4': VARCHAR,
    'ipv6': VARCHAR,
    'mac': VARCHAR,
    # TODO probably VARCHAR?
    'link': None,
    'array': ARRAY,
    'set': ARRAY,
    'special': JSON, # deprecated, use json instead
    'json': JSON, # TODO: use JSONB, but we need at least psql9.6, we currently have 9.2
    'dict': JSON, # TODO: if the list of fields is known, it might be better to store it as several normal columns
}

# static preconfiguration of attribute's history table
HISTORY_ATTRIBS_CONF = {
    'id': AttrSpec("id", {'name': "id", 'type': "plain", 'data_type': "int"}),
    'eid': AttrSpec("eid", {'name': "eid", 'type': "plain", 'data_type': "string"}),
    # 'value' is inserted manually, because it depends on the attribute
    't1': AttrSpec("t1", {'name': "t1", 'type': "plain", 'data_type': "time"}),
    't2': AttrSpec("t2", {'name': "t2", 'type': "plain", 'data_type': "time"}),
    'c': AttrSpec("c", {'name': "c", 'type': "plain", 'data_type': "float"}),
    'src': AttrSpec("src", {'name': "src", 'type': "plain", 'data_type': "string"}),

    'tag': AttrSpec("tag", {'name': "tag", 'type': "plain", 'data_type': "int"}) #TODO rather smallint?
}

# preconfigured attributes all tables (records) should have
TABLE_MANDATORY_ATTRIBS = {
    'ts_added': AttrSpec("ts_added", {'name': "timestamp of record creation", 'type': "plain", 'data_type': "time"}),
    'ts_last_update': AttrSpec("ts_last_update", {'name': "timestamp of record last update", 'type': "plain", 'data_type': "time"}),
}

# Preconfiguration of main entity tables
EID_CONF = {'eid': AttrSpec("eid", {'name': "entity id", 'type': "plain", 'data_type': "string"})}


# Custom exceptions
class DatabaseConfigMismatchError(Exception):
    def __init__(self, message=None):
        if message is None:
            message = "Database configuration is not valid, current configuration is not the same with current database " \
                      "server schema configuration. Migration is not supported yet, drop all the updated tables" \
                      " first, if you want to apply your configuration!"
        super().__init__(message)

class DatabaseError(Exception):
    pass

class MissingTableError(DatabaseError):
    pass


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
        username = connection_conf.get('username', "dp3")
        password = connection_conf.get('password', "dp3")
        address = connection_conf.get('address', "localhost")
        port = str(connection_conf.get('port', 5432))
        db_name = connection_conf.get('db_name', "dp3")
        database_url = "postgresql://" + username + ":" + password + "@" + address + ":" + port + "/" + db_name
        self.log.debug(f"Connection URL: {database_url}")
        db_engine = create_engine(database_url)

        try:
            self._db = db_engine.connect()
        except Exception as e:
            raise DatabaseError(f"Cannot connect to database with specified connection arguments: {e}")

        # internal structure for storing table objects
        self._tables = {}
        # for type check purposes let reflect as True, later with migration support may be solved in some other way
        self._db_metadata = MetaData(bind=self._db, reflect=True)

        self._db_schema_config = attr_spec
        self.init_database_schema()
        self.log.info("Database successfully initialized!")

    def are_tables_identical(self, first_table: Table, second_table: Table) -> bool:
        """
        Check table columns names and their types.
        :param first_table: first table to compare with
        :param second_table: second table to compare with
        :return: True if tables are identical, False otherwise
        """
        db_table_metadata = set([(col.key, col.type.python_type) for col in first_table.columns])
        config_table_metadata = set([(col.key, col.type.python_type) for col in second_table.columns])
        if db_table_metadata != config_table_metadata:
            self.log.error("Tables not identical: "
                           f"expected in db, but missing: {config_table_metadata - db_table_metadata}; "
                           f"in db, but unexpected: {db_table_metadata - config_table_metadata}")
        return db_table_metadata == config_table_metadata

    @staticmethod
    def init_table_columns(table_attribs: dict, history: bool) -> List[Column]:
        """
        Instantiate Columns based on attributes configuration.
        :param table_attribs: dictionary of AttrSpecs
        :param history: boolean flag determining if new table is history table or not
        :return: list of Columns
        """
        columns = []

        for attrib_id, attrib_conf in table_attribs.items():
            # Get database type of column
            if attrib_conf.type == "timeseries":
                # No column with current value of timeseries
                continue
            elif attrib_conf.data_type.startswith(("array", "set")):
                # e.g. "array<int>" --> ["array", "int>"] --> "int"
                data_type = attrib_conf.data_type.split('<')[1][:-1]
                column_type = ARRAY(ATTR_TYPE_MAPPING[data_type])
            elif attrib_conf.data_type.startswith(('dict')):
                column_type = ATTR_TYPE_MAPPING['dict']
            else:
                column_type = ATTR_TYPE_MAPPING[attrib_conf.data_type]

            # If the attribute is multi-value, convert column into an array
            if not history and attrib_conf.multi_value:
                column_type = ARRAY(column_type)

            # Create column
            if (history and attrib_id == "id") or (not history and attrib_id == "eid"):
                # primary key column
                columns.append(Column(attrib_id, column_type, primary_key=True))
            elif history and attrib_id == "eid":
                # create index on "eid" column of history tables
                columns.append(Column(attrib_id, column_type, index=True))
            else:
                # normal column
                columns.append(Column(attrib_id, column_type))

            # Add confidence column if required from configuration
            if attrib_conf.confidence:
                column_type = REAL
                # Multi-value attributes can have different confidence for each individual value
                if attrib_conf.multi_value:
                    column_type = ARRAY(column_type)
                columns.append(Column(attrib_id + ":c", column_type))

            # Add value expiration date for attributes with history
            if attrib_conf.type == "observations":
                column_type = TIMESTAMP
                # Multi-value attributes can have different expiration date for each individual value
                if attrib_conf.multi_value:
                    column_type = ARRAY(column_type)
                columns.append(Column(attrib_id + ":exp", column_type))

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
        columns = self.init_table_columns(attribs_conf, history=history)
        entity_table = Table(table_name, self._db_metadata, *columns, extend_existing=True)
        try:
            # check if table exists
            current_table = db_current_state.tables[table_name]
        except KeyError:
            # Table doesn't exist yet, create it
            # TODO: If multipe workers are running in parallel, only one (the first one) should attempt to create it.
            #   We should check the process index here and if it's not 0, wait a second or two (until the first worker
            #   create the tables) and re-check the table existence.
            #   But it will probably need a larger code refactoring.
            self.log.info(f"Entity table {table_name} does not exist in the database yet, creating it now.")
            entity_table.create()
            self._tables[table_name] = entity_table
            return

        # if table exists, check if the definition is the same
        if not self.are_tables_identical(current_table, entity_table):
            raise DatabaseConfigMismatchError(f"Table {table_name} already exists, but has different settings and "
                                              f"migration is not supported yet!")
        self._tables[table_name] = entity_table

    def init_history_timeseries_tables(self, table_name_prefix: str, table_attribs: dict, db_current_state: MetaData):
        """
        Initialize all history and timeseries tables in database schema.
        :param table_name_prefix: prefix of table names (entity_name)
        :param table_attribs: configuration of table's attributes, some of them may contain history=True
        :param db_current_state: current state of database (needed to check if table already exists and if it is identical)
        :return: None
        """
        # TODO How to handle history_params (max_age, expire_time, etc.)? It will be probably handled by secondary
        #  modules.
        for _, attrib_conf in table_attribs.items():
            if attrib_conf.type == "observations" or attrib_conf.type == "timeseries":
                history_conf = deepcopy(HISTORY_ATTRIBS_CONF)

                if attrib_conf.type == "timeseries":
                    # Create one "value" column for all series
                    for series_id, series_item in attrib_conf.series.items():
                        series_data_type = series_item["data_type"]
                        prefixed_id = "v_" + series_id

                        # Data type is array of configured data type.
                        # Incoming data points are arrays and they are stored
                        # in the same format.
                        history_conf[prefixed_id] = AttrSpec(prefixed_id, {
                            'name': series_id,
                            'type': "plain",
                            'data_type': f"array<{series_data_type}>"
                        })
                else:
                    history_conf['v'] = AttrSpec("v", {
                        'name': "value",
                        'type': "plain",
                        'data_type': attrib_conf.data_type
                    })

                # History tables are named as <entity_name>__<attr_name> (e.g. "ip__activity_flows")
                table_name = f"{table_name_prefix}__{attrib_conf.id}"
                self.create_table(table_name, {'attribs': history_conf}, db_current_state, True)

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
            self.init_history_timeseries_tables(entity_name, entity_conf['attribs'], db_current_state)

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
        # if row count is not zero, the record exists
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
            q_result = self._db.execute(select_statement).fetchone()
        except Exception:
            self.log.error(f"Something went wrong while selecting record {key} from {table_name} table in database!")
            return
        if q_result is None:
            return None
        return q_result[attrib_name]

    def get_record(self, table_name, key):
        """
        Queries whole record in table.
        :param table_name: name of table, from which will be record selected
        :param key: key to selected record
        :return: selected record as tuple
        """
        try:
            table = self._tables[table_name]
        except KeyError:
            self.log.error(f"Cannot get record {key}, because table {table_name} does not exist!")
            return None
        select_statement = select([table]).where(self.get_id_condition(table, key))
        try:
            q_result = self._db.execute(select_statement).fetchone()
        except Exception:
            self.log.error(f"Something went wrong while selecting record {key} from {table_name} table in database!")
            return
        return q_result

    def update_record(self, table_name: str, key: str, updates: dict):
        """
        Updates all requested values of some record in table.
        :param table_name: name of updated table
        :param key: key to updated record
        :param updates: dictionary of changes - attributes and its new values
        :raises MissingTableError: table does'nt exist in database
        :raises DatabaseError: an error occurred when trying to execute database operation
        """
        try:
            record_table = self._tables[table_name]
        except KeyError:
            raise MissingTableError(f"Record {key} cannot be updated, because table {table_name} does not exist!")
        update_statement = record_table.update().where(self.get_id_condition(record_table, key)).values(**updates)
        try:
            self._db.execute(update_statement)
        except Exception as e:
            raise DatabaseError(f"Something went wrong while updating record {key} of {table_name} table in database: {e}")

    def create_record(self, table_name: str, key: str, data: dict):
        """
        Creates new record in table.
        :param table_name: name of updated table
        :param key: key to newly created record
        :param data: data to insert (including eid/id)
        :raises MissingTableError: table does'nt exist in database
        :raises DatabaseError: an error occurred when trying to execute database operation
        """
        try:
            record_table = self._tables[table_name]
        except KeyError:
            raise MissingTableError(f"New record {key} cannot be created, because table {table_name} does not exist!")
        insert_statement = record_table.insert().values(**data)
        try:
            self._db.execute(insert_statement)
        except Exception as e:
            raise DatabaseError(f"Something went wrong while inserting new record {key} of {table_name} table into database: {e}")

    def create_multiple_records(self, table_name: str, data: list):
        """
        Creates multiple records in table.
        :param table_name: name of updated table
        :param data: list of dictionaries, which contains data of each record to save
        :return: True if records were successfully created, False if some error occurred
        :raises MissingTableError: table does'nt exist in database
        :raises DatabaseError: an error occurred when trying to execute database operation
        """
        try:
            record_table = self._tables[table_name]
        except KeyError:
            raise MissingTableError(f"New records cannot be created, because table {table_name} does not exist!")
        try:
            self._db.execute(record_table.insert(), data)
        except Exception as e:
            raise DatabaseError(f"Multiple insert into {table_name} table failed: {e}")

    def delete_record(self, table_name: str, key: str):
        """
        Deletes whole record from database table.
        :param table_name: name of updated table
        :param key: key to record, which will get deleted
        :raises MissingTableError: table does'nt exist in database
        :raises DatabaseError: an error occurred when trying to execute database operation
        """
        try:
            record_table = self._tables[table_name]
        except KeyError:
            raise MissingTableError(f"Record {key} of {table_name} table cannot be deleted, because {table_name} table does not exist!")

        del_statement = record_table.delete().where(self.get_id_condition(record_table, key))
        try:
            self._db.execute(del_statement)
        except Exception as e:
            raise DatabaseError(f"Something went wrong while deleting record {key} of {table_name} table from database: {e}")

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
            raise MissingTableError(f"Cannot delete records from {table_name} table, because table does not exist!")
        if "id" in table.c:
            delete_statement = table.delete().where(getattr(table.c, "id").in_(list_of_ids))
        else:
            delete_statement = table.delete().where(getattr(table.c, "eid").in_(list_of_ids))
        try:
            self._db.execute(delete_statement)
        except Exception as e:
            raise DatabaseError(f"Deletion of multiple records of ids {list_of_ids} in {table_name} table failed: {e}")

    def create_datapoint(self, etype: str, attr_name: str, datapoint_body: dict):
        """
        Creates new data-point in attribute history table
        :param etype: entity type
        :param attr_name: name of history attribute (table name)
        :param datapoint_body: data of datapoint to save
        :return: True if datapoint was successfully processed, False otherwise
        """
        full_attr_name = f"{etype}__{attr_name}"
        try:
            datapoint_table = self._tables[full_attr_name]
            attrib_conf = self._db_schema_config[etype]["attribs"][attr_name]
        except KeyError:
            self.log.error(f"Cannot create datapoint of attribute {full_attr_name}, because such history table does not exist!")
            return False

        try:
            # Timeseries' `v` in datapoint represents multiple columns at once.
            if attrib_conf.type == "timeseries":
                datapoint_body = self.process_timeseries_datapoint(datapoint_body, etype, attr_name)

            datapoint_body.update({'ts_added': datetime.utcnow()})
            self.create_record(full_attr_name, datapoint_body['eid'], datapoint_body)
            #self.log.debug(f"Data-point stored: {datapoint_body}")
            return True
        except Exception as e:
            self.log.error(f"Creating new datapoint of {etype} entity and {attr_name} attribute failed: {e}")
            return False

    def get_current_value(self, etype: str, eid, attr_name: str):
        """
        Get the latest value of attribute's data-points.
        # TODO: should be the latest *valid* value according to configured timeouts
        # TODO: in case if "multi-value" attributes, all currently valid attributes should be returned as array
        :param etype: entity type
        :param eid: entity ID
        :param attr_name: name of history attribute (table name)
        :return: The value (or None if no data-point is found)
        """
        full_attr_name = f"{etype}__{attr_name}"
        try:
            datapoint_table = self._tables[full_attr_name]
        except KeyError:
            raise MissingTableError(f"Cannot get current value of attribute {full_attr_name}, because such history table does not exist!")

        select_val_of_max_t2 = select([datapoint_table.c.v]) \
            .where(datapoint_table.c.eid == eid) \
            .order_by(desc(datapoint_table.c.t2)) \
            .limit(1)
        result = self._db.execute(select_val_of_max_t2).fetchone()
        if result is None:
            return None
        return result[0]

    def search(self, etype, attrs=None, query=None, limit=None, offset=None, sort_by=None, sort_ascending=True, **kwargs):
        """TODO"""
        try:
            table = self._tables[etype]
        except KeyError:
            self.log.error(f"search: No table for entity type '{etype}'")
            return None # TODO raise exception?

        # Prepare SELECT statement
        cols = [table.c.eid]
        select_statement = select(cols) # note: table to use (FROM) is derived automatically from columns
        if attrs is None:
            NotImplementedError("attrs param not implemented yet")

        if query:
            for attr_name, value in query.items():
                if attr_name not in table.c:
                    self.log.error(f"Cannot search by attribute {attr_name} of table {etype}, because such attribute"
                            f" does not exist!")
                    return None

                if type(value) == str:
                    select_statement = select_statement.where(getattr(table.c, attr_name).like(value))
                else:
                    select_statement = select_statement.where(getattr(table.c, attr_name) == value)

        if limit is not None:
            select_statement = select_statement.limit(limit)
        if offset is not None:
            select_statement = select_statement.offset(offset)
        if sort_by is not None:
            if sort_ascending:
                select_statement = select_statement.order_by(asc(sort_by))
            else:
                select_statement = select_statement.order_by(desc(sort_by))

        # Execute statement
        try:
            result = self._db.execute(select_statement)
        except Exception as e:
            self.log.error(f"Search on table '{etype}' failed: {e}")
            return None # TODO raise exception?

        # Read all rows and return list of matching entity IDs
        return list(row[0] for row in result)

    def get_object_from_db_record(self, etype, db_record):
        """
        Loads correct object from database query (one row)
        :param table: row Table instance to correctly match columns to query row
        :param db_record: row of database from SQL query
        :return: correct object loaded query row
        """
        try:
            table = self._tables[etype]
        except KeyError:
            self.log.error(f"Cannot get object from DB record, because table of {etype} does not exist!")
            return None
        record_object = {}
        for i, column in enumerate(table.c):
            record_object[column.description] = db_record[i]
        return record_object

    def get_datapoints_range(self, etype: str, attr_name: str, eid: str = None, t1: str = None, t2: str = None, closed_interval: bool = True, sort: int = None, filter_redundant: bool = True):
        """
        Gets data-points of certain time interval between t1 and t2 from database.
        :param etype: entity type
        :param attr_name: name of attribute
        :param eid: id of entity, to which data-points correspond (optional)
        :param t1: left value of time interval
        :param t2: right value of time interval
        :param closed_interval: include interval endpoints? (default = True)
        :param sort: sort by timestamps - 0: ascending order by t1, 1: descending order by t2, None: dont sort
        :param filter_redundant: True (default): return only non-redundant; False: return only redundant; None: dont filter
        :return: list of data-point objects or None on error
        """
        full_attr_name = f"{etype}__{attr_name}"
        try:
            data_point_table = self._tables[full_attr_name]
        except KeyError:
            self.log.error(f"Cannot get data-points range, because history table of {full_attr_name} does not exist!")
            return None
        # wanted datapoints values must have their 't2' > t1 and 't1' < t2 (where t without '' are arguments from this
        # method)
        select_statement = select([data_point_table])

        if eid is not None:
            select_statement = select_statement.where(getattr(data_point_table.c, "eid") == eid)

        if filter_redundant is True:
            select_statement = select_statement.where(getattr(data_point_table.c, "tag") != TAG_REDUNDANT)
        elif filter_redundant is False:
            select_statement = select_statement.where(getattr(data_point_table.c, "tag") == TAG_REDUNDANT)

        if t1 is not None:
            if closed_interval:
                select_statement = select_statement.where(getattr(data_point_table.c, "t2") >= t1)
            else:
                select_statement = select_statement.where(getattr(data_point_table.c, "t2") > t1)
        if t2 is not None:
            if closed_interval:
                select_statement = select_statement.where(getattr(data_point_table.c, "t1") <= t2)
            else:
                select_statement = select_statement.where(getattr(data_point_table.c, "t1") < t2)

        if sort == 0:
            select_statement = select_statement.order_by(asc(data_point_table.c.t1))
        elif sort == 1:
            select_statement = select_statement.order_by(desc(data_point_table.c.t2))

        try:
            result = self._db.execute(select_statement)
        except Exception as e:
            self.log.error(f"Select of data-points range from {t1} to {t2} failed: {e}")
            return None
        data_points_result = []
        for row in result:
            data_points_result.append(self.get_object_from_db_record(full_attr_name, row))
        return data_points_result

    def delete_old_datapoints(self, etype: str, attr_name: str, t_old: str, t_redundant: str, tag: int):
        full_attr_name = f"{etype}__{attr_name}"
        try:
            data_point_table = self._tables[full_attr_name]
        except KeyError:
            self.log.error(f"Cannot get data-points range, because history table of {full_attr_name} does not exist!")
            return None
        # Delete all datapoints older than 't_old' and redundant datapoints older than 't_redundant'
        delete_statement = delete(data_point_table).where((getattr(data_point_table.c, "t2") < t_old) | ((getattr(data_point_table.c, "t2") < t_redundant) & (getattr(data_point_table.c, "tag") == tag)))
        try:
            result = self._db.execute(delete_statement)
        except Exception as e:
            self.log.error(f"Delete operation failed: {e}")
            return None

    def rewrite_data_points(self, etype: str, attr_name: str, list_of_ids_to_delete: list, new_data_points: list):
        """
        Deletes data-points of specified ids and inserts new data-points.
        :param etype: entity type
        :param attr_name: name of data-point attribute
        :param list_of_ids_to_delete: ids of data-points, which will be deleted from database
        :param new_data_points: list of dictionaries, which contains data of new data points
        :return: None
        """
        full_attr_name = f"{etype}__{attr_name}"
        # TODO: do in a transaction (that is probably needed on other places as well)
        self.delete_multiple_records(full_attr_name, list_of_ids_to_delete)
        self.create_multiple_records(full_attr_name, new_data_points)

    def get_entities(self, etype: str):
        """
        Returns all unique entities (eid) of given type
        :param etype: entity type
        :return: List of entity ids
        """
        try:
            table_name = self._tables[etype]
        except KeyError:
            self.log.error(f"get_entities(): Table '{etype}' does not exist!")
            return None
        select_statement = select([table_name.c.eid]).distinct()
        try:
            result = self._db.execute(select_statement)
        except Exception as e:
            self.log.error(f"get_entities(): Select failed: {e}")
            return None
        return [r[0] for r in result]

    def unset_expired_values(self, etype: str, attr: str, confidence: bool, return_updated_ids: False):
        """
        Set expired values of given attribute to NULL (doesn't work for multi-value attributes)
        :param etype: entity type
        :param attr: attribute id
        :param confidence: unset confidence as well if set to true
        :param return_updated_ids: whether to return IDs of updated entities
        :return: None or IDs of updated entities
        """
        try:
            record_table = self._tables[etype]
        except KeyError:
            raise MissingTableError(f"unset_expired_values(): Table {etype} does not exist!")
        updated_ids = None
        exp_column = f"{attr}:exp"
        c_column = f"{attr}:c"
        updates = {attr: None, exp_column: None}
        if confidence:
            updates[c_column] = None
        if return_updated_ids:
            select_statement = record_table.select().where(getattr(record_table.c, exp_column) < datetime.now())
            try:
                result = self._db.execute(select_statement)
                updated_ids = [r[0] for r in result]
            except Exception as e:
                self.log.error(f"unset_expired_values(): Select failed: {e}")
        update_statement = record_table.update().where(getattr(record_table.c, exp_column) < datetime.now()).values(updates)
        try:
            self._db.execute(update_statement)
        except Exception as e:
            raise DatabaseError(
                f"unset_expired_values(): Update failed: {e}")
        return updated_ids

    def get_entities_with_expired_values(self, etype: str, attr: str):
        """
        Search for records containing an expired value (only works for multi-value attributes)
        :param etype: entity type
        :param attr: attribute id
        :return: list of entity ids
        """
        try:
            table_name = self._tables[etype]
        except KeyError:
            self.log.error(f"get_entities_with_expired_values(): Table '{etype}' does not exist!")
            return None
        exp_column = f"\"{attr}:exp\""
        select_statement = select([table_name.c.eid]).select_from(text(f"unnest({exp_column}) as exp")).where(text(f"exp < \'{datetime.now()}\'"))
        try:
            result = self._db.execute(select_statement)
        except Exception as e:
            self.log.error(f"get_entities_with_expired_values(): Select failed: {e}")
            return None
        return [r[0] for r in result]

    def last_updated(self, etype, before, after=None, weekly=False, limit=None):
        try:
            table = self._tables[etype]
        except KeyError:
            self.log.error(f"need_update: No table for entity type '{etype}'")
            return set()

        cols = [table.c.eid, table.c._lru, table.c.ts_added]
        select_statement = select(cols) \
            .where(table.c._lru < before)
        if after is not None:
            select_statement = select_statement.where(table.c._lru > after)
        if limit is not None:
            select_statement = select_statement.limit(limit)

        # Execute statement
        try:
            result = self._db.execute(select_statement)
        except Exception as e:
            self.log.error(f"need_update: Search on table '{etype}' failed: {e}")
            return set()

        # Read all rows and return set of matching entity IDs
        return result 

    def process_timeseries_datapoint(self, datapoint_body: dict, etype: str, attr_name: str):
        """Splits timeseries datapoints' value into multiple values.

        Timeseries' `v` in datapoint represents multiple columns at once.
        Split it into multiple fields/columns according to attribute's spec.
        """
        attrib_conf = self._db_schema_config[etype]["attribs"][attr_name]
        v = datapoint_body["v"]
        t1 = parse_rfc_time(datapoint_body["t1"])
        t2 = parse_rfc_time(datapoint_body["t2"])
        time_step = attrib_conf.time_step

        # Check if all value arrays are the same length
        values_len = [ len(v_i) for _, v_i in v.items() ]
        if len(set(values_len)) != 1:
            raise ValueError(f"Datapoint arrays have different lengths: {values_len}")

        # Check t2
        if attrib_conf.timeseries_type == "regular":
            if t2 - t1 != values_len[0]*time_step:
                raise ValueError(f"Difference of t1 and t2 is invalid. Must be n*time_step.")

        # Check all series are present
        for series_id in attrib_conf.series:
            # Time for regular timeseries will be added automatically later
            if series_id == "time" and attrib_conf.timeseries_type == "regular":
                continue
            if not series_id in v:
                raise ValueError(f"Datapoint is missing values for '{series_id}' series")

        # Split `v`
        for v_id, v_i in v.items():
            series_conf = attrib_conf.series.get(v_id)
            if not series_conf:
                raise ValueError(f"Series {v_id} doesn't exist")
            series_data_type = series_conf["data_type"]
            prefixed_id = "v_" + v_id

            # Validate values
            for j, primitive in enumerate(v_i):
                if not validators[series_data_type](primitive):
                    raise ValueError(f"Series value {primitive} is invalid (type should be {series_data_type})")

            # Convert timestamps
            if series_data_type == "time":
                v_i = [ parse_rfc_time(p) for p in v_i ]

                # Validate all timestamps
                for dt in v_i:
                    if not t1 <= dt <= t2:
                        raise ValueError(f"Series value {dt} is invalid (must be in [{t1}, {t2}] interval)")

            datapoint_body[prefixed_id] = v_i

        # Check for overlapping records
        if attrib_conf.timeseries_type == "regular":
            eid = datapoint_body["eid"]

            overlapping_count = self.get_overlapping_dp_count(etype, attr_name, eid, t1, t2)
            if overlapping_count > 0:
                raise ValueError(f"Datapoint is overlapping with {overlapping_count} other datapoints")

        del datapoint_body["v"]

        return datapoint_body

    def get_overlapping_dp_count(self, etype: str, attr_name: str, eid: str = None, t1: datetime = None, t2: datetime = None):
        """
        Counts overlapping datapoints with new record that spans between t1 and t2.
        :param etype: entity type
        :param attr_name: name of attribute
        :param eid: id of entity, to which data-points correspond (optional)
        :param t1: left value of time interval
        :param t2: right value of time interval
        :param closed_interval: include interval endpoints? (default = True)
        :return: int or None on error
        """
        full_attr_name = f"{etype}__{attr_name}"
        try:
            table = self._tables[full_attr_name]
        except KeyError:
            self.log.error(f"Cannot get timeseries, because history table of {full_attr_name} does not exist!")
            return None

        # Build a query
        query = select([func.count()]).select_from(table)
        query = query.where(table.c.eid == eid)
        query = query.where(table.c.t2 > t1)
        query = query.where(table.c.t1 < t2)

        return self._db.execute(query).scalar()

    def get_timeseries(self, etype: str, attr_name: str, eid: str = None, t1: str = None, t2: str = None, closed_interval: bool = True):
        """
        Gets timeseries of certain time interval between t1 and t2 from database.
        :param etype: entity type
        :param attr_name: name of attribute
        :param eid: id of entity, to which data-points correspond (optional)
        :param t1: left value of time interval
        :param t2: right value of time interval
        :param closed_interval: include interval endpoints? (default = True)
        :return: dict
        """
        # Get raw data
        query_result_dicts = self.get_timeseries_raw(etype, attr_name, eid, t1, t2, closed_interval)

        attrib_conf = self._db_schema_config[etype]["attribs"][attr_name]

        if t1 is not None:
            t1 = parse_rfc_time(t1)
        if t2 is not None:
            t2 = parse_rfc_time(t2)

        # Convert to dict of lists
        series_ids = list(attrib_conf.series.keys())       # [ "time", "bytes", ... ]
        series_result = dict((s, []) for s in series_ids)  # { "time": [], "bytes": [], ... }

        if attrib_conf.timeseries_type == "regular":
            if len(query_result_dicts) > 0:
                if t1 or t2:
                    query_result_dicts = self.discard_dp_outside_interval_regular_timeseries(query_result_dicts, attrib_conf, t1, t2, series_ids)

                if not t1:
                    t1 = query_result_dicts[0]["t1"]
                if not t2:
                    t2 = query_result_dicts[-1]["t2"]

            # Add additional data to result
            series_result["t1"] = t1
            series_result["t2"] = t2
            series_result["time_step"] = int(attrib_conf.time_step.total_seconds())

            # t2s has +[t1], so that t2s[-1] index does work correctly
            t1s = [ row["t1"] for row in query_result_dicts ]
            t2s = [ row["t2"] for row in query_result_dicts ] + [ t1 ]

            # Process series
            for i, row in enumerate(query_result_dicts):
                # Check for time without any data
                # Calculate how many "cycles" were skipped between previous
                # t2 and current t1 and corresponding count of None values.
                t2_t1_delta = t1s[i] - t2s[i-1]
                if t2_t1_delta:
                    cycles_skipped = t2_t1_delta // attrib_conf.time_step
                    for series_id in series_ids:
                        series_result[series_id] += cycles_skipped * [ None ]

                for series_id in series_ids:
                    # Concatenate timeseries
                    series_result[series_id] += row["v_" + series_id]
        else:
            # Process series
            for i, row in enumerate(query_result_dicts):
                for series_id in series_ids:
                    # Concatenate timeseries' datapoints
                    series_result[series_id].append(row[series_id])

        return series_result

    def get_timeseries_raw(self, etype: str, attr_name: str, eid: str = None, t1: str = None, t2: str = None, closed_interval: bool = True):
        """
        Gets raw timeseries data of certain time interval between t1 and t2 from database.
        Wrapper around SQL query.
        :param etype: entity type
        :param attr_name: name of attribute
        :param eid: id of entity, to which data-points correspond (optional)
        :param t1: left value of time interval
        :param t2: right value of time interval
        :param closed_interval: include interval endpoints? (default = True)
        :return: list of dicts
        """
        full_attr_name = f"{etype}__{attr_name}"
        try:
            table = self._tables[full_attr_name]
        except KeyError:
            self.log.error(f"Cannot get timeseries, because history table of {full_attr_name} does not exist!")
            return None

        attrib_conf = self._db_schema_config[etype]["attribs"][attr_name]

        # Get id of first default series (should be "time" or "time_first")
        sort_by = timeseries_types[attrib_conf.timeseries_type]["sort_by"]

        try:
            # Build query
            if attrib_conf.timeseries_type == "regular":
                select_fields = [table]
            else:
                select_fields = []
                for series_id in attrib_conf.series:
                    column = getattr(table.c, "v_" + series_id)
                    select_fields.append(func.unnest(column).label(series_id))

            query = select(select_fields)

            if eid is not None:
                query = query.where(table.c.eid == eid)
            if t1 is not None:
                query = query.where(table.c.t2 >= t1 if closed_interval else table.c.t2 > t1)
            if t2 is not None:
                query = query.where(table.c.t1 <= t2 if closed_interval else table.c.t1 < t2)

            query = query.order_by(asc(sort_by))

            query_result = self._db.execute(query)
        except Exception as e:
            self.log.error(f"Select of timeseries from {t1} to {t2} failed: {e}")
            return None

        # Convert to list of dicts
        return list(map(dict, query_result))

    def discard_dp_outside_interval_regular_timeseries(self, query_result_dicts: list, attrib_conf: AttrSpec, t1: datetime, t2: datetime, series_ids: list):
        """
        Discards datapoints outside interval [t1, t2]. Only valid for regular
        timeseries.
        :param query_result_dicts: list of SQL query rows (as dicts)
        :param t1: left value of time interval
        :param t2: right value of time interval
        :return: list of dicts
        """
        if len(query_result_dicts) > 0:
            if t1:
                # First row may include items before (in front of) [t1, t2] interval.
                # Discard them.
                delta = t1 - query_result_dicts[0]["t1"]
                if delta > timedelta(seconds=0):
                    items_to_discard = delta // attrib_conf.time_step
                    for series_id in series_ids:
                        # Discard first items_to_discard items
                        prefixed_id = "v_" + series_id
                        del query_result_dicts[0][prefixed_id][:items_to_discard]

            if t2:
                # Last row may include items after [t1, t2] interval.
                # Discard them.
                delta = query_result_dicts[-1]["t2"] - t2
                if delta > timedelta(seconds=0):
                    items_to_discard = delta // attrib_conf.time_step
                    for series_id in series_ids:
                        # Discard last items_to_discard items
                        prefixed_id = "v_" + series_id
                        del query_result_dicts[-1][prefixed_id][-items_to_discard:]

        return query_result_dicts
