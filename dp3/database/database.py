import logging
from typing import List
from copy import deepcopy
from datetime import datetime

from sqlalchemy import create_engine, Table, Column, MetaData, func
from sqlalchemy.dialects.postgresql import VARCHAR, TIMESTAMP, BOOLEAN, INTEGER, BIGINT, ARRAY, REAL, JSON
from sqlalchemy.sql import text, select, delete, func, and_, desc, asc

from ..common.config import load_attr_spec
from ..common.attrspec import AttrSpec

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
    'id': AttrSpec("id", {'name': "id", 'data_type': "int"}),
    'eid': AttrSpec("eid", {'name': "eid", 'data_type': "string"}),
    # 'value' is inserted manually, because it depends on the attribute
    't1': AttrSpec("t1", {'name': "t1", 'data_type': "time"}),
    't2': AttrSpec("t2", {'name': "t2", 'data_type': "time"}),
    'c': AttrSpec("c", {'name': "c", 'data_type': "float"}),
    'src': AttrSpec("src", {'name': "src", 'data_type': "string"}),

    'tag': AttrSpec("tag", {'name': "tag", 'data_type': "int"}) #TODO rather smallint?
}

# preconfigured attributes all tables (records) should have
TABLE_MANDATORY_ATTRIBS = {
    'ts_added': AttrSpec("ts_added", {'name': "timestamp of record creation", 'data_type': "time"}),
    'ts_last_update': AttrSpec("ts_last_update", {'name': "timestamp of record last update", 'data_type': "time"}),
}

# Preconfiguration of main entity tables
EID_CONF = {'eid': AttrSpec("eid", {'name': "entity id", 'data_type': "string"})}


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
            raise DatabaseError(f"Cannot connect to database with specified connection arguments: {e}")

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
            if attrib_conf.data_type.startswith(("array", "set")):
                # e.g. "array<int>" --> ["array", "int>"] --> "int"
                data_type = attrib_conf.data_type.split('<')[1][:-1]
                column_type = ARRAY(ATTR_TYPE_MAPPING[data_type])
            elif attrib_conf.data_type.startswith(('dict')):
                column_type = ATTR_TYPE_MAPPING['dict']
            else:
                column_type = ATTR_TYPE_MAPPING[attrib_conf.data_type]

            # If the attribute is multi-value, convert column into an array
            if not history and attrib_conf.multi_value is True:
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
            if attrib_conf.history:
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
        if not EntityDatabase.are_tables_identical(current_table, entity_table):
            raise DatabaseConfigMismatchError(f"Table {table_name} already exists, but has different settings and "
                                              f"migration is not supported yet!")
        self._tables[table_name] = entity_table

    def init_history_tables(self, table_name_prefix: str, table_attribs: dict, db_current_state: MetaData):
        """
        Initialize all history tables in database schema.
        :param table_name_prefix: prefix of table names (entity_name)
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
            self.init_history_tables(entity_name, entity_conf['attribs'], db_current_state)

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
        except KeyError:
            self.log.error(f"Cannot create datapoint of attribute {full_attr_name}, because such history table does not exist!")
            return False
        try:
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
        result = self._db.execute(select_val_of_max_t2)
        if result is None:
            return None
        return result.fetchone()[0]

    def search(self, etype, attrs=None, query=None, limit=None, **kwargs):
        """TODO"""
        try:
            table = self._tables[etype]
        except KeyError:
            self.log.error(f"search: No table for entity type '{etype}'")
            return None # TODO raise exception?
        if query:
            NotImplementedError("query param not implemented yet")
        if attrs:
            NotImplementedError("attrs param not implemented yet")

        # Prepare SELECT statement
        cols = [table.c.eid]
        select_statement = select(cols) # note: table to use (FROM) is derived automatically from columns
        if limit is not None:
            select_statement = select_statement.limit(limit)

        # Execute statement
        try:
            result = self._db.execute(select_statement)
        except Exception as e:
            self.log.error(f"Search on table '{etype}' failed: {e}")
            return None # TODO raise exception?

        # Read all rows and return list of matching entity IDs
        return list(row[0] for row in result)

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

    def get_datapoints_range(self, etype: str, attr_name: str, eid: str = None, t1: str = None, t2: str = None, closed_interval: bool = True, sort: int = None, tag: int = None):
        """
        Gets data-points of certain time interval between t1 and t2 from database.
        :param etype: entity type
        :param attr_name: name of attribute
        :param eid: id of entity, to which data-points correspond (optional)
        :param t1: left value of time interval
        :param t2: right value of time interval
        :param closed_interval: include interval endpoints? (default = True)
        :param sort: sort by timestamps - 0: ascending order by t1, 1: descending order by t2, None: dont sort
        :param tag: optional filter for aggregation metadata
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

        if tag is not None:
            select_statement = select_statement.where(getattr(data_point_table.c, "tag") == tag)

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
            data_points_result.append(self.get_object_from_db_record(data_point_table, row))
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

    def unset_expired_values(self, etype: str, attr: str, confidence: bool):
        """
        Set expired values of given attribute to NULL (doesn't work for multi-value attributes)
        :param etype: entity type
        :param attr: attribute id
        :param confidence: unset confidence as well if set to true
        :return: None
        """
        try:
            record_table = self._tables[etype]
        except KeyError:
            raise MissingTableError(f"unset_expired_values(): Table {etype} does not exist!")
        exp_column = f"{attr}:exp"
        c_column = f"{attr}:c"
        updates = {attr: None, exp_column: None}
        if confidence:
            updates[c_column] = None
        update_statement = record_table.update().where(getattr(record_table.c, exp_column) < datetime.now()).values(updates)
        try:
            self._db.execute(update_statement)
        except Exception as e:
            raise DatabaseError(
                f"unset_expired_values(): Update failed: {e}")

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

        cols = [table.c.eid,table.c._lru,table.c.ts_added]
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
