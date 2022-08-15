#!/usr/bin/env python3

"""
Load and check dp3 configuration files from given directory and update database scheme as needed.

The Script requires an argument with the path to the configuration.

IMPORTANT: ALL WORKERS MUST BE STOPPED BEFORE RUNNING THIS SCRIPT.
To check if any workers are running, use the '-u' argument and pass the URL where the script can check if any worker
is active.

The script connects to the database, checks the existing tables and columns, compares them with the current
configuration and if needed, it automatically adds the tables and columns needed to match the configuration.

It also checks if data types of columns in the database and data types in configuration are the same.
If not, it gives you an option to change the data type of the database column to the data type from configuration.

Script also checks if there any columns in database that shouldn't be here. 
If it finds a column or table like this it gives you the option to drop this table or column.
"""

import inspect
import yaml
import sys
import os
import argparse
import requests
import json
from sqlalchemy.dialects.postgresql import VARCHAR, TIMESTAMP, BOOLEAN, INTEGER, BIGINT, ARRAY, REAL, JSON
from sqlalchemy import create_engine, inspect, Table, Column, MetaData, func

sys.path.insert(1, os.path.join(os.path.dirname(__file__), '..'))
from dp3.common.config import read_config_dir, load_attr_spec

# map supported data types to Postgres SQL data types (from database.py)
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
    'link': None,
    'array': ARRAY,
    'set': ARRAY,
    'special': JSON,
    'json': JSON, 
    'dict': JSON, 
}


def create_config_column_list(config_item, attr_spec):
    # creating list of names of columns required by attributes
    # print(config_item)
    attr_columns = attr_spec.get(config_item).get("attribs")
    columns_in_attr = list()
    for attr_col in attr_columns.keys():
        columns_in_attr.append(attr_col) # adds name of attribute to list of columns that should be in the table
        if attr_columns.get(attr_col).history: # if attribute has history set to true, column with stamp :exp has to be added
            exp = attr_col + ":exp"
            columns_in_attr.append(exp)
        if attr_columns.get(attr_col).confidence: # if attribute has confidence set to true, column with stamp :exp has to be added
            columns_in_attr.append(attr_col + ":c")

    return columns_in_attr


def create_db_column_list(config_item, db_inspector):
    # creates list with names of columns that are already in database
    db_columns = db_inspector.get_columns(config_item, schema="public") 
    columns_in_db = list()
    for col in db_columns:
        columns_in_db.append(col.get("name"))
    return columns_in_db


def insert_column(col_name, config_item, attr_spec, meta):
    # inserting new column into existing table
    table = meta.tables[config_item]
    data_type = get_data_type(col_name, attr_spec.get(config_item).get("attribs"))
    column = Column(col_name, data_type)
    column.create(table)
    print(f'Added column "{col_name}" into "{config_item}" table.')


def create_column_list(columns_in_attr, attributes):
    # creates list with all columns that have to be in new table
    column_list = list()
    for item in columns_in_attr:
        data_type = get_data_type(item, attributes)
        column_list.append(Column(item, data_type))
    return(column_list)


def create_new_entity_table(config_item, db_engine, column_list, meta):
    # creating new table for entity
    table = Table(
        config_item, meta,
        Column("eid", VARCHAR, primary_key = True),
        *column_list,
        Column("ts_added", TIMESTAMP),
        Column("ts_last_update", TIMESTAMP))
    meta.create_all(db_engine)
    print(f'New table "{config_item}" was created.')


def create_history_table(table_name, meta, db_engine, data_type):
    table = Table(
        table_name, meta,
        Column("id", INTEGER, primary_key=True),
        Column("eid", VARCHAR, index=True),
        Column("t1", TIMESTAMP),
        Column("t2", TIMESTAMP),
        Column("c", REAL),
        Column("src", VARCHAR),
        Column("tag", INTEGER),
        Column("v", data_type),
        Column("ts_added", TIMESTAMP)
    )
    meta.create_all(db_engine)
    print(f'New history table {table_name} was created.')


def get_data_type(item, attributes):
    name = item
    if item.endswith(":c"):
        name = item.split(":")[0]
        data_type = REAL
    elif item.endswith(":exp"):
        name = item.split(":")[0]
        data_type = TIMESTAMP
    elif attributes.get(item).data_type.startswith(("set", "array")):
        data_type = ARRAY(ATTR_TYPE_MAPPING[attributes.get(item).data_type.split('<')[1][:-1]])
    elif attributes.get(item).data_type.startswith("dict"):
        data_type =  ATTR_TYPE_MAPPING["dict"]
    else:
        data_type = ATTR_TYPE_MAPPING[attributes.get(item).data_type]

    if attributes.get(name).multi_value:
        data_type = ARRAY(data_type)
    
    return data_type


def change_col_data_type(config_item, col_name, config_col_type, meta):
    # when data type of column in database is not the same as in configuration, type of column in db can be changed
    while True:
        answer = input(f'Do you want to change type of column "{col_name}" in table "{config_item}" in database according to configuration (yes/no)? ')
        answer = answer.lower()
        if answer == "yes":
            # delete and create 
            table = meta.tables[config_item]
            column = table.c[col_name]
            column.drop()
            new_column = Column(col_name, config_col_type)
            new_column.create(table)
            break
        elif answer == "no":
            break
        else:
            print('Answer "yes" or "no".')


def add_table_or_column(attr_spec, db_inspector, db_engine, meta):
    """ checks if any tables or colums have to be added """
    
    db_table = db_inspector.get_table_names(schema="public")
    for config_item in attr_spec.keys():
        columns_in_attr = create_config_column_list(config_item, attr_spec)
        # if table that should be in database is not there, this table is created
        if config_item not in db_table:
            # creates new table in database
            column_list = create_column_list(columns_in_attr, attr_spec.get(config_item).get("attribs"))
            create_new_entity_table(config_item, db_engine, column_list, meta)
        else:
            # checks if all columns are in database and also checks the data type
            table = meta.tables.get(config_item)
            columns_in_db = create_db_column_list(config_item, db_inspector)
            for col_name in columns_in_attr:
                if col_name not in columns_in_db:
                    insert_column(col_name, config_item, attr_spec, meta)
                elif not col_name.endswith((":c", ":exp")):
                    config_col_type = get_data_type(col_name, attr_spec.get(config_item).get("attribs"))
                    db_col_type = table.columns[col_name].type
                    if config_col_type is not db_col_type.__class__: # not same data_type
                        if config_col_type.__class__ is ARRAY and db_col_type.__class__ is ARRAY: # both are arrays
                            if config_col_type.item_type.__class__ is not db_col_type.item_type.__class__: # different types of arrays
                                change_col_data_type(config_item, col_name, config_col_type, meta)
                        else: # not arrays and not same data type
                            change_col_data_type(config_item, col_name, config_col_type, meta)
        
        # checks if there is a new history table that needs to be added
        for col_name in columns_in_attr:
            if not col_name.endswith((":c", ":exp")):
                table_name = config_item + "__" + col_name
                if table_name not in db_table and attr_spec.get(config_item).get("attribs").get(col_name).history:
                    data_type = get_data_type(col_name, attr_spec.get(config_item).get("attribs"))
                    create_history_table(table_name, meta, db_engine, data_type)


def get_table_names_attr(attr_spec):
    # returns list with names of tables that should be in database according to configuration
    attr_table = list(attr_spec.keys())
    for table in attr_spec.keys():
        attribs = attr_spec.get(table).get("attribs")
        for item in attribs:
            if attribs.get(item).history:
                attr_table.append(table + "__" + item)
    return attr_table


def delete_table(table_name, meta, db_engine):
    # drops table
    while True:
        delete = input(f"Do you really want to delete table {table_name} (yes/no)? ")
        delete = delete.lower()
        if delete == "yes":
            table = meta.tables.get(table_name)
            table.drop(db_engine)
            break
        elif delete == "no":
            break
        else:
            print('Answer "yes" or "no".')


def delete_column(table_name, col, meta):
    # drops column from table
    while True:
        delete = input(f'Do you really want to delete column "{col}" from table "{table_name}" (yes/no)? ')
        delete = delete.lower()
        if delete == "yes": 
            column = meta.tables[table_name].c[col]
            column.drop()
            break
        elif delete == "no":
            break
        else:
            print('Answer "yes" or "no".')


def delete_table_or_column(attr_spec, db_inspector, db_engine, meta, connection):
    # deletes tables and columns that are in database, but they are not in configuration
    db_table = db_inspector.get_table_names(schema="public")
    attr_table = get_table_names_attr(attr_spec)
    for table_name in db_table:
        if table_name not in attr_table:
            delete_table(table_name, meta, db_engine)
            continue
            # db_table.remove(table_name)

        if "__" not in table_name:
            config_list = create_config_column_list(table_name, attr_spec)
            db_list = create_db_column_list(table_name, db_inspector)
            col_list = ["eid","ts_added", "ts_last_update"]

            for col in db_list:
                if col not in config_list and col not in col_list:
                    delete_column(table_name, col, meta)
        

def validity_of_config(args):
    # checking if the configuration is valid
    try:
        config = read_config_dir(args.config_dir, True)
        attr_spec = load_attr_spec(config.get("db_entities"))
    except Exception as e:
        print(f"CONFIGURATION ERROR: {e}")
        sys.exit(1)

    print("Configuration is valid.")

    if args.verbose:
        # Print parsed config as JSON (print unserializable objects using str())
        print(json.dumps(config, indent=4, default=str))

    return attr_spec


def get_db_connection(config_dir):
    # connecting to ADiCT database
    db_config_file = os.path.join(config_dir, "database.yml")
    with open(db_config_file, "r") as f:
        db = yaml.safe_load(f)
    connection_conf = db.get('connection', {})
    username = connection_conf.get('username')
    password = connection_conf.get('password', "")
    address = connection_conf.get('address', "localhost")
    port = str(connection_conf.get('port', 5432))
    db_name = connection_conf.get('db_name')
    database_url = "postgresql://" + username + ":" + password + "@" + address + ":" + port + "/" + db_name
    print(f"Connection URL: {database_url}")
    print("Connecting to database...")
    try:
        db_engine = create_engine(url=database_url)
    except Exception as e:
        print(f"CONNECTION ERROR: {e}")
        sys.exit(2)
    
    return db_engine

def check_workers(worker_check_url):
    print("Checking workers...")
    url = os.path.join(worker_check_url, "workers_alive")
    for x in range(5):
        res = requests.get(url)
        workers = json.loads(res.content).get("workers_alive")
        if workers:
            print("Script can't run while workers are running, stop workers and run the script again.")
            exit(1)


def parse_arguments():
    # Parse arguments
    parser = argparse.ArgumentParser(
        prog="update_db_scheme",
        description="Load and check dp3 configuration files from given directory and update database scheme as needed."
    )
    parser.add_argument('config_dir', metavar='CONFIG_DIRECTORY',
                        help="Path to a directory containing configuration files (e.g. /etc/my_app/config)")
    parser.add_argument('-u','--worker_check_url', metavar='WORKER_CHECK_URL',
                        help='Base URL of an API where we can check if any workers are active (via "workers_alive" endpoint)')
    parser.add_argument('-v', '--verbose', action="store_true", help="Verbose mode - print parsed configuration", default=False)
    return parser.parse_args()


def main():
    args = parse_arguments()
    if args.worker_check_url is not None:
        check_workers(args.worker_check_url)

    attr_spec = validity_of_config(args) # checks if configuration is valid
    # connecting database
    db_engine = get_db_connection(args.config_dir)
    try:
        connection = db_engine.connect()
    except Exception as e:
        print(f"ERROR: {e}")
        exit(1)
    print("Checking if the database scheme matches the configuration...")
    db_inspector = inspect(db_engine)
    meta = MetaData()
    meta.reflect(bind=db_engine)
    meta.bind = db_engine

    # checking if any changes in database schema have to be made
    add_table_or_column(attr_spec, db_inspector, db_engine, meta)
    delete_table_or_column(attr_spec, db_inspector,db_engine, meta, connection)
    
    # closing database connection
    connection.close()
    db_engine.dispose()
    print("Done.")
    sys.exit(0)


if __name__ == "__main__":
    main()