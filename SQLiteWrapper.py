import sqlite3
import os

# Path to directory containing db file
db_dir = "PATH TO DB DIRECTORY"

# Name of the db file
db_name = "Datapoints.db"

# Template string used when creating the db
query_template_create = '''CREATE TABLE Datapoints (
                                attr TEXT,
                                id TEXT,
                                t1 TEXT,
                                t2 TEXT,
                                type TEXT,
                                v TEXT,
                                c REAL,
                                src TEXT,
                                PRIMARY KEY(attr, id, t1)
                            );'''

# Template string used when inserting to the db
query_template_insert = '''INSERT INTO Datapoints (attr, id, t1, t2, type, v, c, src) 
                           VALUES ("{}", "{}", "{}", "{}", "{}", "{}", {}, "{}");'''


# Create the db if it doesnt exist yet
def init():
    if db_name not in os.listdir(db_dir):
        process_query(query_template_create)


# Insert record (dict) to db
def insert(record):
    try:
        query = query_template_insert.format(
            record["attr"],
            record["id"],
            record["t1"],
            record["t2"],
            record["type"],
            record["v"],
            record["c"],
            record["src"]
        )
        process_query(query)
    except sqlite3.Error as e:
        print(e)
        raise


# Connect to the db and process given query
def process_query(query):
    try:
        connection = sqlite3.connect(db_dir + db_name)
        cursor = connection.cursor()
        cursor.execute(query)
        connection.commit()
        cursor.close()
        connection.close()
    except sqlite3.Error as e:
        print(e)
        raise
