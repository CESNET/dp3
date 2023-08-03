# Database

File `database.yml` specifies mainly MongoDB database connection details and credentials.

It looks like this:

```yaml
# MongoDB Connection configuration
username: "dp3_user"
password: "dp3_password"
db_name: "dp3_database"

# Standalone - single host, just specify the connection
connection:
  mode: "standalone"
  host:
    address: "127.0.0.1"
    port: 27017
```

## Authentication and database

The base of configuration is as follows:

| Parameter  | Data-type | Default value | Description                                                                                                                                            |
|------------|-----------|---------------|--------------------------------------------------------------------------------------------------------------------------------------------------------|
| `username` | string    | `dp3`         | Username for connection to DB. Escaped using [`urllib.parse.quote_plus`](https://docs.python.org/3/library/urllib.parse.html#urllib.parse.quote_plus). |
| `password` | string    | `dp3`         | Password for connection to DB. Escaped using [`urllib.parse.quote_plus`](https://docs.python.org/3/library/urllib.parse.html#urllib.parse.quote_plus). |
| `db_name`  | string    | `dp3`         | Database name to be utilized by DP³.                                                                                                                   |

## Connection

There are two modes of connection to MongoDB:

- `standalone` - single host, just specify the connection
- `replica_set` - multiple hosts, specify the connection and replica set name

| Parameter     | Data-type | Default value | Description                                                |
|---------------|-----------|---------------|------------------------------------------------------------|
| `mode`        | string    | -             | Connection mode. Must be either "standalone" or "replica". |
| `replica_set` | string    | `dp3`         | Replica set name, only applicable in "replica" mode.       |
| `address`     | string    | `localhost`   | IP address or hostname for connection to DB.               |
| `port`        | int       | 27017         | Listening port of DB.                                      |

### Standalone connection

```yaml
connection:
  mode: "standalone"
  host:
    address: "127.0.0.1"
    port: 27017
```

### Replica set connection

```yaml
connection:
  mode: "replica"
  replica_set: "dp3_replica"
  hosts:
    - address: "127.0.0.1"
      port: 27017
    - address: "127.0.0.2"
      port: 27017
    - address: "127.0.0.3"
      port: 27017
```


