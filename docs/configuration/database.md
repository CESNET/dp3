# Database

File `database.yml` specifies mainly MongoDB database connection details and credentials.

It looks like this:

```yaml
connection:
  username: "dp3_user"
  password: "dp3_password"
  address: "127.0.0.1"
  port: 27017
  db_name: "dp3_database"
```

## Connection

Connection details contain:

| Parameter  | Data-type | Default value | Description |
|------------|-----------|---------------|-------------|
| `username` | string    | `dp3`         | Username for connection to DB. Escaped using [`urllib.parse.quote_plus`](https://docs.python.org/3/library/urllib.parse.html#urllib.parse.quote_plus). |
| `password` | string    | `dp3`         | Password for connection to DB. Escaped using [`urllib.parse.quote_plus`](https://docs.python.org/3/library/urllib.parse.html#urllib.parse.quote_plus). |
| `address`  | string    | `localhost`   | IP address or hostname for connection to DB. |
| `port`     | int       | 27017         | Listening port of DB. |
| `db_name`  | string    | `dp3`         | Database name to be utilized by DP³. |
