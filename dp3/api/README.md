# DP³ API

API for data ingestion and DP³ web interface.

Documentation of this API is generated automatically using FastAPI and Pydantic and is accessible on `/docs` path.

TODO: add more details

## Usage

Using `uvicorn` ASGI server (for development with hot-reloading):

```sh
cd /path/to/dp3/api
APP_NAME=dp3_app_name CONF_DIR=/path/to/dp3_config uvicorn main:app --reload --host 0.0.0.0 --port 5000
```

Using the `api` entrypoint of dp3 package (for application development):

```sh
APP_NAME=dp3_app_name CONF_DIR=/path/to/dp3_config api
```

```
usage: api [-h] [--host HOST] [--port PORT] [--reload]

Run the DP3 API using uvicorn.

optional arguments:
  -h, --help   show this help message and exit
  --host HOST  The host to bind to. (default: 0.0.0.0)
  --port PORT  The port to bind to. (default: 5000)
  --reload     Enable auto-reload of the api. (API changes will be picked up automatically.)
```