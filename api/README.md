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
