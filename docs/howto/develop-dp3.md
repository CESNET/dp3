# How to set up for DP³ platform development

This guide walks through setting up the DP³ repository for local platform development.

Use it when you want to work on DP³ itself rather than only on an application built on top of DP³. That includes changes to the core package, API, workers, docs, hooks, or configuration system.

You will end up with:

- a local clone of the DP³ repository
- an editable Python installation of DP³ with development dependencies
- Docker services for MongoDB, RabbitMQ, and Redis
- a running API and worker based on the repository's test configuration
- a development setup ready for running tests and building docs

## Before you start

For platform development, you need:

- Python 3.9 or higher
- `pip`
- `git`
- Docker
- Docker Compose

## 1. Clone the repository

Clone the DP³ repository and enter it:

```shell
git clone git@github.com:CESNET/dp3.git dp3
cd dp3
```

If you use HTTPS instead of SSH for GitHub, adjust the clone URL accordingly.

## 2. Create a Python environment and install the development dependencies

Create and activate a virtual environment, then install DP³ in editable mode with the development extras:

```shell
python3 -m venv venv  # (1)!
source venv/bin/activate  # (2)!
python -m pip install --upgrade pip  # (3)!
pip install --editable ".[dev]"  # (4)!
pre-commit install  # (5)!
```

1. Creates an isolated Python environment for platform development.
2. Windows: `venv\Scripts\activate.bat`
3. If your `pip` is already up to date, you can skip this step.
4. Editable installation means changes in the repository are immediately reflected in the environment.
5. Installs the repository's formatting and linting hooks for commits.

You can confirm that `pre-commit` is available with:

```shell
pre-commit --version
```

If you want to check the hooks across the repository immediately, run:

```shell
pre-commit run --all-files
```

## 3. Start the dependent services and the test setup

The repository includes a `docker-compose.yml` that starts:

- MongoDB
- RabbitMQ
- Redis
- a test API process
- a test worker process

Start the full setup with:

```shell
docker compose up -d --build  # (1)!
```

1. This builds or downloads the needed images and starts the full test environment in the background.

Check the running services:

```shell
docker compose ps
```

The repository's compose setup uses `tests/test_config` as the application configuration.

## 4. Confirm that the test platform is running

Check the API health endpoint:

```shell
curl -X GET 'http://localhost:5000/' \
  -H 'Accept: application/json'
```

A healthy API responds with:

```json
{
  "detail": "It works!"
}
```

You can also send a sample datapoint through the running test API:

```shell
curl -X POST 'http://localhost:5000/datapoints' \
  -H 'Content-Type: application/json' \
  --data '[{"type": "test_entity_type", "id": "abc", "attr": "test_attr_int", "v": 123, "t1": "2023-07-01T12:00:00", "t2": "2023-07-01T13:00:00"}]'
```

## 5. Run tests as you work

The repository uses `unittest`.

Run the common tests with:

```shell
python -m unittest discover \
       -s tests/test_common \
       -v
```

Run the API tests with the test configuration:

```shell
CONF_DIR=tests/test_config \
python -m unittest discover \
       -s tests/test_api \
       -v
```

Choose the smallest relevant test subset while iterating, then rerun broader coverage before finishing.

## 6. Work on documentation

The development installation also includes the documentation tooling.

To build the docs locally:

```shell
mkdocs build --strict
```

To serve them locally while editing:

```shell
mkdocs serve
```

For documentation-specific conventions, see [Extending Documentation](../extending.md).

## Related pages

- [How-to guides](index.md)
- [Get started with local DP³ app development](get-started.md)
- [Install](../install.md)
- [Extending Documentation](../extending.md)
- [Configuration overview](../configuration/index.md)
