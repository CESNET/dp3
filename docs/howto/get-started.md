# Get started with local DP³ app development

This guide helps you get to a local development environment where you can run a DP³ application on your machine, test configuration changes, and iterate on new inputs or secondary modules before moving them to production.

This is the recommended starting point for the other how-to guides in this section.

You will end up with:

- a Python environment with the `dp3` command available
- a local DP³ application directory
- MongoDB, RabbitMQ, and Redis running through Docker Compose
- a local API and worker process you can test against

## Before you start

For local application development, you need:

- Python 3.9 or higher
- `pip`
- `git`
- Docker
- Docker Compose

## 1. Prepare your application directory

Choose the tab that matches your situation.

=== "New app"

    Create and enter a working directory for the new app:

    ```shell
    mkdir my_app
    cd my_app
    ```

=== "Existing app"

    Clone or open the existing application directory:

    ```shell
    git clone <your_app_repository>  # (1)!
    cd <your_app_directory>
    ```

    1. If you already have the application checked out locally, just change into that directory.

## 2. Create a Python environment and install DP³

Create and activate a virtual environment inside the application directory, then install DP³:

```shell
python3 -m venv venv  # (1)!
source venv/bin/activate  # (2)!
pip install --upgrade pip  # (3)!
pip install dp-cubed  # (4)!
```

1. Creates an isolated Python environment for local development.
2. Windows: `venv\Scripts\activate.bat`
3. If your `pip` is already up to date, you can skip this step.
4. Installs the `dp3` command and the DP³ package into the virtual environment.

If you are starting a new app, create the application skeleton now:

=== "New app"

    ```shell
    dp3 setup . my_app
    ```

    This produces the following directory structure:
    ```shell
     📂 .
     ├── 📁 config  # (1)! 
     │   ├── 📄 api.yml
     │   ├── 📄 control.yml
     │   ├── 📄 database.yml
     │   ├── 📁 db_entities # (2)!
     │   ├── 📄 event_logging.yml
     │   ├── 📄 history_manager.yml
     │   ├── 📁 modules # (3)!
     │   ├── 📄 processing_core.yml
     │   └── 📄 snapshots.yml
     ├── 📁 docker # (4)!
     │   ├── 📁 python
     │   └── 📁 rabbitmq
     ├── 📄 docker-compose.app.yml
     ├── 📄 docker-compose.yml
     ├── 📁 modules # (5)!
     │   └── 📄 test_module.py
     ├── 📄 README.md # (6)!
     └── 📄 requirements.txt
    ```
    
    1. The `config` directory contains the configuration files for the DP³ platform. For more details,
       please check out the [configuration documentation](../configuration/index.md).
    2. The `config/db_entities` directory contains the database entities of the application.
      This defines the data model of your application. 
      For more details, you may want to check out the [data model](../data_model.md) and the
      [DB entities](../configuration/db_entities.md) documentation.
    3. The `config/modules` directory is where you can place the configuration specific to your modules.
    4. The `docker` directory contains the Dockerfiles for the RabbitMQ and python images, 
      tailored to your application. 
    5. The `modules` directory contains the modules of your application. To get started,
      a single module called `test_module` is included. 
      For more details, please check out the [Modules page](../modules.md).
    6. The `README.md` file contains some instructions to get started. 
      Edit it to your liking.

=== "Existing app"

    For an existing app, make sure the application directory should contain:
    
    - `config/`
    - `modules/`
    - `docker-compose.yml`
    
    If the application was originally created with `dp3 setup`, these files should already be present.
    If the directories were renamed, search for the `processing_core.yml` config in the codebase, or its contents:
    
    === "find"
    
        ```shell
        find . -name 'processing_core.yml'
        ```
    
    === "ripgrep"
    
        ```shell
        rg '^modules_dir:'
        ```
    

## 3. Prepare a local development configuration if needed

Many existing applications already have a production-oriented configuration directory. For local development, it is often more convenient to keep a separate local configuration that:

- points MongoDB, RabbitMQ, and Redis to services running on your machine
- uses local file paths for logs or temporary data
- keeps the same `db_entities` model and module configuration as the application you want to work on

A common pattern is to keep a separate local config directory such as `config_local/` and use that when starting the API and workers.

One practical way to do that is:

```shell
cp -a config config_local  # (1)!
rm -rf config_local/db_entities config_local/modules  # (2)!
ln -s ../config/db_entities config_local/db_entities  # (3)!
ln -s ../config/modules config_local/modules  # (4)!
```

1. Start with a full copy of the default application config.
2. Remove the copied model and module-config directories if you want to share them with the main config.
3. Reuse the same entity and attribute model in both environments.
4. Reuse the same module-specific YAML config in both environments.

Then edit the files in `config_local/` that should differ locally, for example:

- `database.yml` for local MongoDB connection details
- `processing_core.yml` for local RabbitMQ connection details, local `modules_dir`, or a smaller worker count
- `event_logging.yml` for local Redis connection details
- `api.yml` for local log-file paths

After that, use `CONF_DIR=config_local` or pass `config_local` to `dp3 worker` and `dp3 check`.

If the default `config/` directory is already suitable for local development, you can keep using it. The examples below use the basic `config` path for simplicity, but you can replace it with another configuration directory path when needed.

## 4. Start the dependent services

Start MongoDB, RabbitMQ, and Redis using the application's Docker Compose file:

```shell
docker compose up -d --build  # (1)!
```

1. This starts the local backing services in the background so the API and workers can connect to them.

Check that the services are up:

```shell
docker compose ps
```

You should see the backing services running. For a standard local setup, these are MongoDB, RabbitMQ, and Redis.

## 5. Run the application locally

There are two common ways to run the app during development.
For module and configuration development, the local-shell option is often easier to debug because you can immediately see API and worker output in separate terminals.

=== "Local shell"

    Run the API in one terminal:

    ```shell
    APP_NAME=my_app CONF_DIR=config dp3 api  # (1)!
    ```

    Run one worker in another terminal:

    ```shell
    dp3 worker my_app config 0  # (2)!
    ```

    1. Replace `my_app` with your application name and `config` with your local configuration directory if you use one.
    2. If your app uses more than one worker process, start each worker separately. Use the same local configuration directory here as well.

=== "Docker Compose app"

    Run the API and worker through the application compose file:

    ```shell
    docker compose -f docker-compose.app.yml up -d --build  # (1)!
    docker compose -f docker-compose.app.yml ps
    ```

    1. If your application uses a separate local config directory, make sure `docker-compose.app.yml` points to it before starting the containers.

## 6. Confirm that the local app is working

Run a simple API health check. For routine same-host checks, prefer `dp3 sh` and keep `curl` as a fallback.

=== "CLI (`dp3 sh`)"

    ```shell
    export DP3_CONFIG_DIR=config
    dp3 sh health
    ```

=== "HTTP (`curl`)"

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

At this point, you have a local DP³ environment ready for trying configuration changes and testing new code.

## 7. Sanity-check configuration changes as you iterate

When you change configuration, especially `config/db_entities`, validate it before restarting services:

```shell
dp3 check config  # (1)!
```

1. Replace `config` with your local configuration directory if you keep one separate from the default application config.

## 8. Where to go next

Once your local app is running, the next steps are usually one of these:

- [How to add an attribute](add-attribute.md)
- [How to add an input module](add-input.md)
- [How to add a secondary module](add-module.md)

## Troubleshooting

### The API does not start

Check that:

- the virtual environment is activated
- the `dp3` command is available
- `APP_NAME` and `CONF_DIR` point to the right application and configuration
- the configuration passes `dp3 check`

### The worker does not start

Check that:

- RabbitMQ, MongoDB, and Redis are running
- the configuration directory matches the application you are trying to run
- worker logs do not report configuration or schema errors

### Docker services are not healthy

Inspect their logs:

```shell
docker compose logs
```

You can also inspect one service at a time:

```shell
docker compose logs rabbitmq
docker compose logs mongo
docker compose logs redis
```

## Related pages

- [How-to guides](index.md)
- [Configuration overview](../configuration/index.md)
- [Modules](../modules.md)
- [API](../api.md)
