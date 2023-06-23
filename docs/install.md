# Installing DP³ platform

When talking about installing the DP³ platform, a distinction must be made between installing
for **platform development**, installing for application development (i.e. platform usage) 
and installing for **application** and platform **deployment**. 
We will cover all three cases separately.

## Installing for application development

Pre-requisites: Python 3.9 or higher, `pip` (with `virtualenv` installed), `git`, `Docker` and `Docker Compose`.

Create a virtualenv and install the DP³ platform using:

```shell
python3 -m venv venv  # (1)!
source venv/bin/activate  # (2)!
python -m pip install --upgrade pip  # (3)!
pip install git+https://github.com/CESNET/dp3.git@new_dp3#egg=dp3
```

1. We recommend using virtual environment. If you are not familiar with it, please read 
   [this](https://docs.python.org/3/tutorial/venv.html) first.
   Note for Windows: If `python3` does not work, try `py -3` or `python` instead.
2. Windows: `venv/Scripts/activate.bat`
3. We require `pip>=21.0.1` for the `pyproject.toml` support.
   If your pip is up-to-date, you can skip this step.

### Creating a DP³ application

To create a new DP³ application we will use the included `dp3-setup` utility. Run:

```shell
dp3-setup <application_directory> <your_application_name> 
```

So for example, to create an application called `my_app` in the current directory, run:

```shell
dp3-setup . my_app
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
 │   ├── 📄 README.md
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
   please check out the [configuration documentation](configuration/index.md).
2. The `config/db_entities` directory contains the database entities of the application.
   This defines the data model of your application. 
   For more details, you may want to check out the [data model](data_model.md) and the
   [DB entities](configuration/db_entities.md) documentation.
3. The `config/modules` directory is where you can place the configuration specific to your modules.
4. The `docker` directory contains the Dockerfiles for the RabbitMQ and python images, 
   tailored to your application. 
5. The `modules` directory contains the modules of your application. To get started,
   a single module called `test_module` is included. 
   For more details, please check out the [Modules page](modules.md).
6. The `README.md` file contains some instructions to get started. 
   Edit it to your liking.

### Running the Application

To run the application, we first need to setup the other services the platform depends on,
such as the MongoDB database, the RabbitMQ message distribution and the Redis database.
This can be done using the supplied `docker-compose.yml` file. Simply run:

```shell
docker compose up -d --build  # (1)!
```

1. The `-d` flag runs the services in the background, so you can continue working in the same terminal.
   The `--build` flag forces Docker to rebuild the images, so you can be sure you are running the latest version.
   If you want to run the services in the foreground, omit the `-d` flag.

??? info "Docker Compose basics"
      
      The state of running containers can be checked using:
      
      ```shell
      docker compose ps
      ```
      
      which will display the state of running processes. The logs of the services can be displayed using:
      
      ```shell
      docker compose logs
      ```
      
      which will display the logs of all services, or:
      
      ```shell
      docker compose logs <service name>
      ```
      
      which will display only the logs of the given service. 
      (In this case, the services are *rabbitmq*, *mongo*, *mongo_express*, and *redis*)
      
      We can now focus on running the platform and developing or testing. After you are done, simply run:
      
      ```shell
      docker compose down
      ```
      
      which will stop and remove all containers, networks and volumes created by `docker compose up`.

There are two main ways to run the application itself. First is a little more hand-on, 
and allows easier debugging. 
There are two main kinds of processes in the application: the API and the worker processes.

To run the API, simply run:

```shell
APP_NAME=my_app CONF_DIR=config api
```

The starting configuration sets only a single worker process, which you can run using:

```shell
worker my_app config 0     
```

The second way is to use the `docker-compose.app.yml` file, which runs the API and the worker processes
in separate containers. To run the API, simply run:

```shell
docker compose -f docker-compose.app.yml up -d --build
```

Either way, to test that everything is running properly, you can run:
```shell
curl -X 'GET' 'http://localhost:5000/' \
     -H 'Accept: application/json' 
```

Which should return a JSON response with the following content:
```json
{
   "detail": "It works!"
}
```

You are now ready to start developing your application!

## Installing for platform development

Pre-requisites: Python 3.9 or higher, `pip` (with `virtualenv` installed), `git`, `Docker` and `Docker Compose`.

Pull the repository and install using:

```shell
git clone --branch new_dp3 git@github.com:CESNET/dp3.git dp3 
cd dp3
python3 -m venv venv  # (1)!
source venv/bin/activate  # (2)!
python -m pip install --upgrade pip  # (3)!
pip install --editable ".[dev]" # (4)!
pre-commit install  # (5)!
```

1. We recommend using virtual environment. If you are not familiar with it, please read 
   [this](https://docs.python.org/3/tutorial/venv.html) first.
   Note for Windows: If `python3` does not work, try `py -3` or `python` instead.
2. Windows: `venv/Scripts/activate.bat`
3. We require `pip>=21.0.1` for the `pyproject.toml` support.
   If your pip is up-to-date, you can skip this step.
4. Install using editable mode to allow for changes in the code to be reflected in the installed package.
   Also, install the development dependencies, such as `pre-commit` and `mkdocs`.
5. Install `pre-commit` hooks to automatically format and lint the code before committing.

### Running the dependencies and the platform

The DP³ platform is now installed and ready for development.
To run it, we first need to setup the other services the platform depends on,
such as the MongoDB database, the RabbitMQ message distribution and the Redis database.
This can be done using the supplied `docker-compose.yml` file. Simply run:

```shell
docker compose up -d --build  # (1)!
```

1. The `-d` flag runs the services in the background, so you can continue working in the same terminal.
   The `--build` flag forces Docker to rebuild the images, so you can be sure you are running the latest version.
   If you want to run the services in the foreground, omit the `-d` flag.

???+ info "On Docker Compose"

    Docker Compose can be installed as a standalone (older v1) or as a plugin (v2), 
    the only difference is when executing the command:
    
    > Note that Compose standalone uses the dash compose syntax instead of current’s standard syntax (space compose).
    > For example: type `docker-compose up` when using Compose standalone, instead of `docker compose up`.

    This documentation uses the v2 syntax, so if you have the standalone version installed, adjust accordingly.

After the first `compose up` command, the images for RabbitMQ, MongoDB and Redis will be downloaded,
their images will be built according to the configuration and all three services will be started.
On subsequent runs, Docker will use the cache, so if the configuration does not change, the download
and build steps will not be repeated.

The configuration is taken implicitly from the `docker-compose.yml` file in the current directory.
The `docker-compose.yml` configuration contains the configuration for the services,
as well as a testing setup of the DP³ platform itself. 
The full configuration is in `tests/test_config`.
The setup includes one worker process and one API process to handle requests. 
The API process is exposed on port 5000, so you can send requests to it using `curl` or from your browser:

```shell
curl -X 'GET' 'http://localhost:5000/' \
     -H 'Accept: application/json' 
```
```shell
curl -X 'POST' 'http://localhost:5000/datapoints' \
     -H 'Content-Type: application/json' \
     --data '[{"type": "test_entity_type", "id": "abc", "attr": "test_attr_int", "v": 123, "t1": "2023-07-01T12:00:00", "t2": "2023-07-01T13:00:00"}]'
```

??? info "Docker Compose basics"
      
      The state of running containers can be checked using:
      
      ```shell
      docker compose ps
      ```
      
      which will display the state of running processes. The logs of the services can be displayed using:
      
      ```shell
      docker compose logs
      ```
      
      which will display the logs of all services, or:
      
      ```shell
      docker compose logs <service name>
      ```
      
      which will display only the logs of the given service. 
      (In this case, the services are *rabbitmq*, *mongo*, *redis*, *receiver_api* and *worker*)
      
      We can now focus on running the platform and developing or testing. After you are done, simply run:
      
      ```shell
      docker compose down
      ```
      
      which will stop and remove all containers, networks and volumes created by `docker compose up`.

### Testing

With the testing platform setup running, we can now run tests. 
Tests are run using the `unittest` framework and can be run using:

```shell
python -m unittest discover \
       -s tests/test_common \
       -v
CONF_DIR=tests/test_config \
python -m unittest discover \
       -s tests/test_api \
       -v
```

### Documentation

For extending of this documentation, please refer to the [Extending](extending.md) page.

[//]: # (TODO expland - installing for application development, deployment.)