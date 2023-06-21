# Installing DP³ platform

When talking about installing the DP³ platform, a distinction must be made between installing
for **platform development**, installing for application development (i.e. platform usage) 
and installing for **application** and platform **deployment**. 
We will cover all three cases separately.

## Installing for platform development

Pre-requisites: Python 3.9 or higher, `pip` (with `virtualenv` installed), `git`, `Docker` and `Docker Compose`.

Pull the repository and install using:

```shell
git clone git@github.com:CESNET/dp3.git dp3 
cd dp3
python3 -m venv venv  # (1)!
source venv/bin/activate  # (2)!
pip install --editable ".[dev]" # (3)!
pre-commit install  # (4)!
```

1. We recommend using virtual environment. If you are not familiar with it, please read 
   [this](https://docs.python.org/3/tutorial/venv.html) first.
   Note for Windows: If `python3` does not work, try `py -3` or `python` instead.
2. Windows: `venv/Scripts/activate.bat`
3. Install using editable mode to allow for changes in the code to be reflected in the installed package.
   Also, install the development dependencies, such as `pre-commit` and `mkdocs`.
4. Install `pre-commit` hooks to automatically format and lint the code before committing.

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