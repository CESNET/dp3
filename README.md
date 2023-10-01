# Dynamic Profile Processing Platform (DP³)

<p align="center">
  <a href="https://cesnet.github.io/dp3/"><img src="https://cesnet.github.io/dp3/img/dp3-logo-min.svg" alt="DP3"></a>
</p>
<p align="center">
  <a href="https://pypi.org/project/dp-cubed/"><img src="https://badge.fury.io/py/dp-cubed.svg" alt="PyPI version" height="20"></a>
</p>

DP³ is a platform helps to keep a database of information (attributes) about individual
entities (designed for IP addresses and other network identifiers, but may be anything),
when the data constantly changes in time.

You can read more about how it works in the [documentation](https://cesnet.github.io/dp3/architecture/).

This is a basis of CESNET's "Asset Discovery Classification and Tagging" (ADiCT) project,
focused on discovery and classification of network devices,
but the platform itself is general and should be usable for any kind of data.

DP³ doesn't do much by itself, it must be supplemented by application-specific modules providing
and processing data.

## Repository structure

* `dp3` - Python package containing code of the processing core and the API
* `config` - default/example configuration
* `install` - deployment configuration

See the [documentation](https://cesnet.github.io/dp3/) for more details.

## Installation

See the [docs](https://cesnet.github.io/dp3/install/) for more details.

### Installing for application development

Pre-requisites: Python 3.9 or higher, `pip` (with `virtualenv` installed), `git`, `Docker` and `Docker Compose`.

Create a virtualenv and install the DP³ platform using:

```shell
python3 -m venv venv
source venv/bin/activate
python -m pip install --upgrade pip
pip install git+https://github.com/CESNET/dp3.git#egg=dp-cubed
```

#### Creating a DP³ application

DP³ comes with a `dp3` utility, which is used to create a new DP³ application and run it.
To create a new DP³ application we use the `setup` command. Run:

```shell
dp3 setup <application_directory> <your_application_name> 
```

So for example, to create an application called `my_app` in the current directory, run:

```shell
dp3 setup . my_app
```

Which produces a template DP3 application directory structure.

#### Running the Application

To run the application, we first need to setup the other services the platform depends on,
such as the MongoDB database, the RabbitMQ message distribution and the Redis database.
This can be done using the supplied `docker-compose.yml` file. Simply run:

```shell
docker compose up -d --build
```
There are two main ways to run the application itself. First is a little more hand-on, 
and allows easier debugging. 
There are two main kinds of processes in the application: the API and the worker processes.

To run the API, simply run:

```shell
APP_NAME=my_app CONF_DIR=config dp3 api
```

The starting configuration sets only a single worker process, which you can run using:

```shell
dp3 worker my_app config 0     
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

Final note, to simplify the experience of adjusting the app configuration, 
especially that of the DB entities, we provide the `dp3 check` command.
The command simply loads the configuration and checks that it is valid, but if not,
it tries really hard to pinpoint where exactly you went wrong. This can be used as follows:

```shell
dp3 check <config_directory>
```

You are now ready to start developing your application!

## Installing for platform development

Pre-requisites: Python 3.9 or higher, `pip` (with `virtualenv` installed), `git`, `Docker` and `Docker Compose`.

Pull the repository and install using:

```shell
git clone --branch master git@github.com:CESNET/dp3.git dp3 
cd dp3
python3 -m venv venv
source venv/bin/activate  
python -m pip install --upgrade pip  
pip install --editable ".[dev]" 
pre-commit install
```

### Running the dependencies and the platform

The DP³ platform is now installed and ready for development.
To run it, we first need to setup the other services the platform depends on,
such as the MongoDB database, the RabbitMQ message distribution and the Redis database.
This can be done using the supplied `docker-compose.yml` file. Simply run:

```shell
docker compose up -d --build
```

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
