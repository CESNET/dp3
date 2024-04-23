# Installing DP³ platform

When talking about installing the DP³ platform, a distinction must be made between installing
for [**platform development**](#platform-development), installing for [**application development**](#application-development) (i.e. platform usage) 
and installing for [**application deployment**](#application-deployment) using supervisor. 
We will cover all three cases separately.

## Application development

Pre-requisites: Python 3.9 or higher, `pip` (with `virtualenv` installed), `git`, `Docker` and `Docker Compose`.

Create a virtualenv and install the DP³ platform using:

```shell
python3 -m venv venv  # (1)!
source venv/bin/activate  # (2)!
pip install --upgrade pip  # (3)!
pip install git+https://github.com/CESNET/dp3.git#egg=dp-cubed
```

1. We recommend using virtual environment. If you are not familiar with it, please read 
   [this](https://docs.python.org/3/tutorial/venv.html) first.
   Note for Windows: If `python3` does not work, try `py -3` or `python` instead.
2. Windows: `venv/Scripts/activate.bat`
3. We require `pip>=21.0.1` for the `pyproject.toml` support.
   If your pip is up-to-date, you can skip this step.

### Creating a DP³ application

DP³ comes with a `dp3` utility, which is used to create a new DP³ application and run it.
To create a new DP³ application we use the `setup` command. Run:

```shell
dp3 setup <application_directory> <your_application_name> 
```

So for example, to create an application called `my_app` in the current directory, run:

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

To run the application, we first need to set up the other services the platform depends on,
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

---

## Application deployment

The application development installation above is not suitable for production use.
For production use, we recommend using the [`supervisor`](http://supervisord.org/) process manager,
which greatly simplifies having multiple worker processes. We recommend [`gunicorn`](https://gunicorn.org/) as the API server, 
hidden behind [`nginx`](https://www.nginx.com/) as a reverse proxy.

We will assume that you have a prototype of your application available on the server you will be deploying on, 
if not, you can use `dp3 setup` to create the application skeleton after installing the DP3 package,
as described in [the previous section](#creating-a-dp3-application).

To start, install the pre-requisites and explicitly dependent packages:

```shell
sudo dnf install git wget nginx supervisor redis
```

!!! warning "Application placement"
      
      We highly recommend using a virtualenv for you python environment
      which can be created using the command:
      
      ```sh
      python3 -m venv <path>
      ```

      We recommend placing your application, as well as the virtualenv,
      outside you home directory, for example in `/opt/<APP_NAME>`.
      This will greatly simplify the permissions management.

Inside your virtualenv, install DP3 with the `deploy` extras, which includes the `gunicorn` server:

```shell
pip install "git+https://github.com/CESNET/dp3.git#egg=dp-cubed[deploy]"
```

We will assume that you have the python environment activated for the rest of the installation.

We want to run your app under a special user, where the username should be the same as the name of your app. 
Create the user and group:

```
sudo useradd <APP_NAME>
sudo groupadd <APP_NAME>
```

### Installing dependencies

We must first cover the dependencies which were set up in the docker-compose file in the development installation.

#### Redis

You have already installed redis as a package, just enable and start the service with default configuration:

```
sudo systemctl start redis
sudo systemctl enable redis
```

#### MongoDB

If you already have an existing MongoDB instance with credentials matching your configuration, you can skip this step.

To install, please follow the official guide for your platform from [MongoDB's webpage](https://www.mongodb.com/docs/manual/administration/install-community/).
This is a quick rundown of the [installation instructions](https://www.mongodb.com/docs/manual/tutorial/install-mongodb-on-red-hat/#install-mongodb-community-edition) for an RPM-based Linux (Oracle Linux 9).

First, add the MongoDB repository:

```shell
cat > /etc/yum.repos.d/mongodb-org-6.0.repo <<EOF
[mongodb-org-6.0]
name=MongoDB Repository
baseurl=https://repo.mongodb.org/yum/redhat/\$releasever/mongodb-org/6.0/x86_64/
gpgcheck=1
enabled=1
gpgkey=https://www.mongodb.org/static/pgp/server-6.0.asc
EOF
```

Then install the MongoDB packages:

```shell
sudo dnf -y install mongodb-org mongodb-mongosh
```

Start and enable the service:

```shell
sudo systemctl daemon-reload
sudo systemctl start mongod
sudo systemctl enable mongod
```

Your DP3 app will need a user with the correct permissions to access the database,
so we will create a new user for this purpose using `mongosh`:

```js
use admin
db.createUser(
 {
  user: "<USER>",
  pwd: "<PASSWORD>",
  roles:["readWrite", "dbAdmin"]
 }
);
```

Now the API should start OK.

#### RabbitMQ

This is the most painful part of the installation, so do not get discouraged, it gets only easier from here.
For the most up-to date instructions, pick and follow an installation guide for your platform from [RabbitMQ's webpage](https://www.rabbitmq.com/download.html). 
In this section we will just briefly go through the [installation process on an RPM-based Linux](https://www.rabbitmq.com/install-rpm.html) (Oracle Linux 9).

As we will be adding RabbitMQ's and Erlang repositories,
which have individual signing keys for their packages, we first need to add these keys:

```shell
sudo rpm --import 'https://github.com/rabbitmq/signing-keys/releases/download/2.0/rabbitmq-release-signing-key.asc'
sudo rpm --import 'https://dl.cloudsmith.io/public/rabbitmq/rabbitmq-erlang/gpg.E495BB49CC4BBE5B.key'
sudo rpm --import 'https://dl.cloudsmith.io/public/rabbitmq/rabbitmq-server/gpg.9F4587F226208342.key'
```

Then add the repositories themselves, into a new file: `/etc/yum.repos.d/rabbitmq.repo`. 
RabbitMQ provides a listing of these depending on the exact distribution version, 
so just look for "Add Yum Repositories for RabbitMQ and Modern Erlang" on the
[guide page](https://www.rabbitmq.com/install-rpm.html) and copy the relevant contents.

Finally, we can install the package itself:

```shell
sudo dnf update -y
sudo dnf install socat logrotate -y
sudo dnf install -y erlang-25.3.2.3 rabbitmq-server
```

We want RabbitMQ to run on localhost, so add a file `/etc/rabbitmq/rabbitmq-env.conf` and paste into it the following:

```
HOSTNAME=localhost
```

Enable and start the service:

```shell
sudo systemctl enable rabbitmq-server
sudo systemctl start rabbitmq-server
```

Enable the web [management interface](https://www.rabbitmq.com/management.html):

```shell
sudo rabbitmq-plugins enable rabbitmq_management
```

Almost ready. RabbitMQ uses an HTTP API for its configuration, but it provides a `rabbitmqadmin` 
CLI interface that abstracts us from the details of this API. We need to get this,
as our configuration scripts depend on it. With the RabbitMQ server running,
we can download the CLI script and place it on the `PATH`:

```shell
wget 'http://localhost:15672/cli/rabbitmqadmin'
sudo chmod +x rabbitmqadmin
sudo mv rabbitmqadmin /usr/bin/
```

Finally, we have to configure the appropriate queues and exchanges, 
which is done using a provided `rmq_reconfigure.sh` script,
which can be run using the `dp3-script` entrypoint:

```shell
sudo venv/bin/dp3-script rmq_reconfigure.sh <APP_NAME> <NUM_WORKERS>
```

### Nginx

Having the app dependencies installed, and running, we can now set up the webserver.
We have already installed `nginx` in the beginning of this guide, 
so all that is left to do is to configure the server. 
DP3 provides a basic configuration that assumes only DP3 is running on the webserver,
so if that is not your case, please adjust the configuration to your liking.

To get the configuration, run:
```shell
sudo $(which dp3) config nginx \
  --hostname <SERVER_HOSTNAME> \ # (1)! 
  --app-name <APP_NAME> \ 
  --www-root <DIRECTORY> # (2)! 
```

1. e.g. dp3.example.com
2. Where to place HTML, e.g. /var/www/dp3

This will set up a *simple* landing page for the server, and proxies for the API, 
its docs and RabbitMQ management interface. With this ready, you can enable and start `nginx`:

```shell
sudo systemctl enable nginx
sudo systemctl start nginx
```

In order to reach your server, you will also have to open the firewall:

```shell
# Get firewalld running
sudo systemctl unmask firewalld
sudo systemctl enable firewalld
sudo systemctl start firewalld
# Open http/tcp
sudo firewall-cmd --add-port 80/tcp --permanent
sudo firewall-cmd --reload
```

Now you should be able to go to your server and see the landing page. 
The RabbitMQ management interface should be up and running with the 
default credentials `guest/guest`. You will notice however, that the API is currently not running,
so let's fix that.

### Setting up Supervisor control of all DP3 processes

We will set up a supervisor configuration for your DP3 app in `/etc/APPNAME`. 
For the base configuration, run:

```shell
sudo $(which dp3) config supervisor --config <CONFIG_DIR> --app-name <APP_NAME>
```

Enable the service:

```shell
sudo systemctl enable <APP_NAME>
sudo systemctl start <APP_NAME>
```

Now a new executable should be on your path, `<APPNAME>ctl`, which you can use to control the app.
It is a shortcut for `supervisorctl -c /etc/APP_NAME/supervisord.conf`, 
so you can use it to start the app:

```shell
<APPNAME>ctl start all
```

You can also use it to check the status of the app:

```shell
<APPNAME>ctl status
```

For more information on `supervisorctl`, see [its documentation](http://supervisord.org/running.html#running-supervisorctl).

You can view the generated configuration in `/etc/<APP_NAME>` and the full logs of the app's processes in `/var/log/<APP_NAME>`.

---

## Platform development

Pre-requisites: Python 3.9 or higher, `pip` (with `virtualenv` installed), `git`, `Docker` and `Docker Compose`.

Pull the repository and install using:

```shell
git clone git@github.com:CESNET/dp3.git dp3 
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

With the dependencies, the [pre-commit](https://pre-commit.com/) package is installed.
You can verify the installation using `pre-commit --version`.
Pre-commit is used to automatically unify code formatting and perform code linting.
The hooks configured in `.pre-commit-config.yaml` should now run automatically on every commit.

In case you want to make sure, you can run `pre-commit run --all-files` to see it in action.

### Running the dependencies and the platform

The DP³ platform is now installed and ready for development.
To run it, we first need to set up the other services the platform depends on,
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

[//]: # (TODO expland - installing for deployment.)