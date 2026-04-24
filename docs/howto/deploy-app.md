# How to deploy a DP³ application

This guide walks through deploying a DP³ application to a server and operating it once it is live.

For production deployments, the recommended setup is:

- [`supervisor`](http://supervisord.org/) to manage the long-lived API and worker processes
- [`gunicorn`](https://gunicorn.org/) as the API server
- [`nginx`](https://www.nginx.com/) as a reverse proxy in front of the API

The recommended workflow is:

1. develop and test the application locally first
2. deploy the tested configuration and code to the server
3. run the long-lived API and worker processes under a process manager
4. use logs, status checks, and telemetry to watch the deployment after changes

If you do not already have a working local development setup, start with [Get started with local DP³ app development](get-started.md).

## Initial Deployment

### Before you start

This guide assumes:

- you already have a DP³ application directory with working configuration, or you are ready to create one with `dp3 setup`
- you have tested important changes locally before deploying them
- you are deploying on a Linux server
- you want long-lived API and worker processes managed by `supervisor`

If you do not yet have an application directory, create one locally with `dp3 setup`, fill in the application-specific configuration, test it, and only then deploy it to the server.

### 1. Prepare the server and Python environment

Install the base packages:

```shell
sudo dnf install git wget nginx supervisor redis  # (1)!
```

1. Package names vary by distribution. Adjust them if you are not on an RPM-based system.

!!! warning "Application placement"

    We recommend placing both the application and its virtual environment outside the home directory, for example under `/opt/<APP_NAME>`.
    This keeps permissions and service management simpler.

Create a virtual environment for the deployment and install DP³ with the deployment extras:

```shell
python3 -m venv /opt/<APP_NAME>/venv  # (1)!
source /opt/<APP_NAME>/venv/bin/activate
pip install --upgrade pip
pip install dp-cubed[deploy]  # (2)!
```

1. Creates the Python environment that will be used by the deployed services.
2. The `deploy` extras include the `gunicorn` server used by the generated supervisor configuration.

Deploy or copy your application directory onto the server. The deployment should include at least:

- the application configuration directory
- the application's `modules/` directory
- any other code or files your application depends on

### 2. Create the application user

Run the deployment under a dedicated user and group:

```shell
sudo useradd <APP_NAME>
sudo groupadd <APP_NAME>
```

Adjust ownership of the deployed application files so the application user can read the configuration and write to the paths you expect it to use.

### 3. Install and start the backing services

#### Redis

Enable and start Redis:

```shell
sudo systemctl start redis
sudo systemctl enable redis
```

#### MongoDB

If you already have a MongoDB instance that matches your application configuration, you can reuse it. Otherwise install MongoDB and create a database user for the app.

For the most up-to-date instructions, follow the official MongoDB installation guide for your platform:

- [MongoDB installation overview](https://www.mongodb.com/docs/manual/administration/install-community/)
- [MongoDB installation on RPM-based Linux](https://www.mongodb.com/docs/manual/tutorial/install-mongodb-on-red-hat/#install-mongodb-community-edition)

A minimal RPM-based installation looks like this:

```shell
cat > /etc/yum.repos.d/mongodb-org-6.0.repo <<EOF
[mongodb-org-6.0]
name=MongoDB Repository
baseurl=https://repo.mongodb.org/yum/redhat/\$releasever/mongodb-org/6.0/x86_64/
gpgcheck=1
enabled=1
gpgkey=https://www.mongodb.org/static/pgp/server-6.0.asc
EOF
sudo dnf -y install mongodb-org mongodb-mongosh
sudo systemctl daemon-reload
sudo systemctl start mongod
sudo systemctl enable mongod
```

Create the MongoDB user that matches your `database.yml`:

```javascript
use admin
db.createUser(
 {
  user: "<USER>",
  pwd: "<PASSWORD>",
  roles:["readWrite", "dbAdmin"]
 }
);
```

#### RabbitMQ

RabbitMQ setup is the most distribution-specific part of the deployment. For the most up-to-date instructions, follow RabbitMQ's official documentation for your platform:

- [RabbitMQ download and installation guides](https://www.rabbitmq.com/download.html)
- [RabbitMQ installation on RPM-based Linux](https://www.rabbitmq.com/install-rpm.html)

Install RabbitMQ and configure it to listen on localhost. On RPM-based systems, one possible setup is:

```shell
sudo rpm --import 'https://github.com/rabbitmq/signing-keys/releases/download/2.0/rabbitmq-release-signing-key.asc'
sudo rpm --import 'https://dl.cloudsmith.io/public/rabbitmq/rabbitmq-erlang/gpg.E495BB49CC4BBE5B.key'
sudo rpm --import 'https://dl.cloudsmith.io/public/rabbitmq/rabbitmq-server/gpg.9F4587F226208342.key'
```

Then add the RabbitMQ and Erlang repositories for your platform, install the packages, and start the service:

```shell
sudo dnf update -y
sudo dnf install socat logrotate -y
sudo dnf install -y erlang-25.3.2.3 rabbitmq-server
printf 'HOSTNAME=localhost\n' | sudo tee /etc/rabbitmq/rabbitmq-env.conf  # (1)!
sudo systemctl enable rabbitmq-server
sudo systemctl start rabbitmq-server
sudo rabbitmq-plugins enable rabbitmq_management  # (2)!
```

1. This keeps RabbitMQ bound to localhost, which is a good default when `nginx` is the public entry point.
2. Enables the RabbitMQ [management interface](https://www.rabbitmq.com/management.html), which is useful both during setup and later troubleshooting.

Install the `rabbitmqadmin` helper and configure the queues for your app:

```shell
wget 'http://localhost:15672/cli/rabbitmqadmin'
sudo chmod +x rabbitmqadmin
sudo mv rabbitmqadmin /usr/bin/
sudo /opt/<APP_NAME>/venv/bin/dp3-script rmq_reconfigure.sh <APP_NAME> <NUM_WORKERS>
```

If you later change the worker count, rerun `rmq_reconfigure.sh` with the new number of workers before restarting the deployment.

### 4. Configure the web server

Having the application dependencies installed and running, you can now set up the web server. DP³ can generate a basic `nginx` configuration for the application. The generated configuration assumes DP³ is the main service on the web server, so if that is not your case, adjust it to your environment.

DP³ can generate a basic `nginx` configuration for the application:

```shell
sudo $(which dp3) config nginx \
  --hostname <SERVER_HOSTNAME> \  # (1)!
  --app-name <APP_NAME> \ 
  --www-root <DIRECTORY>  # (2)!
```

1. For example `dp3.example.com`.
2. For example `/var/www/dp3`.

The generated config sets up a simple landing page and reverse proxies for the API, its docs, and the RabbitMQ management interface.

Enable and start `nginx`:

```shell
sudo systemctl enable nginx
sudo systemctl start nginx
```

If the server is protected by a firewall, open HTTP access:

```shell
sudo systemctl unmask firewalld
sudo systemctl enable firewalld
sudo systemctl start firewalld
sudo firewall-cmd --add-port 80/tcp --permanent
sudo firewall-cmd --reload
```

At this point, the landing page and the RabbitMQ management interface should be reachable, but the API is still not running under process supervision. The next step is to generate the supervisor configuration.

### 5. Generate the supervisor configuration

Generate the base supervisor configuration for the app:

```shell
sudo $(which dp3) config supervisor --config <CONFIG_DIR> --app-name <APP_NAME>
```

The generated supervisor configuration lives under `/etc/<APP_NAME>` and the process logs are written under `/var/log/<APP_NAME>`.
For more on supervisor itself, see the [supervisorctl documentation](http://supervisord.org/running.html#running-supervisorctl).

Enable and start the generated system service:

```shell
sudo systemctl enable <APP_NAME>
sudo systemctl start <APP_NAME>
```

This creates two app-specific helpers:

- `<APPNAME>ctl` wraps `supervisorctl` for the app
- `<APPNAME>sh` wraps `dp3 sh` with the app config directory already set

```shell
<APPNAME>ctl start all
<APPNAME>ctl status
<APPNAME>sh health
<APPNAME>sh entities | jq keys
```

`<APPNAME>sh entities` returns the full entity-type configuration map exposed by the API. When you only need the configured entity type names, pipe it through `jq keys`.

You can also enable shell completion for the wrapper or for `dp3` itself.
The completion is registered for the executable name seen by the shell:

- use `--command dp3` for the main `dp3` command tree
- use `--command <APPNAME>sh` for the generated wrapper

#### Session-local autocomplete 

Enable completion in the current shell session only:

=== "Bash"

    ```shell
    source <(dp3 sh completion bash --command dp3 --command <APPNAME>sh)
    ```

=== "Zsh"

    ```shell
    source <(dp3 sh completion zsh --command dp3 --command <APPNAME>sh)
    ```

=== "Fish"

    ```shell
    dp3 sh completion fish --command dp3 --command <APPNAME>sh | source
    ```

#### Persistent activation

To keep completion enabled across shell sessions, add the appropriate command to your shell startup file.

=== "Bash"

    Add one of these to `~/.bashrc`:

    ```shell
    source <(<APPNAME>sh completion bash --command <APPNAME>sh)
    source <(dp3 sh completion bash --command dp3)
    ```

=== "Zsh"

    Add one of these to `~/.zshrc`:

    ```shell
    source <(<APPNAME>sh completion zsh --command <APPNAME>sh)
    source <(dp3 sh completion zsh --command dp3)
    ```

=== "Fish"

    Write the generated completion to a Fish completion file:

    ```shell
    <APPNAME>sh completion fish --command <APPNAME>sh > ~/.config/fish/completions/<APPNAME>sh.fish
    dp3 sh completion fish --command dp3 > ~/.config/fish/completions/dp3.fish
    ```

The generated completion is config-aware. It can suggest entity types and attribute names from the resolved DP3 configuration.

### 6. Check that the deployment is healthy

Start with process status:

```shell
<APPNAME>ctl status
```

Then verify the API health endpoint. On the deployment host, prefer the generated `<APPNAME>sh` wrapper and keep `curl` as a fallback.

=== "CLI (`<APPNAME>sh`)"

    ```shell
    <APPNAME>sh health
    ```

=== "HTTP (`curl`)"

    ```shell
    curl -X GET 'http://<HOSTNAME>/' \
      -H 'Accept: application/json'
    ```

A healthy API responds with:

```json
{
  "detail": "It works!"
}
```

If your reverse proxy exposes the FastAPI docs, also check `/docs` in a browser.

## Update a deployed application safely

When the application is already live, the safest pattern is:

1. test the change locally first
2. update the code and configuration on the server
3. validate the configuration
4. restart only the processes that need the new state
5. watch status and logs after the restart

A typical update sequence looks like this:

```shell
source /opt/<APP_NAME>/venv/bin/activate
cd /opt/<APP_NAME>/<APP_DIR>
git pull  # (1)!
dp3 check <CONFIG_DIR>  # (2)!
<APPNAME>ctl restart api w:*  # (3)!
<APPNAME>ctl status
```

1. Replace this with your real deployment update mechanism if you do not deploy with `git pull`.
2. Validate the exact configuration directory used in production before restarting anything.
3. For smaller changes, restart only the affected process group if you prefer.

A few update scenarios need extra care:

- **Destructive schema changes**: if workers refuse to start and request `dp3 schema-update`, run that command and approve the required changes. Take care as this will usually delete the existing affected attribute data.
- **Only module YAML changes**: if your module uses `load_config`, you can use [`refresh_module_config`](../configuration/control.md#refresh_module_config) instead of a full worker restart.
- **Changed worker count**: rerun `dp3-script rmq_reconfigure.sh <APP_NAME> <NUM_WORKERS>` before restarting workers - [read more details here](../configuration/processing_core.md#worker-processes).

## Watch the deployment after changes

After every deployment or restart, watch the system for a few minutes. The main observability points in a DP³ deployment are supervisor status, process logs under `/var/log/<APPNAME>`, API health and data endpoints, RabbitMQ management, API datapoint logging configured in `api.yml`, event counters configured in `event_logging.yml`, and telemetry endpoints such as `/telemetry/sources_validity`.

### Process status and logs

```shell
<APPNAME>ctl status
tail -f /var/log/<APPNAME>/api.log
tail -f /var/log/<APPNAME>/worker0.log
```

For a broader scan across workers:

```shell
grep "Exception\|Error\|Traceback\|File \"" -B1 -A1 /var/log/<APPNAME>/worker*.log
```

### API-level checks

Use the API to confirm that the deployment still answers health and data requests:

- `GET /` for health
- `/docs` for interactive endpoint reference
- `dp3 sh entities | jq keys` to list the configured entity type names exposed by the running app (`dp3 sh entities` returns the full entity-type configuration map)
- `dp3 sh entity <ETYPE> id <EID> master` or `dp3 sh entity <ETYPE> id <EID> attr <ATTR> get` for spot checks against real data

When you are working directly on the deployment host, prefer `<APPNAME>sh ...` because it already knows the production config directory.

### RabbitMQ management

If you enabled the management plugin and proxy it through `nginx`, the RabbitMQ management UI is a useful place to confirm that queues exist and are behaving normally. Right after initial setup, it is normal to verify that the interface is reachable with the default `guest/guest` credentials before tightening the surrounding deployment.

If you want more detailed datapoint-level debugging during a rollout, temporarily enable `api.datapoint_logger.good_log` or `api.datapoint_logger.bad_log` and point them at writable log files. For longer-running operational visibility, configure Redis-backed event counters in `event_logging.yml` and use the API telemetry endpoints when source-validity information is relevant.

## Related pages

- [Get started with local DP³ app development](get-started.md)
- [Configuration overview](../configuration/index.md)
- [API configuration](../configuration/api.md)
- [Database configuration](../configuration/database.md)
- [Event logging configuration](../configuration/event_logging.md)
- [Processing core configuration](../configuration/processing_core.md)
- [Control configuration](../configuration/control.md)
