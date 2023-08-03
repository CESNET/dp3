# Processing core

Processing core's configuration in `processing_core.yml` file looks like this:

```yaml
msg_broker:
  host: localhost
  port: 5672
  virtual_host: /
  username: dp3_user
  password: dp3_password
worker_processes: 2
worker_threads: 16
modules_dir: "../dp3_modules"
enabled_modules:
  - "module_one"
  - "module_two"
```

## Message broker

Message broker section describes connection details to RabbitMQ (or compatible) broker.

| Parameter      | Data-type | Default value | Description                                      |
|----------------|-----------|---------------|--------------------------------------------------|
| `host`         | string    | `localhost`   | IP address or hostname for connection to broker. |
| `port`         | int       | 5672          | Listening port of broker.                        |
| `virtual_host` | string    | `/`           | Virtual host for connection to broker.           |
| `username`     | string    | `guest`       | Username for connection to broker.               |
| `password`     | string    | `guest`       | Password for connection to broker.               |

## Worker processes

Number of worker processes. This has to be at least 1.

If changing number of worker processes, the following process must be followed:

1. stop all inputs writing to task queue (e.g. API)
2. when all queues are empty, stop all workers
3. reconfigure queues in RabbitMQ using script found in `/scripts/rmq_reconfigure.sh`
4. change the settings here and in init scripts for worker processes (e.g. supervisor)
5. reload workers (e.g. using `supervisorctl`) and start all inputs again

## Worker threads

Number of worker threads per process.

This may be higher than number of CPUs, because this is not primarily intended
to utilize computational power of multiple CPUs (which Python cannot do well
anyway due to the GIL), but to mask long I/O operations (e.g. queries to
external services via network).

## Modules directory

Path to directory with plug-in (secondary) modules.

Relative path is evaluated relative to location of this configuration file.

## Enabled modules

List of plug-in modules which should be enabled in processing pipeline.

Name of module filename without `.py` extension must be used!
