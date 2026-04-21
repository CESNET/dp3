# Scripts

DP³ provides a set of scripts for to help you with various tasks.
They are exposed using the `dp3-script` command. You can list the available scripts by running 

```shell
dp3-script ls
```

And you can run the script by running

```shell
dp3-script <script-name> [args]
```

## Script requirements

Some of the scripts which manipulate the datapoint logs require pandas to be installed.
All the script requirements are placed into an optional `scripts` dependency, which you can install by running

```shell
pip install dp-cubed[scripts]
```

## Notable scripts

### `dp3-script dummy_sender.py`

Simple datapoint sender script for testing local DP3 instance. 
Reads the JSON datapoint log from the API and sends it to the specified DP3 instance.

### `dp3-script rmq_reconfigure.sh`

Used during RabbitMQ (re)configuration, sets up the appropriate queues and exchanges for 
the specified number of workers. For details, see [How to deploy a DP³ application](howto/deploy-app.md#update-a-deployed-application-safely) and [Processing Core](configuration/processing_core.md#worker-processes).