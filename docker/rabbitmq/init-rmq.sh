#!/bin/bash
# Usage: export DP3_APP_NAME=<appname>; export DP3_WORKER_NUM=<N>; ./init-rmq.sh

# Check that required environment variables are set
if [ -z "$DP3_APP_NAME" ]; then
  echo "Missing environment variable: DP3_APP_NAME (expected value is application name)";
else
  APPNAME=$DP3_APP_NAME
fi
if [ -z "$DP3_WORKER_NUM" ]; then
  echo "Missing environment variable: DP3_WORKER_NUM (expected value is number of workers)";
else
  N=$DP3_WORKER_NUM
fi
if [ -z "$DP3_APP_NAME" ] || [ -z "$DP3_WORKER_NUM" ]; then
  exit 1
fi

# set -m enables job control, which we need to bring the rabbitmq-server process to foreground after configuration
set -m

# start server in background and wait until initialized
rabbitmq-server &
rabbitmqctl wait --timeout 60 "$RABBITMQ_PID_FILE"

echo "** Removing all DP3 exchanges and queues for app '${APPNAME}' **"

exchange_list=$(rabbitmqadmin list exchanges name -f tsv | grep "^${APPNAME}-.*-task-exchange")
for q in $exchange_list
do
  rabbitmqadmin delete exchange "name=$q"
done

queue_list=$(rabbitmqadmin list queues name -f tsv | grep "^${APPNAME}-worker-")
for q in $queue_list
do
  rabbitmqadmin delete queue "name=$q"
done

if [[ "$N" -eq 0 ]]; then
  exit 0
fi

echo "** Setting up exchanges and queues for $N workers **"

# Declare exchanges for datapoint tasks (normal and priority) and for snapshots
rabbitmqadmin declare exchange "name=${APPNAME}-main-task-exchange" type=direct durable=true
rabbitmqadmin declare exchange "name=${APPNAME}-priority-task-exchange" type=direct durable=true

rabbitmqadmin declare exchange "name=${APPNAME}-main-snapshot-exchange" type=direct durable=true

# Declare exchange for control tasks
rabbitmqadmin declare exchange "name=${APPNAME}-control-exchange" type=direct durable=true

# Declare queues for N workers
for i in $(seq 0 $((N-1)))
do
  rabbitmqadmin declare queue "name=${APPNAME}-worker-$i" durable=true 'arguments={"x-max-length": 10000, "x-overflow": "reject-publish"}'
  rabbitmqadmin declare queue "name=${APPNAME}-worker-$i-pri" durable=true

  rabbitmqadmin declare queue "name=${APPNAME}-worker-$i-snapshots" durable=true
  rabbitmqadmin declare queue "name=${APPNAME}-worker-$i-control" durable=true
done

# Bind queues to exchanges
for i in $(seq 0 $((N-1)))
do
  rabbitmqadmin declare binding "source=${APPNAME}-main-task-exchange" "destination=${APPNAME}-worker-$i" routing_key=$i
  rabbitmqadmin declare binding "source=${APPNAME}-priority-task-exchange" "destination=${APPNAME}-worker-$i-pri" routing_key=$i

  rabbitmqadmin declare binding "source=${APPNAME}-main-snapshot-exchange" "destination=${APPNAME}-worker-$i-snapshots" routing_key=$i
  rabbitmqadmin declare binding "source=${APPNAME}-control-exchange" "destination=${APPNAME}-worker-$i-control" routing_key=$i
done

# bring rabbitmq-server process to foreground
fg
