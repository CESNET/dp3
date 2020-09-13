#!/bin/bash

# Check there are two parameters and the second one is integer
if [[ "$#" != 2 || ! "$2" =~ ^[0-9]+$ ]]; then
  echo "Usage: $0 APPNAME NUM_WORKERS" >&2
  exit 1
fi
APPNAME=$1
N=$2

# TODO: Check there are no workers running

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

# Declare exchanges (normal and priority)
rabbitmqadmin declare exchange "name=${APPNAME}-main-task-exchange" type=direct durable=true
rabbitmqadmin declare exchange "name=${APPNAME}-priority-task-exchange" type=direct durable=true

# Declare queues for N workers
for i in $(seq 0 $((N-1)))
do
  rabbitmqadmin declare queue "name=${APPNAME}-worker-$i" durable=true 'arguments={"x-max-length": 100, "x-overflow": "reject-publish"}'
  rabbitmqadmin declare queue "name=${APPNAME}-worker-$i-pri" durable=true
done

# Bind queues to exchanges
for i in $(seq 0 $((N-1)))
do
  rabbitmqadmin declare binding "source=${APPNAME}-main-task-exchange" "destination=${APPNAME}-worker-$i" routing_key=$i
  rabbitmqadmin declare binding "source=${APPNAME}-priority-task-exchange" "destination=${APPNAME}-worker-$i-pri" routing_key=$i
done