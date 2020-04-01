N=$1

echo "** Removing all processing platform exchanges and queues **"

exchange_list=$(rabbitmqadmin list exchanges name -f tsv | grep "^pp-.*-task-exchange")
for q in $exchange_list
do
  rabbitmqadmin delete exchange name=$q
done

queue_list=$(rabbitmqadmin list queues name -f tsv | grep "^pp-worker-")
for q in $queue_list
do
  rabbitmqadmin delete queue name=$q
done

if [[ "$N" -eq 0 ]]; then
  exit 0
fi

echo "** Setting up exchanges and queues for $N workers **"

# Declare exchanges (normal and priority)
rabbitmqadmin declare exchange name=pp-main-task-exchange type=direct durable=true
rabbitmqadmin declare exchange name=pp-priority-task-exchange type=direct durable=true

# Declare queues for N workers
for i in $(seq 0 $(($N-1)))
do
  rabbitmqadmin declare queue name=pp-worker-$i durable=true 'arguments={"x-max-length": 100, "x-overflow": "reject-publish"}'
  rabbitmqadmin declare queue name=pp-worker-$i-pri durable=true
done

# Bind queues to exchanges
for i in $(seq 0 $(($N-1)))
do
  rabbitmqadmin declare binding source=pp-main-task-exchange destination=pp-worker-$i routing_key=$i
  rabbitmqadmin declare binding source=pp-priority-task-exchange destination=pp-worker-$i-pri routing_key=$i
done