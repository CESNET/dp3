# syntax=docker/dockerfile:1
FROM rabbitmq:3-management

# Define environment variable indicating file in which the process id is placed for use by `rabbitmqctl wait`
ENV RABBITMQ_PID_FILE=/var/lib/rabbitmq/mnesia/rabbitmq

# Add custom configuration script
COPY --chmod=755 init-rmq.sh /init-rmq.sh

CMD ["/init-rmq.sh"]
