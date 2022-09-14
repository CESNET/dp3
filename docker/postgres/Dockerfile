# syntax=docker/dockerfile:1
FROM postgres

# Scripts in /docker-entrypoint-initdb.d directory are executed by postgress at startup
COPY init-db.sh /docker-entrypoint-initdb.d/init-db.sh
RUN chmod +x /docker-entrypoint-initdb.d/init-db.sh
