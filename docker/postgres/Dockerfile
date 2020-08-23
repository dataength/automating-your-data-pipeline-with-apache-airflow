FROM postgres:11.4-alpine

COPY init-hive-db.sh /docker-entrypoint-initdb.d/init-hive-db.sh

RUN chmod +x /docker-entrypoint-initdb.d/init-hive-db.sh

EXPOSE 5432