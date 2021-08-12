FROM postgres:11.4-alpine

RUN mkdir -p /mnt/data
COPY data/products.csv /mnt/data/products.csv
COPY data/stores.csv /mnt/data/stores.csv
COPY data/transactions.csv /mnt/data/transactions.csv
COPY psql_dump.sql /docker-entrypoint-initdb.d/
