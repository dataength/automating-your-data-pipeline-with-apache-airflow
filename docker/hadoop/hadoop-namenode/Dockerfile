FROM hadoop-base:latest
LABEL maintainer="MarcLamberti"

ENV HDFS_CONF_DFS_NAMENODE_DATA_DIR=file:///hadoop/dfs/name

RUN mkdir -p /hadoop/dfs/name

COPY ./start-namenode.sh ./start-namenode.sh

RUN chmod +x ./start-namenode.sh

EXPOSE 9870

CMD [ "./start-namenode.sh" ]