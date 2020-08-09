FROM hadoop-base:latest
LABEL maintainer="MarcLamberti"

ENV HDFS_CONF_DFS_DATANODE_DATA_DIR=file:///hadoop/dfs/data

RUN mkdir -p /hadoop/dfs/data

COPY start-datanode.sh start-datanode.sh

RUN chmod +x start-datanode.sh

EXPOSE 9864

CMD [ "./start-datanode.sh" ]