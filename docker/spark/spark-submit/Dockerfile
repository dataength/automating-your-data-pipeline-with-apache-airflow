FROM spark-base:latest

ENV SPARK_MASTER_URL="spark://spark-master:7077"
ENV SPARK_SUBMIT_ARGS=""
ENV SPARK_APPLICATION_ARGS=""

COPY ./spark-submit.sh .

RUN chmod +x spark-submit.sh

CMD [ "spark-submit.sh" ]