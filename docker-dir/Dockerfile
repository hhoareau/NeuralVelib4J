FROM afritzler/openjdk-arm:8

ENV hadoop_ver 2.6.0
ENV spark_ver 2.1.0

# Get Hadoop from US Apache mirror and extract just the native
# libs. (Until we care about running HDFS with these containers, this
# is all we need.)
RUN mkdir -p /opt && \
    cd /opt && \
    curl http://www.us.apache.org/dist/hadoop/common/hadoop-2.6.0/hadoop-2.6.0.tar.gz | \
        tar -zx hadoop-2.6.0/lib/native && \
    ln -s hadoop-2.6.0 hadoop && \
    echo Hadoop 2.6.0 native libraries installed in /opt/hadoop/lib/native

# Get Spark from US Apache mirror.
RUN mkdir -p /opt && \
    cd /opt && \
    curl http://www.us.apache.org/dist/spark/spark-2.1.0/spark-2.1.0-bin-hadoop2.6.tgz | tar -zx && \
    ln -s spark-2.1.0-bin-hadoop2.6 spark && \
    echo Spark 2.1.0 installed in /opt

RUN cd /opt/spark && ls

# Add the GCS connector.
RUN cd /opt/spark/lib && \
    curl -O https://storage.googleapis.com/hadoop-lib/gcs/gcs-connector-latest-hadoop2.jar

# if numpy is installed on a driver it needs to be installed on all
# workers, so install it everywhere
RUN apt-get update && \
    apt-get install -y python-numpy && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

ADD log4j.properties /opt/spark/conf/log4j.properties
ADD start-common.sh start-worker start-master /
ADD core-site.xml /opt/spark/conf/core-site.xml
ADD spark-defaults.conf /opt/spark/conf/spark-defaults.conf
ENV PATH $PATH:/opt/spark/bin