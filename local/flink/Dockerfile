FROM flink:1.16-scala_2.12

USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends python3 python3-pip python3-venv \
    && rm -rf /var/lib/apt/lists/* \
    && ln -sf /usr/bin/python3 /usr/bin/python

# Install minimal Python packages
RUN pip3 install --no-cache-dir kafka-python protobuf

# Download all required JARs for Kafka, Iceberg, and S3
# Using versions that are known to work together
RUN set -eux; \
    mkdir -p /opt/flink/lib /opt/flink/plugins/s3-fs-hadoop; \
    # Flink Kafka connector 
    curl -fSL https://repo1.maven.org/maven2/org/apache/flink/flink-connector-kafka/1.16.3/flink-connector-kafka-1.16.3.jar -o /opt/flink/lib/flink-connector-kafka-1.16.3.jar; \
    # Kafka clients
    curl -fSL https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.2.3/kafka-clients-3.2.3.jar -o /opt/flink/lib/kafka-clients-3.2.3.jar; \
    # Iceberg Flink runtime
    curl -fSL https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-flink-runtime-1.16/1.4.2/iceberg-flink-runtime-1.16-1.4.2.jar -o /opt/flink/lib/iceberg-flink-runtime-1.16-1.4.2.jar; \
    # Iceberg AWS bundle for S3FileIO (avoids Hadoop S3A dependency issues)
    curl -fSL https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-aws-bundle/1.4.2/iceberg-aws-bundle-1.4.2.jar -o /opt/flink/lib/iceberg-aws-bundle-1.4.2.jar; \
    # Flink S3 filesystem plugin 
    curl -fSL https://repo1.maven.org/maven2/org/apache/flink/flink-s3-fs-hadoop/1.16.3/flink-s3-fs-hadoop-1.16.3.jar -o /opt/flink/plugins/s3-fs-hadoop/flink-s3-fs-hadoop-1.16.3.jar; \
    # Hive Metastore dependencies
    curl -fSL https://repo1.maven.org/maven2/org/apache/hive/hive-exec/3.1.3/hive-exec-3.1.3.jar -o /opt/flink/lib/hive-exec-3.1.3.jar; \
    curl -fSL https://repo1.maven.org/maven2/org/apache/hive/hive-standalone-metastore/3.1.3/hive-standalone-metastore-3.1.3.jar -o /opt/flink/lib/hive-standalone-metastore-3.1.3.jar; \
    curl -fSL https://repo1.maven.org/maven2/org/apache/thrift/libthrift/0.12.0/libthrift-0.12.0.jar -o /opt/flink/lib/libthrift-0.12.0.jar; \
    curl -fSL https://repo1.maven.org/maven2/org/apache/thrift/libfb303/0.9.3/libfb303-0.9.3.jar -o /opt/flink/lib/libfb303-0.9.3.jar; \
    # Hadoop shaded uber JAR for Hadoop config classes
    curl -fSL https://repo1.maven.org/maven2/org/apache/flink/flink-shaded-hadoop-2-uber/2.8.3-10.0/flink-shaded-hadoop-2-uber-2.8.3-10.0.jar -o /opt/flink/lib/flink-shaded-hadoop-2-uber-2.8.3-10.0.jar

# Copy core-site.xml for S3 config
COPY core-site.xml /opt/flink/conf/core-site.xml

USER flink
