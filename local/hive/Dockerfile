FROM apache/hive:3.1.3

USER root

# Install wget and download S3 JARs
RUN apt-get update && apt-get install -y wget && \
    wget -q -O /opt/hive/lib/hadoop-aws-3.1.0.jar https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.1.0/hadoop-aws-3.1.0.jar && \
    wget -q -O /opt/hive/lib/aws-java-sdk-bundle-1.11.271.jar https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.11.271/aws-java-sdk-bundle-1.11.271.jar && \
    rm -rf /var/lib/apt/lists/*

# Copy Hadoop and Hive configuration for S3
COPY core-site.xml /opt/hadoop/etc/hadoop/core-site.xml
COPY hive-site.xml /opt/hive/conf/hive-site.xml

USER hive
