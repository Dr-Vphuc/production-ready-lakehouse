FROM apache/spark:3.5.9-java17-scala AS runtime

# ---- (1) Khai báo phiên bản ở một chỗ ----
ARG ICEBERG_VERSION=1.11.0
ARG SPARK_MINOR=3.5
ARG SCALA_VERSION=2.12
ARG MAVEN_ICEBERG=https://repo1.maven.org/maven2/org/apache/iceberg

# ---- (2) Tải jar vào classpath của Spark ----
ADD --chmod=644 ${MAVEN_ICEBERG}/iceberg-spark-runtime-${SPARK_MINOR}_${SCALA_VERSION}/${ICEBERG_VERSION}/iceberg-spark-runtime-${SPARK_MINOR}_${SCALA_VERSION}-${ICEBERG_VERSION}.jar /opt/spark/jars/
ADD --chmod=644 ${MAVEN_ICEBERG}/iceberg-aws-bundle/${ICEBERG_VERSION}/iceberg-aws-bundle-${ICEBERG_VERSION}.jar /opt/spark/jars/

# ---- Stage build: biên dịch job Scala ----
FROM sbtscala/scala-sbt:eclipse-temurin-17.0.20_8_1.13.0_2.12.21 AS build
WORKDIR /src
COPY jobs/build.sbt ./
COPY jobs/project/build.properties ./project/
RUN sbt update
COPY jobs/src ./src
RUN sbt package

# ---- Stage jobs: runtime + jar ----
FROM runtime AS jobs

# Kafka connector cho Structured Streaming. Chỉ image jobs cần, nên không đụng tới
# image runtime của master/worker/thrift (đổi image đó sẽ làm các container này bị tạo lại).
ARG SPARK_VERSION=3.5.9
ARG SCALA_VERSION=2.12
ARG KAFKA_CLIENTS_VERSION=3.4.1
ARG COMMONS_POOL2_VERSION=2.11.1
ARG MAVEN=https://repo1.maven.org/maven2
ADD --chmod=644 ${MAVEN}/org/apache/spark/spark-sql-kafka-0-10_${SCALA_VERSION}/${SPARK_VERSION}/spark-sql-kafka-0-10_${SCALA_VERSION}-${SPARK_VERSION}.jar /opt/spark/jars/
ADD --chmod=644 ${MAVEN}/org/apache/spark/spark-token-provider-kafka-0-10_${SCALA_VERSION}/${SPARK_VERSION}/spark-token-provider-kafka-0-10_${SCALA_VERSION}-${SPARK_VERSION}.jar /opt/spark/jars/
ADD --chmod=644 ${MAVEN}/org/apache/kafka/kafka-clients/${KAFKA_CLIENTS_VERSION}/kafka-clients-${KAFKA_CLIENTS_VERSION}.jar /opt/spark/jars/
ADD --chmod=644 ${MAVEN}/org/apache/commons/commons-pool2/${COMMONS_POOL2_VERSION}/commons-pool2-${COMMONS_POOL2_VERSION}.jar /opt/spark/jars/

# Thư mục checkpoint của job streaming; volume mount vào đây sẽ nhận quyền của user spark
RUN mkdir -p /opt/spark/checkpoints

COPY --from=build /src/target/scala-2.12/*.jar /opt/spark/apps/lakehouse-jobs.jar