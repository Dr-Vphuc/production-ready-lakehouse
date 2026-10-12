FROM apache/spark:3.5.9-java17-python3

# ---- (1) Khai báo phiên bản ở một chỗ ----
ARG ICEBERG_VERSION=1.11.0
ARG SPARK_MINOR=3.5
ARG SCALA_VERSION=2.12
ARG MAVEN_ICEBERG=https://repo1.maven.org/maven2/org/apache/iceberg

# ---- (2) Tải jar vào classpath của Spark ----
ADD --chmod=644 ${MAVEN_ICEBERG}/iceberg-spark-runtime-${SPARK_MINOR}_${SCALA_VERSION}/${ICEBERG_VERSION}/iceberg-spark-runtime-${SPARK_MINOR}_${SCALA_VERSION}-${ICEBERG_VERSION}.jar /opt/spark/jars/
ADD --chmod=644 ${MAVEN_ICEBERG}/iceberg-aws-bundle/${ICEBERG_VERSION}/iceberg-aws-bundle-${ICEBERG_VERSION}.jar /opt/spark/jars/

# ---- (3) Không thêm gì nữa ----
