FROM maven:3.9.9-eclipse-temurin-17 AS avro-converter

WORKDIR /build
COPY avro-converter-pom.xml pom.xml
RUN mvn -B -ntp \
    org.apache.maven.plugins:maven-dependency-plugin:3.8.1:copy-dependencies \
    -DincludeScope=runtime \
    -DoutputDirectory=/opt/confluent-avro-converter

FROM quay.io/debezium/connect:3.2.4.Final

# Debezium 2+ intentionally omits Confluent's converter. Keep the complete,
# Maven-resolved 7.9.8 runtime closure in one isolated Connect plugin directory.
COPY --from=avro-converter /opt/confluent-avro-converter/ /kafka/connect/confluent-avro/
