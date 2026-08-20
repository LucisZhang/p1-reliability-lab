FROM quay.io/debezium/connect:3.2.4.Final

# Debezium 2+ intentionally omits Confluent's converter. Keep the complete,
# host-Maven-resolved 7.9.8 runtime closure in one isolated Connect plugin directory.
COPY target/avro-plugin/ /kafka/connect/confluent-avro/
