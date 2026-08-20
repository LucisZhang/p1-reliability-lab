package com.p1.reliability.cdc;

import java.util.HashMap;
import java.util.Map;
import org.apache.flink.api.java.utils.ParameterTool;

public final class JobConfig {
  private static final String DEFAULT_MYSQL_HOST = "mysql";
  private static final int DEFAULT_MYSQL_PORT = 3306;
  private static final String DEFAULT_MYSQL_DATABASE = "cdc_lab";
  private static final String DEFAULT_MYSQL_USER = "cdc";
  private static final String DEFAULT_MYSQL_PASSWORD = "cdc_pw";
  private static final String DEFAULT_CATALOG_NAME = "lab_iceberg";
  private static final String DEFAULT_CATALOG_DATABASE = "iceberg_catalog";
  private static final String DEFAULT_ICEBERG_DATABASE = "cdc_lab";
  private static final String DEFAULT_WAREHOUSE = "s3://warehouse/iceberg";
  private static final String DEFAULT_S3_ENDPOINT = "http://minio:9000";
  private static final String DEFAULT_S3_ACCESS_KEY = "minioadmin";
  private static final String DEFAULT_S3_SECRET_KEY = "minioadmin";
  private static final String DEFAULT_S3_REGION = "us-east-1";
  private static final String DEFAULT_SERVER_ID = "5400-5404";
  private static final long DEFAULT_CHECKPOINT_INTERVAL_MS = 30_000L;
  private static final long DEFAULT_TASK_CRASH_EVENT_ID = -1L;
  private static final String DEFAULT_TASK_CRASH_MARKER_PATH =
      "/tmp/p1-reliability-task-crash-once.marker";
  private static final long DEFAULT_CHECKPOINT_COMPLETE_FAULT_EVENT_ID = -1L;
  private static final String DEFAULT_CHECKPOINT_COMPLETE_FAULT_MARKER_PATH =
      "/tmp/p1-reliability-checkpoint-complete-fault-once.marker";
  private static final String DEFAULT_WRITE_TARGET_FILE_SIZE_BYTES = "";
  private static final String DEFAULT_WRITE_PARQUET_ROW_GROUP_SIZE_BYTES = "";
  private static final String DEFAULT_COMMIT_MANIFEST_MIN_COUNT_TO_MERGE = "";
  private static final long DEFAULT_BACKPRESSURE_SLEEP_MS = 0L;
  private static final long DEFAULT_ALIGNMENT_PROBE_SLEEP_MS = 0L;
  private static final String DEFAULT_KAFKA_BOOTSTRAP_SERVERS = "kafka:9092";
  private static final String DEFAULT_KAFKA_TOPIC = "broker.cdc_lab.orders";
  private static final String DEFAULT_KAFKA_GROUP_ID = "p1-broker-parity-b1";
  private static final String DEFAULT_SCHEMA_REGISTRY_URL = "http://schema-registry:8081";

  private final ParameterTool parameters;

  private JobConfig(ParameterTool parameters) {
    this.parameters = parameters;
  }

  public static JobConfig fromArgs(String[] args) {
    return new JobConfig(ParameterTool.fromArgs(args));
  }

  public String mysqlHost() {
    return parameters.get("mysql-host", DEFAULT_MYSQL_HOST);
  }

  public int mysqlPort() {
    return parameters.getInt("mysql-port", DEFAULT_MYSQL_PORT);
  }

  public String mysqlDatabase() {
    return parameters.get("mysql-database", DEFAULT_MYSQL_DATABASE);
  }

  public String mysqlUser() {
    return parameters.get("mysql-user", DEFAULT_MYSQL_USER);
  }

  public String mysqlPassword() {
    return parameters.get("mysql-password", DEFAULT_MYSQL_PASSWORD);
  }

  public String mysqlTable() {
    return mysqlDatabase() + ".orders";
  }

  public String catalogName() {
    return parameters.get("iceberg-catalog-name", DEFAULT_CATALOG_NAME);
  }

  public String catalogDatabase() {
    return parameters.get("iceberg-catalog-database", DEFAULT_CATALOG_DATABASE);
  }

  public String icebergDatabase() {
    return parameters.get("iceberg-database", DEFAULT_ICEBERG_DATABASE);
  }

  public String warehouse() {
    return parameters.get("iceberg-warehouse", DEFAULT_WAREHOUSE);
  }

  public String catalogUri() {
    return parameters.get(
        "iceberg-catalog-uri",
        "jdbc:mysql://"
            + mysqlHost()
            + ":"
            + mysqlPort()
            + "/"
            + catalogDatabase()
            + "?useSSL=false&allowPublicKeyRetrieval=true");
  }

  public String s3Endpoint() {
    return parameters.get("s3-endpoint", DEFAULT_S3_ENDPOINT);
  }

  public String s3AccessKey() {
    return parameters.get("s3-access-key", DEFAULT_S3_ACCESS_KEY);
  }

  public String s3SecretKey() {
    return parameters.get("s3-secret-key", DEFAULT_S3_SECRET_KEY);
  }

  public String s3Region() {
    return parameters.get("s3-region", DEFAULT_S3_REGION);
  }

  public String serverId() {
    return parameters.get("server-id", DEFAULT_SERVER_ID);
  }

  public long checkpointIntervalMs() {
    return parameters.getLong("checkpoint-interval-ms", DEFAULT_CHECKPOINT_INTERVAL_MS);
  }

  public long taskCrashEventId() {
    return parameters.getLong("task-crash-event-id", DEFAULT_TASK_CRASH_EVENT_ID);
  }

  public String taskCrashMarkerPath() {
    return parameters.get("task-crash-marker-path", DEFAULT_TASK_CRASH_MARKER_PATH);
  }

  public long checkpointCompleteFaultEventId() {
    return parameters.getLong(
        "checkpoint-complete-fault-event-id", DEFAULT_CHECKPOINT_COMPLETE_FAULT_EVENT_ID);
  }

  public String checkpointCompleteFaultMarkerPath() {
    return parameters.get(
        "checkpoint-complete-fault-marker-path", DEFAULT_CHECKPOINT_COMPLETE_FAULT_MARKER_PATH);
  }

  public String writeTargetFileSizeBytes() {
    return parameters.get("write-target-file-size-bytes", DEFAULT_WRITE_TARGET_FILE_SIZE_BYTES);
  }

  public String writeParquetRowGroupSizeBytes() {
    return parameters.get(
        "write-parquet-row-group-size-bytes", DEFAULT_WRITE_PARQUET_ROW_GROUP_SIZE_BYTES);
  }

  public String commitManifestMinCountToMerge() {
    return parameters.get(
        "commit-manifest-min-count-to-merge", DEFAULT_COMMIT_MANIFEST_MIN_COUNT_TO_MERGE);
  }

  public long backpressureSleepMs() {
    return parameters.getLong("backpressure-sleep-ms", DEFAULT_BACKPRESSURE_SLEEP_MS);
  }

  public long alignmentProbeSleepMs() {
    return parameters.getLong("alignment-probe-sleep-ms", DEFAULT_ALIGNMENT_PROBE_SLEEP_MS);
  }

  public String kafkaBootstrapServers() {
    return parameters.get("kafka-bootstrap-servers", DEFAULT_KAFKA_BOOTSTRAP_SERVERS);
  }

  public String kafkaTopic() {
    return parameters.get("kafka-topic", DEFAULT_KAFKA_TOPIC);
  }

  public String kafkaGroupId() {
    return parameters.get("kafka-group-id", DEFAULT_KAFKA_GROUP_ID);
  }

  public String schemaRegistryUrl() {
    return parameters.get("schema-registry-url", DEFAULT_SCHEMA_REGISTRY_URL);
  }

  public Map<String, String> icebergCatalogProperties() {
    Map<String, String> properties = new HashMap<>();
    properties.put("type", "iceberg");
    properties.put("catalog-type", "jdbc");
    properties.put("uri", catalogUri());
    properties.put("jdbc.user", mysqlUser());
    properties.put("jdbc.password", mysqlPassword());
    properties.put("warehouse", warehouse());
    properties.put("io-impl", "org.apache.iceberg.aws.s3.S3FileIO");
    properties.put("s3.endpoint", s3Endpoint());
    properties.put("s3.path-style-access", "true");
    properties.put("s3.access-key-id", s3AccessKey());
    properties.put("s3.secret-access-key", s3SecretKey());
    properties.put("client.region", s3Region());
    return properties;
  }

  public String icebergCatalogDdl() {
    return "CREATE CATALOG "
        + catalogName()
        + " WITH ("
        + "'type'='iceberg',"
        + "'catalog-impl'='org.apache.iceberg.jdbc.JdbcCatalog',"
        + "'uri'='"
        + catalogUri()
        + "',"
        + "'jdbc.user'='"
        + mysqlUser()
        + "',"
        + "'jdbc.password'='"
        + mysqlPassword()
        + "',"
        + "'warehouse'='"
        + warehouse()
        + "',"
        + "'io-impl'='org.apache.iceberg.aws.s3.S3FileIO',"
        + "'s3.endpoint'='"
        + s3Endpoint()
        + "',"
        + "'s3.path-style-access'='true',"
        + "'s3.access-key-id'='"
        + s3AccessKey()
        + "',"
        + "'s3.secret-access-key'='"
        + s3SecretKey()
        + "',"
        + "'client.region'='"
        + s3Region()
        + "')";
  }
}
