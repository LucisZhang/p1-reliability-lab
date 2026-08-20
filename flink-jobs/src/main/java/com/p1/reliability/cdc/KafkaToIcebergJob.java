package com.p1.reliability.cdc;

import java.util.Collections;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.RowData;
import org.apache.iceberg.flink.sink.FlinkSink;

public final class KafkaToIcebergJob {
  private KafkaToIcebergJob() {}

  public static void main(String[] args) throws Exception {
    JobConfig config = JobConfig.fromArgs(args);
    IcebergTables.ensureTables(config);

    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.enableCheckpointing(config.checkpointIntervalMs(), CheckpointingMode.EXACTLY_ONCE);
    env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, Time.seconds(3)));
    env.getCheckpointConfig().setCheckpointTimeout(10 * 60 * 1000L);
    env.getCheckpointConfig()
        .setMinPauseBetweenCheckpoints(Math.max(1_000L, config.checkpointIntervalMs() / 3));
    env.getCheckpointConfig()
        .enableExternalizedCheckpoints(
            CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

    KafkaSource<OrderChange> source =
        KafkaSource.<OrderChange>builder()
            .setBootstrapServers(config.kafkaBootstrapServers())
            .setTopics(config.kafkaTopic())
            .setGroupId(config.kafkaGroupId())
            .setStartingOffsets(OffsetsInitializer.earliest())
            .setDeserializer(
                new KafkaDebeziumAvroRecordDeserializationSchema(config.schemaRegistryUrl()))
            .setProperty("commit.offsets.on.checkpoint", "true")
            .build();

    DataStream<OrderChange> changes =
        env.fromSource(source, WatermarkStrategy.noWatermarks(), "kafka-debezium-orders")
            .name("kafka-debezium-orders")
            .uid("kafka-debezium-orders-source")
            .returns(TypeInformation.of(OrderChange.class))
            .setParallelism(1);

    DataStream<RowData> currentRows =
        changes
            .filter(new CurrentTableChangeFilter())
            .name("broker-orders-current-drop-update-before")
            .uid("broker-orders-current-drop-update-before")
            .map(new CurrentRowDataMapper())
            .name("broker-orders-current-rowdata")
            .uid("broker-orders-current-rowdata")
            .returns(TypeInformation.of(RowData.class));

    FlinkSink.forRowData(currentRows)
        .tableLoader(IcebergTables.currentTableLoader(config))
        .upsert(true)
        .equalityFieldColumns(Collections.singletonList("order_id"))
        .uidPrefix("broker-iceberg-orders-current")
        .append();

    DataStream<RowData> changelogRows =
        changes
            .map(new ChangelogRowDataMapper())
            .name("broker-orders-changelog-rowdata")
            .uid("broker-orders-changelog-rowdata")
            .returns(TypeInformation.of(RowData.class));

    FlinkSink.forRowData(changelogRows)
        .tableLoader(IcebergTables.changelogTableLoader(config))
        .uidPrefix("broker-iceberg-orders-changelog")
        .append();

    env.execute("kafka-debezium-to-iceberg-v2-upsert");
  }
}
