package com.p1.reliability.cdc;

import java.util.Collections;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.RowData;
import org.apache.iceberg.flink.sink.FlinkSink;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;

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

    KafkaSource<BrokerRecord> source =
        KafkaSource.<BrokerRecord>builder()
            .setBootstrapServers(config.kafkaBootstrapServers())
            .setTopics(config.kafkaTopic())
            .setGroupId(config.kafkaGroupId())
            .setStartingOffsets(startingOffsets(config))
            .setDeserializer(
                new KafkaDebeziumAvroRecordDeserializationSchema(config.schemaRegistryUrl()))
            .setProperty("commit.offsets.on.checkpoint", "true")
            .build();

    DataStream<BrokerRecord> brokerRecords =
        env.fromSource(source, WatermarkStrategy.noWatermarks(), "kafka-debezium-orders")
            .name("kafka-debezium-orders")
            .uid("kafka-debezium-orders-source")
            .returns(TypeInformation.of(BrokerRecord.class))
            .setParallelism(1);

    // Keep the decode/current projection on the source's single ordered channel. Expanding this
    // segment before keyed routing introduces a rebalance whose channel merge can reorder two
    // records that Kafka delivered in order for the same primary key.
    DataStream<OrderChange> changes =
        brokerRecords
            .filter(BrokerRecord::isDecoded)
            .name("broker-orders-valid-records")
            .uid("broker-orders-valid-records")
            .setParallelism(1)
            .map(record -> record.change)
            .name("broker-orders-decoded-change")
            .uid("broker-orders-decoded-change")
            .returns(TypeInformation.of(OrderChange.class))
            .setParallelism(1);

    DataStream<String> deadLetters =
        brokerRecords
            .filter(BrokerRecord::isDeadLetter)
            .name("broker-orders-poison-records")
            .uid("broker-orders-poison-records")
            .map(record -> record.deadLetterJson)
            .name("broker-orders-dead-letter-json")
            .uid("broker-orders-dead-letter-json")
            .returns(TypeInformation.of(String.class));

    KafkaSink<String> deadLetterSink =
        KafkaSink.<String>builder()
            .setBootstrapServers(config.kafkaBootstrapServers())
            .setRecordSerializer(
                KafkaRecordSerializationSchema.builder()
                    .setTopic(config.kafkaDlqTopic())
                    .setValueSerializationSchema(new SimpleStringSchema())
                    .build())
            .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
            .build();

    deadLetters
        .sinkTo(deadLetterSink)
        .name("broker-orders-dead-letter-sink")
        .uid("broker-orders-dead-letter-sink");

    DataStream<OrderChange> currentChanges =
        changes
            .filter(new CurrentTableChangeFilter())
            .name("broker-orders-current-drop-update-before")
            .uid("broker-orders-current-drop-update-before")
            .setParallelism(1);

    if (config.kafkaReplayCoalesceMs() > 0L) {
      currentChanges =
          currentChanges
              .keyBy(change -> change.orderId)
              .process(new LatestPerKeyReplayCoalescer(config.kafkaReplayCoalesceMs()))
              .name("broker-orders-replay-latest-per-key")
              .uid("broker-orders-replay-latest-per-key");
    }

    DataStream<RowData> currentRows =
        currentChanges
            .map(new CurrentRowDataMapper())
            .name("broker-orders-current-rowdata")
            .uid("broker-orders-current-rowdata")
            .returns(TypeInformation.of(RowData.class))
            .setParallelism(config.kafkaReplayCoalesceMs() > 0L ? 2 : 1);

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

  private static OffsetsInitializer startingOffsets(JobConfig config) {
    String mode = config.kafkaStartingOffsets();
    if ("earliest".equals(mode)) {
      return OffsetsInitializer.earliest();
    }
    if ("committed".equals(mode)) {
      return OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST);
    }
    if ("timestamp".equals(mode)) {
      if (config.kafkaStartTimestampMs() < 0L) {
        throw new IllegalArgumentException(
            "--kafka-start-timestamp-ms is required for timestamp starting offsets");
      }
      return OffsetsInitializer.timestamp(config.kafkaStartTimestampMs());
    }
    throw new IllegalArgumentException(
        "--kafka-starting-offsets must be earliest, committed, or timestamp; got " + mode);
  }
}
