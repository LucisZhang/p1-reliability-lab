package com.p1.reliability.cdc;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Comparator;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;

/** Binary-safe producer/consumer used only by deterministic broker failure drills. */
public final class KafkaBinaryAdmin {
  private static final String PRODUCE = "produce";
  private static final String CONSUME = "consume";

  private KafkaBinaryAdmin() {}

  public static void main(String[] args) throws Exception {
    int commandIndex = commandIndex(args);
    if (commandIndex < 0) {
      throw new IllegalArgumentException(
          "Usage: KafkaBinaryAdmin {produce|consume} --bootstrap-servers host:port --topic name");
    }
    if (PRODUCE.equals(args[commandIndex])) {
      produce(args);
    } else {
      consume(args);
    }
  }

  private static void produce(String[] args) throws Exception {
    String bootstrap = requiredOption(args, "--bootstrap-servers");
    String topic = requiredOption(args, "--topic");
    int partition = Integer.parseInt(option(args, "--partition", "-1"));
    long timestamp = Long.parseLong(option(args, "--timestamp-ms", "-1"));
    byte[] key = decodeNullable(requiredOption(args, "--key-base64"));
    byte[] value = decodeNullable(requiredOption(args, "--value-base64"));

    Properties properties = new Properties();
    properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
    properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
    properties.put(
        ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
    properties.put(ProducerConfig.ACKS_CONFIG, "all");
    properties.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");

    Integer selectedPartition = partition < 0 ? null : partition;
    Long selectedTimestamp = timestamp < 0 ? null : timestamp;
    ProducerRecord<byte[], byte[]> record =
        new ProducerRecord<>(topic, selectedPartition, selectedTimestamp, key, value);
    try (KafkaProducer<byte[], byte[]> producer = new KafkaProducer<>(properties)) {
      RecordMetadata metadata = producer.send(record).get(30, TimeUnit.SECONDS);
      producer.flush();
      System.out.println(
          "{\"topic\":"
              + quote(metadata.topic())
              + ",\"partition\":"
              + metadata.partition()
              + ",\"offset\":"
              + metadata.offset()
              + ",\"timestamp_ms\":"
              + metadata.timestamp()
              + "}");
    }
  }

  private static void consume(String[] args) {
    String bootstrap = requiredOption(args, "--bootstrap-servers");
    String topic = requiredOption(args, "--topic");
    int maximumRecords = Integer.parseInt(option(args, "--max-records", "100"));
    long timeoutMs = Long.parseLong(option(args, "--timeout-ms", "10000"));
    if (maximumRecords < 1 || timeoutMs < 1L) {
      throw new IllegalArgumentException("--max-records and --timeout-ms must be positive");
    }

    Properties properties = new Properties();
    properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
    properties.put(ConsumerConfig.GROUP_ID_CONFIG, "p1-binary-audit-" + UUID.randomUUID());
    properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
    properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
    properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);

    try (KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(properties)) {
      List<PartitionInfo> partitionInfo = consumer.partitionsFor(topic, Duration.ofSeconds(10));
      if (partitionInfo == null || partitionInfo.isEmpty()) {
        throw new IllegalArgumentException("topic has no partitions: " + topic);
      }
      List<TopicPartition> partitions = new ArrayList<>();
      for (PartitionInfo info : partitionInfo) {
        partitions.add(new TopicPartition(topic, info.partition()));
      }
      partitions.sort(Comparator.comparingInt(TopicPartition::partition));
      consumer.assign(partitions);
      consumer.seekToBeginning(partitions);

      int emitted = 0;
      long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMs);
      while (emitted < maximumRecords && System.nanoTime() < deadline) {
        ConsumerRecords<byte[], byte[]> records = consumer.poll(Duration.ofMillis(250));
        for (ConsumerRecord<byte[], byte[]> record : records) {
          System.out.println(toJson(record));
          emitted++;
          if (emitted >= maximumRecords) {
            break;
          }
        }
      }
    }
  }

  private static String toJson(ConsumerRecord<byte[], byte[]> record) {
    return "{\"topic\":"
        + quote(record.topic())
        + ",\"partition\":"
        + record.partition()
        + ",\"offset\":"
        + record.offset()
        + ",\"timestamp_ms\":"
        + record.timestamp()
        + ",\"key_base64\":"
        + nullableBytes(record.key())
        + ",\"value_base64\":"
        + nullableBytes(record.value())
        + "}";
  }

  private static String nullableBytes(byte[] value) {
    return value == null ? "null" : quote(Base64.getEncoder().encodeToString(value));
  }

  private static byte[] decodeNullable(String encoded) {
    return "-".equals(encoded) ? null : Base64.getDecoder().decode(encoded);
  }

  private static int commandIndex(String[] args) {
    for (int index = 0; index < args.length; index++) {
      if (PRODUCE.equals(args[index]) || CONSUME.equals(args[index])) {
        return index;
      }
    }
    return -1;
  }

  private static String requiredOption(String[] args, String name) {
    String value = option(args, name, null);
    if (value == null || value.isEmpty()) {
      throw new IllegalArgumentException(name + " is required");
    }
    return value;
  }

  private static String option(String[] args, String name, String defaultValue) {
    for (int index = 0; index < args.length - 1; index++) {
      if (name.equals(args[index])) {
        return args[index + 1];
      }
    }
    return defaultValue;
  }

  private static String quote(String value) {
    return "\""
        + value.replace("\\", "\\\\").replace("\"", "\\\"").replace("\n", "\\n")
        + "\"";
  }
}
