package com.p1.reliability.cdc;

import io.confluent.connect.avro.AvroConverter;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.Struct;

public final class KafkaDebeziumAvroRecordDeserializationSchema
    implements KafkaRecordDeserializationSchema<BrokerRecord> {
  private static final long serialVersionUID = 1L;
  private static final DateTimeFormatter MYSQL_DATETIME =
      DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss[.SSS][.SSSSSS]", Locale.ROOT);

  private final String schemaRegistryUrl;
  private transient AvroConverter converter;

  public KafkaDebeziumAvroRecordDeserializationSchema(String schemaRegistryUrl) {
    if (schemaRegistryUrl == null || schemaRegistryUrl.trim().isEmpty()) {
      throw new IllegalArgumentException("schemaRegistryUrl must not be blank");
    }
    this.schemaRegistryUrl = schemaRegistryUrl;
  }

  @Override
  public void open(DeserializationSchema.InitializationContext context) {
    Map<String, Object> converterConfig = new HashMap<>();
    converterConfig.put("schema.registry.url", schemaRegistryUrl);
    converterConfig.put("enhanced.avro.schema.support", true);
    converter = new AvroConverter();
    converter.configure(converterConfig, false);
  }

  @Override
  public void deserialize(ConsumerRecord<byte[], byte[]> record, Collector<BrokerRecord> out) {
    if (record.value() == null) {
      return;
    }
    if (converter == null) {
      throw new IllegalStateException("Avro converter was not initialized");
    }
    try {
      SchemaAndValue converted = converter.toConnectData(record.topic(), record.value());
      if (!(converted.value() instanceof Struct)) {
        throw new IllegalArgumentException("Debezium Avro value is not a Kafka Connect Struct");
      }
      Struct value = (Struct) converted.value();
      String operation = value.getString("op");
      Long sourceTsMs = value.getInt64("ts_ms");

      if ("c".equals(operation)) {
        out.collect(
            BrokerRecord.decoded(
                fromStruct(RowKind.INSERT, "insert", value.getStruct("after"), sourceTsMs)));
      } else if ("r".equals(operation)) {
        out.collect(
            BrokerRecord.decoded(
                fromStruct(RowKind.INSERT, "snapshot", value.getStruct("after"), sourceTsMs)));
      } else if ("u".equals(operation)) {
        out.collect(
            BrokerRecord.decoded(
                fromStruct(
                    RowKind.UPDATE_BEFORE,
                    "update_before",
                    value.getStruct("before"),
                    sourceTsMs)));
        out.collect(
            BrokerRecord.decoded(
                fromStruct(
                    RowKind.UPDATE_AFTER,
                    "update_after",
                    value.getStruct("after"),
                    sourceTsMs)));
      } else if ("d".equals(operation)) {
        out.collect(
            BrokerRecord.decoded(
                fromStruct(RowKind.DELETE, "delete", value.getStruct("before"), sourceTsMs)));
      } else {
        throw new IllegalArgumentException("Unsupported Debezium operation: " + operation);
      }
    } catch (RuntimeException failure) {
      out.collect(BrokerRecord.deadLetter(DeadLetterRecord.from(record, failure)));
    }
  }

  @Override
  public TypeInformation<BrokerRecord> getProducedType() {
    return TypeInformation.of(BrokerRecord.class);
  }

  private static OrderChange fromStruct(
      RowKind rowKind, String operation, Struct row, Long sourceTsMs) {
    if (row == null) {
      throw new IllegalArgumentException("Missing row payload for " + operation);
    }
    return new OrderChange(
        rowKind,
        operation,
        number(row.get("order_id")).longValue(),
        row.getString("business_key"),
        number(row.get("event_id")).longValue(),
        number(row.get("customer_id")).longValue(),
        row.getString("status"),
        number(row.get("amount_cents")).longValue(),
        timestamp(row.get("updated_at")),
        number(row.get("seed")).intValue(),
        sourceTsMs);
  }

  private static Number number(Object value) {
    if (!(value instanceof Number)) {
      throw new IllegalArgumentException("Expected numeric value but got " + value);
    }
    return (Number) value;
  }

  private static LocalDateTime timestamp(Object value) {
    if (value instanceof LocalDateTime) {
      return (LocalDateTime) value;
    }
    if (value instanceof java.util.Date) {
      return LocalDateTime.ofInstant(((java.util.Date) value).toInstant(), ZoneOffset.UTC);
    }
    if (value instanceof Number) {
      long epochValue = ((Number) value).longValue();
      if (Math.abs(epochValue) < 100_000_000_000_000L) {
        return LocalDateTime.ofInstant(Instant.ofEpochMilli(epochValue), ZoneOffset.UTC);
      }
      long seconds = Math.floorDiv(epochValue, 1_000_000L);
      long nanos = Math.floorMod(epochValue, 1_000_000L) * 1_000L;
      return LocalDateTime.ofEpochSecond(seconds, (int) nanos, ZoneOffset.UTC);
    }
    if (value instanceof String) {
      String text = ((String) value).replace('T', ' ');
      if (text.endsWith("Z")) {
        return LocalDateTime.ofInstant(Instant.parse((String) value), ZoneOffset.UTC);
      }
      return LocalDateTime.parse(text, MYSQL_DATETIME);
    }
    throw new IllegalArgumentException("Unsupported timestamp value: " + value);
  }
}
