package com.p1.reliability.cdc;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Collections;
import java.util.Locale;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.json.JsonConverter;

public final class KafkaDebeziumJsonRecordDeserializationSchema
    implements KafkaRecordDeserializationSchema<OrderChange> {
  private static final long serialVersionUID = 1L;
  private static final DateTimeFormatter MYSQL_DATETIME =
      DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss[.SSS][.SSSSSS]", Locale.ROOT);

  private transient JsonConverter converter;

  @Override
  public void open(DeserializationSchema.InitializationContext context) {
    converter = new JsonConverter();
    converter.configure(Collections.singletonMap("schemas.enable", true), false);
  }

  @Override
  public void deserialize(ConsumerRecord<byte[], byte[]> record, Collector<OrderChange> out) {
    if (record.value() == null) {
      return;
    }
    if (converter == null) {
      throw new IllegalStateException("JSON converter was not initialized");
    }
    SchemaAndValue converted = converter.toConnectData(record.topic(), record.value());
    if (!(converted.value() instanceof Struct)) {
      throw new IllegalArgumentException("Debezium value is not a Kafka Connect Struct");
    }
    Struct value = (Struct) converted.value();
    String operation = value.getString("op");
    Long sourceTsMs = value.getInt64("ts_ms");

    if ("c".equals(operation)) {
      out.collect(fromStruct(RowKind.INSERT, "insert", value.getStruct("after"), sourceTsMs));
    } else if ("r".equals(operation)) {
      out.collect(fromStruct(RowKind.INSERT, "snapshot", value.getStruct("after"), sourceTsMs));
    } else if ("u".equals(operation)) {
      out.collect(
          fromStruct(
              RowKind.UPDATE_BEFORE,
              "update_before",
              value.getStruct("before"),
              sourceTsMs));
      out.collect(
          fromStruct(
              RowKind.UPDATE_AFTER, "update_after", value.getStruct("after"), sourceTsMs));
    } else if ("d".equals(operation)) {
      out.collect(fromStruct(RowKind.DELETE, "delete", value.getStruct("before"), sourceTsMs));
    } else {
      throw new IllegalArgumentException("Unsupported Debezium operation: " + operation);
    }
  }

  @Override
  public TypeInformation<OrderChange> getProducedType() {
    return TypeInformation.of(OrderChange.class);
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
