package com.p1.reliability.cdc;

import java.time.Instant;
import java.util.Base64;
import org.apache.kafka.clients.consumer.ConsumerRecord;

/** Stable JSON metadata for an Avro record that Path B could not deserialize. */
public final class DeadLetterRecord {
  private DeadLetterRecord() {}

  public static String from(ConsumerRecord<byte[], byte[]> record, RuntimeException failure) {
    String key = record.key() == null ? null : Base64.getEncoder().encodeToString(record.key());
    String value =
        record.value() == null ? null : Base64.getEncoder().encodeToString(record.value());
    return "{"
        + "\"dlq_schema_version\":1,"
        + "\"quarantined_at\":"
        + quote(Instant.now().toString())
        + ",\"original_topic\":"
        + quote(record.topic())
        + ",\"original_partition\":"
        + record.partition()
        + ",\"original_offset\":"
        + record.offset()
        + ",\"original_timestamp_ms\":"
        + record.timestamp()
        + ",\"original_key_base64\":"
        + nullableQuote(key)
        + ",\"original_value_base64\":"
        + nullableQuote(value)
        + ",\"error_type\":"
        + quote(failure.getClass().getName())
        + ",\"error_message\":"
        + quote(String.valueOf(failure.getMessage()))
        + "}";
  }

  private static String nullableQuote(String value) {
    return value == null ? "null" : quote(value);
  }

  private static String quote(String value) {
    StringBuilder output = new StringBuilder(value.length() + 2).append('"');
    for (int index = 0; index < value.length(); index++) {
      char character = value.charAt(index);
      switch (character) {
        case '\\':
          output.append("\\\\");
          break;
        case '"':
          output.append("\\\"");
          break;
        case '\n':
          output.append("\\n");
          break;
        case '\r':
          output.append("\\r");
          break;
        case '\t':
          output.append("\\t");
          break;
        default:
          if (character < 0x20) {
            output.append(String.format("\\u%04x", (int) character));
          } else {
            output.append(character);
          }
      }
    }
    return output.append('"').toString();
  }
}
