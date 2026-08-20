package com.p1.reliability.cdc;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.util.Base64;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;

/** Binary-safe Confluent Avro codec for the deterministic B3 record probe utility. */
public final class AvroWireCodec {
  private static final ObjectMapper MAPPER = new ObjectMapper();

  private AvroWireCodec() {}

  public static byte[] encode(int schemaId, String schemaJson, String datumJson) throws Exception {
    Schema schema = new Schema.Parser().parse(schemaJson);
    Object datum = toDatum(MAPPER.readTree(datumJson), schema);
    ByteArrayOutputStream output = new ByteArrayOutputStream();
    output.write(0);
    output.write(ByteBuffer.allocate(4).putInt(schemaId).array());
    BinaryEncoder encoder = EncoderFactory.get().directBinaryEncoder(output, null);
    new GenericDatumWriter<>(schema).write(datum, encoder);
    encoder.flush();
    return output.toByteArray();
  }

  public static String decode(
      int expectedSchemaId, String schemaJson, byte[] confluentPayload) throws Exception {
    if (confluentPayload.length < 5 || confluentPayload[0] != 0) {
      throw new IllegalArgumentException("payload is not Confluent Avro wire format");
    }
    int schemaId = ByteBuffer.wrap(confluentPayload, 1, 4).getInt();
    if (schemaId != expectedSchemaId) {
      throw new IllegalArgumentException(
          "schema id " + schemaId + " does not match expected " + expectedSchemaId);
    }
    Schema schema = new Schema.Parser().parse(schemaJson);
    BinaryDecoder decoder =
        DecoderFactory.get().binaryDecoder(confluentPayload, 5, confluentPayload.length - 5, null);
    Object datum = new GenericDatumReader<>(schema).read(null, decoder);
    return MAPPER.writeValueAsString(fromDatum(datum, schema));
  }

  private static Object toDatum(JsonNode node, Schema schema) {
    switch (schema.getType()) {
      case NULL:
        return null;
      case RECORD:
        if (!node.isObject()) {
          throw new IllegalArgumentException("expected JSON object for record " + schema.getName());
        }
        GenericRecord record = new GenericData.Record(schema);
        for (Schema.Field field : schema.getFields()) {
          JsonNode child = node.get(field.name());
          if (child == null) {
            if (field.hasDefaultValue()) {
              record.put(field.name(), GenericData.get().getDefaultValue(field));
              continue;
            }
            throw new IllegalArgumentException("missing field " + field.name());
          }
          record.put(field.name(), toDatum(child, field.schema()));
        }
        return record;
      case UNION:
        if (node.isNull()) {
          for (Schema branch : schema.getTypes()) {
            if (branch.getType() == Schema.Type.NULL) {
              return null;
            }
          }
          throw new IllegalArgumentException("null does not match union " + schema);
        }
        RuntimeException lastFailure = null;
        for (Schema branch : schema.getTypes()) {
          if (branch.getType() == Schema.Type.NULL) {
            continue;
          }
          try {
            Object candidate = toDatum(node, branch);
            if (GenericData.get().validate(branch, candidate)) {
              return candidate;
            }
          } catch (RuntimeException failure) {
            lastFailure = failure;
          }
        }
        throw new IllegalArgumentException("datum does not match union " + schema, lastFailure);
      case ARRAY:
        if (!node.isArray()) {
          throw new IllegalArgumentException("expected JSON array");
        }
        GenericData.Array<Object> array = new GenericData.Array<>(node.size(), schema);
        for (JsonNode child : node) {
          array.add(toDatum(child, schema.getElementType()));
        }
        return array;
      case MAP:
        if (!node.isObject()) {
          throw new IllegalArgumentException("expected JSON object for map");
        }
        Map<String, Object> map = new java.util.LinkedHashMap<>();
        node.fields()
            .forEachRemaining(
                entry -> map.put(entry.getKey(), toDatum(entry.getValue(), schema.getValueType())));
        return map;
      case STRING:
        return node.asText();
      case ENUM:
        return new GenericData.EnumSymbol(schema, node.asText());
      case BOOLEAN:
        return node.booleanValue();
      case INT:
        return node.intValue();
      case LONG:
        return node.longValue();
      case FLOAT:
        return (float) node.doubleValue();
      case DOUBLE:
        return node.doubleValue();
      case BYTES:
        return ByteBuffer.wrap(Base64.getDecoder().decode(node.asText()));
      case FIXED:
        return new GenericData.Fixed(schema, Base64.getDecoder().decode(node.asText()));
      default:
        throw new IllegalArgumentException("unsupported Avro type " + schema.getType());
    }
  }

  private static JsonNode fromDatum(Object datum, Schema schema) {
    JsonNodeFactory factory = JsonNodeFactory.instance;
    if (datum == null) {
      return factory.nullNode();
    }
    if (schema.getType() == Schema.Type.UNION) {
      int branch = GenericData.get().resolveUnion(schema, datum);
      return fromDatum(datum, schema.getTypes().get(branch));
    }
    switch (schema.getType()) {
      case RECORD:
        ObjectNode object = factory.objectNode();
        GenericRecord record = (GenericRecord) datum;
        for (Schema.Field field : schema.getFields()) {
          object.set(field.name(), fromDatum(record.get(field.name()), field.schema()));
        }
        return object;
      case ARRAY:
        ArrayNode array = factory.arrayNode();
        for (Object item : (Iterable<?>) datum) {
          array.add(fromDatum(item, schema.getElementType()));
        }
        return array;
      case MAP:
        ObjectNode map = factory.objectNode();
        for (Map.Entry<?, ?> entry : ((Map<?, ?>) datum).entrySet()) {
          map.set(entry.getKey().toString(), fromDatum(entry.getValue(), schema.getValueType()));
        }
        return map;
      case STRING:
      case ENUM:
        return factory.textNode(String.valueOf(datum));
      case BOOLEAN:
        return factory.booleanNode((Boolean) datum);
      case INT:
        return factory.numberNode((Integer) datum);
      case LONG:
        return factory.numberNode((Long) datum);
      case FLOAT:
        return factory.numberNode((Float) datum);
      case DOUBLE:
        return factory.numberNode((Double) datum);
      case BYTES:
        ByteBuffer bytes = ((ByteBuffer) datum).duplicate();
        byte[] byteArray = new byte[bytes.remaining()];
        bytes.get(byteArray);
        return factory.textNode(Base64.getEncoder().encodeToString(byteArray));
      case FIXED:
        return factory.textNode(
            Base64.getEncoder().encodeToString(((GenericData.Fixed) datum).bytes()));
      case NULL:
        return factory.nullNode();
      default:
        throw new IllegalArgumentException("unsupported Avro type " + schema.getType());
    }
  }
}
