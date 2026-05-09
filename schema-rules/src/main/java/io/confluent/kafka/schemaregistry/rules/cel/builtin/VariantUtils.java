/*
 * Copyright 2026 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.confluent.kafka.schemaregistry.rules.cel.builtin;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Message;
import io.confluent.avro.type.VariantConversion;
import io.confluent.kafka.schemaregistry.type.Variant;
import java.io.IOException;
import java.nio.ByteBuffer;
import org.apache.avro.generic.IndexedRecord;

/**
 * Conversion helpers backing {@code to_variant} and the {@code variant_*} accessor
 * functions. The CEL surface treats Variant as the canonical type
 * {@link CelTypes#VARIANT}; this client backs it with
 * {@link io.confluent.kafka.schemaregistry.type.Variant} (Spark variant binary
 * format).
 */
final class VariantUtils {

  /**
   * Reused thread-safe Jackson mapper for JSON ↔ Variant conversion. ObjectMapper
   * instances are documented as thread-safe after configuration, and we don't
   * mutate this one after construction.
   */
  private static final ObjectMapper JSON_MAPPER = new ObjectMapper();

  private VariantUtils() {
  }

  /**
   * Construct from raw {@code value} + {@code metadata} byte arrays.
   */
  static Variant fromBytes(byte[] value, byte[] metadata) {
    return new Variant(value, metadata);
  }

  /**
   * Decode a {@code confluent.type.Variant} proto message.
   */
  static Variant fromProto(io.confluent.protobuf.type.Variant v) {
    return new Variant(v.getValue().asReadOnlyByteBuffer(),
        v.getMetadata().asReadOnlyByteBuffer());
  }

  /**
   * Decode any {@code confluent.type.Variant} message — concrete generated class or
   * {@link com.google.protobuf.DynamicMessage} produced by a runtime-parsed schema.
   */
  static Variant fromProtoMessage(Message msg) {
    FieldDescriptor valueField = msg.getDescriptorForType().findFieldByName("value");
    FieldDescriptor metadataField = msg.getDescriptorForType().findFieldByName("metadata");
    ByteString value = (ByteString) msg.getField(valueField);
    ByteString metadata = (ByteString) msg.getField(metadataField);
    return new Variant(value.asReadOnlyByteBuffer(), metadata.asReadOnlyByteBuffer());
  }

  /**
   * Decode an Avro {@code variant} logical-type record (raw form).
   */
  static Variant fromAvroRecord(IndexedRecord record) {
    return new VariantConversion().fromRecord(record, record.getSchema(), null);
  }

  /**
   * Runtime dispatch backing {@code to_variant(dyn)}. Accepts the shapes Proto/Avro
   * decoders typically produce.
   */
  static Variant toVariant(Object o) {
    if (o == null) {
      throw new IllegalArgumentException("Cannot convert null to Variant");
    }
    if (o instanceof Variant) {
      return (Variant) o;
    }
    if (o instanceof io.confluent.protobuf.type.Variant) {
      return fromProto((io.confluent.protobuf.type.Variant) o);
    }
    if (o instanceof Message
        && "confluent.type.Variant".equals(
            ((Message) o).getDescriptorForType().getFullName())) {
      return fromProtoMessage((Message) o);
    }
    if (o instanceof IndexedRecord) {
      return fromAvroRecord((IndexedRecord) o);
    }
    if (o instanceof String) {
      return fromJson((String) o);
    }
    throw new IllegalArgumentException(
        "Cannot convert " + o.getClass().getName() + " to Variant");
  }

  /**
   * Parse a JSON string into a Variant. Throws {@link IllegalArgumentException}
   * with a clear message on malformed JSON; the caller's safe-overload wrapper
   * surfaces this as a {@code ValidationRuleError}.
   */
  static Variant fromJson(String json) {
    try {
      JsonNode node = JSON_MAPPER.readTree(json);
      return io.confluent.kafka.schemaregistry.type.VariantUtils.fromJsonNode(node);
    } catch (IOException e) {
      throw new IllegalArgumentException(
          "Cannot parse JSON for to_variant: " + e.getMessage(), e);
    }
  }

  /**
   * Serialize a Variant to its JSON string form via the {@code schema-types}
   * canonical JSON converter.
   */
  static String toJsonString(Variant v) {
    return io.confluent.kafka.schemaregistry.type.VariantUtils.toJsonString(v);
  }

  /**
   * Coerce a CEL bytes value to {@code byte[]}.
   */
  static byte[] toBytes(Object o) {
    if (o instanceof byte[]) {
      return (byte[]) o;
    }
    if (o instanceof ByteBuffer) {
      ByteBuffer bb = ((ByteBuffer) o).duplicate();
      byte[] out = new byte[bb.remaining()];
      bb.get(out);
      return out;
    }
    if (o instanceof ByteString) {
      return ((ByteString) o).toByteArray();
    }
    throw new IllegalArgumentException(
        "Cannot coerce " + (o == null ? "null" : o.getClass().getName()) + " to bytes");
  }
}
