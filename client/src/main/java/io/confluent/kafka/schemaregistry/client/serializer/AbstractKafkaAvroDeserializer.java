/**
 * Copyright 2014 Confluent Inc.
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
package io.confluent.kafka.schemaregistry.client.serializer;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.util.Utf8;
import org.apache.kafka.common.errors.SerializationException;

import java.io.IOException;
import java.nio.ByteBuffer;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
public class AbstractKafkaAvroDeserializer {
  private static final byte MAGIC_BYTE = 0x0;
  private static final int idSize = 4;
  private final DecoderFactory decoderFactory = DecoderFactory.get();
  protected final String SCHEMA_REGISTRY_URL = "schema.registry.url";
  protected SchemaRegistryClient schemaRegistry;

  private ByteBuffer getByteBuffer(byte[] payload) {
    ByteBuffer buffer = ByteBuffer.wrap(payload);
    if (buffer.get() != MAGIC_BYTE) {
      throw new SerializationException("Unknown magic byte!");
    }
    return buffer;
  }

  protected Object deserialize(byte[] payload) throws SerializationException {
    try {
      ByteBuffer buffer = getByteBuffer(payload);
      int id = buffer.getInt();
      Schema schema = schemaRegistry.getByID(id);
      int length = buffer.limit() - 1 - idSize;
      if (schema.getType().equals(Schema.Type.BYTES)) {
        byte[] bytes = new byte[length];
        buffer.get(bytes, 0 , length);
        return bytes;
      }
      int start = buffer.position() + buffer.arrayOffset();
      DatumReader<Object> reader = new GenericDatumReader<Object>(schema);
      Object object =
          reader.read(null, decoderFactory.binaryDecoder(buffer.array(), start, length, null));

      if (schema.getType().equals(Schema.Type.STRING)) {
        object = ((Utf8) object).toString();
      }
      return object;
    } catch (IOException e) {
      throw new SerializationException("Error deserializing Avro message", e);
    }
  }
}
