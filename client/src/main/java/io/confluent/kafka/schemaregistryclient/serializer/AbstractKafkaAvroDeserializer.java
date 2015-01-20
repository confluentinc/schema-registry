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
package io.confluent.kafka.schemaregistryclient.serializer;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;

import java.io.IOException;
import java.nio.ByteBuffer;

import io.confluent.kafka.schemaregistryclient.SchemaRegistryClient;

public class AbstractKafkaAvroDeserializer {
  private static final byte MAGIC_BYTE = 0x0;
  private static final int idSize = 8;
  private final DecoderFactory decoderFactory = DecoderFactory.get();
  protected final String SCHEMA_REGISTRY_URL = "schema.registry.url";
  protected SchemaRegistryClient schemaRegistry;

  private ByteBuffer getByteBuffer(byte[] payload) {
    ByteBuffer buffer = ByteBuffer.wrap(payload);
    if (buffer.get() != MAGIC_BYTE) {
      throw new IllegalArgumentException("Unknown magic byte!");
    }
    return buffer;
  }

  protected Object deserialize(byte[] payload) throws IOException {
    ByteBuffer buffer = getByteBuffer(payload);
    long id = buffer.getLong();
    Schema schema = schemaRegistry.getByID(id);
    int start = buffer.position() + buffer.arrayOffset();
    int length = buffer.limit() - 1 - idSize;
    DatumReader<Object> reader = new GenericDatumReader<Object>(schema);
    Object object = reader.read(null,  decoderFactory.binaryDecoder(buffer.array(),start, length, null));
    return object;
  }
}
