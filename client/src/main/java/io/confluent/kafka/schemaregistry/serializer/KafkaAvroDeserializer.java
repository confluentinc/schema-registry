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
package io.confluent.kafka.schemaregistry.serializer;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.Schema;


public class KafkaAvroDeserializer {

  private static final byte MAGIC_BYTE = 0x0;

  private final DecoderFactory decoderFactory = DecoderFactory.get();

  private final Schema schema;

  public KafkaAvroDeserializer(Schema schema) {
    this.schema = schema;
  }

  private ByteBuffer getByteBuffer(byte[] payload) {
    ByteBuffer buffer = ByteBuffer.wrap(payload);
    if (buffer.get() != MAGIC_BYTE) {
      throw new IllegalArgumentException("Unknown magic byte!");
    }
    return buffer;
  }

  public Object deserialize(byte[] payload) throws IOException {
    ByteBuffer buffer = getByteBuffer(payload);
    buffer.getInt();
    int start = buffer.position() + buffer.arrayOffset();
    int length = buffer.limit() - 5;
    DatumReader<Object> reader = new GenericDatumReader<Object>();
    Object object = reader.read(null, decoderFactory.binaryDecoder(buffer.array(),start, length, null));
    return object;
  }
}
