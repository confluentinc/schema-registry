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
package io.confluent.kafka.serializers;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.common.errors.SerializationException;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

public abstract class AbstractKafkaAvroSerializer extends AbstractKafkaAvroSerDe {
  private final EncoderFactory encoderFactory = EncoderFactory.get();

  protected byte[] serializeImpl(String subject, Object record) throws SerializationException {
    try {
      ByteArrayOutputStream out = new ByteArrayOutputStream();
      Schema schema = getSchema(record);
      int id = schemaRegistry.register(subject, schema);
      out.write(MAGIC_BYTE);
      out.write(ByteBuffer.allocate(idSize).putInt(id).array());
      if (record instanceof byte[]) {
        out.write((byte[]) record);
      } else {
        BinaryEncoder encoder = encoderFactory.directBinaryEncoder(out, null);
        DatumWriter<Object> writer;
        if (record instanceof SpecificRecord) {
          writer = new SpecificDatumWriter<Object>(schema);
        } else {
          writer = new GenericDatumWriter<Object>(schema);
        }
        writer.write(record, encoder);
        encoder.flush();
      }
      byte[] bytes = out.toByteArray();
      out.close();
      return bytes;
    } catch (IOException e) {
      throw new SerializationException("Error serializing Avro message", e);
    }
  }

  public int register(String subject, Schema schema) throws IOException {
    return schemaRegistry.register(subject, schema);
  }

  public Schema getByID(int id) throws IOException {
    return schemaRegistry.getByID(id);
  }
}
