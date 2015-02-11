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

import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;

public abstract class AbstractKafkaAvroSerializer extends AbstractKafkaAvroSerDe {
  private final EncoderFactory encoderFactory = EncoderFactory.get();

  protected byte[] serializeImpl(String subject, Object object) throws SerializationException {
    Schema schema = null;
    // null needs to treated specially since the client most likely just wants to send
    // an individual null value instead of making the subject a null type. Also, null in
    // Kafka has a special meaning for deletion in a topic with the compact retention policy.
    // Therefore, we will bypass schema registration and return a null value in Kafka, instead
    // of an Avro encoded null.
    if (object == null) {
      return null;
    }

    try {
      schema = getSchema(object);
      int id = schemaRegistry.register(subject, schema);
      ByteArrayOutputStream out = new ByteArrayOutputStream();
      out.write(MAGIC_BYTE);
      out.write(ByteBuffer.allocate(idSize).putInt(id).array());
      if (object instanceof byte[]) {
        out.write((byte[]) object);
      } else {
        BinaryEncoder encoder = encoderFactory.directBinaryEncoder(out, null);
        DatumWriter<Object> writer;
        if (object instanceof SpecificRecord) {
          writer = new SpecificDatumWriter<Object>(schema);
        } else {
          writer = new GenericDatumWriter<Object>(schema);
        }
        writer.write(object, encoder);
        encoder.flush();
      }
      byte[] bytes = out.toByteArray();
      out.close();
      return bytes;
    } catch (IOException e) {
      throw new SerializationException("Error serializing Avro message", e);
    } catch (RestClientException e) {
      throw new SerializationException("Error registering Avro schema: " + schema, e);
    } catch (RuntimeException e) {
      // avro serialization can throw AvroRuntimeException, NullPointerException,
      // ClassCastException, etc
      throw new SerializationException("Error serializing Avro message", e);
    }
  }
}
