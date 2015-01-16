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

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;

import java.io.IOException;
import java.nio.ByteBuffer;

import io.confluent.kafka.schemaregistry.SchemaRegistryClient;
import kafka.serializer.Decoder;
import kafka.utils.VerifiableProperties;

public class KafkaAvroDeserializer implements Decoder<Object>  {

  private static final byte MAGIC_BYTE = 0x0;
  private final DecoderFactory decoderFactory = DecoderFactory.get();
  private final VerifiableProperties props;
  private final String propertyName = "schema.registry.url";
  private SchemaRegistryClient schemaRegistry;

  public KafkaAvroDeserializer(VerifiableProperties props) {
    this.props = props;
    String url = props.getProperty(propertyName);
    schemaRegistry = new SchemaRegistryClient(url);

  }

  private ByteBuffer getByteBuffer(byte[] payload) {
    ByteBuffer buffer = ByteBuffer.wrap(payload);
    if (buffer.get() != MAGIC_BYTE) {
      throw new IllegalArgumentException("Unknown magic byte!");
    }
    return buffer;
  }

  @Override
  public Object fromBytes(byte[] bytes){
    try {
      return deserialize(bytes);
    } catch (IOException e) {

    }
    return null;
  }

  private Object deserialize(byte[] payload) throws IOException {
    ByteBuffer buffer = getByteBuffer(payload);
    int id = buffer.getInt();
    Schema schema = schemaRegistry.getByID("topic", id);
    int start = buffer.position() + buffer.arrayOffset();
    int length = buffer.limit() - 5;
    DatumReader<Object> reader = new GenericDatumReader<Object>(schema);
    Object object = reader.read(null,  decoderFactory.binaryDecoder(buffer.array(),start, length, null));
    return object;
  }
}
