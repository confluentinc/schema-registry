/**
 * Copyright 2015 Confluent Inc.
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
 **/

package io.confluent.kafka.serializers;

import io.confluent.kafka.converters.TBaseToGenericRecord;
import io.confluent.kafka.io.GenericBinaryDecoder;
import io.confluent.kafka.io.ThriftReader;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import org.apache.avro.Schema;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DatumReader;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.thrift.TBase;

import java.util.Map;

public class KafkaThriftDeserializer extends AbstractKafkaAvroDeserializer
    implements Deserializer<Object> {

  private boolean isKey;
  private Schema writerSchema;
  private boolean convertToGenericRecord;

  /**
   * Constructor used by Kafka consumer.
   */
  public KafkaThriftDeserializer() {

  }

  public KafkaThriftDeserializer(SchemaRegistryClient client) {
    schemaRegistry = client;
  }

  public KafkaThriftDeserializer(SchemaRegistryClient client, Map<String, ?> props) {
    schemaRegistry = client;
    configure(deserializerConfig(props));
  }

  @Override
  public void configure(Map<String, ?> configs, boolean isKey) {
    this.isKey = isKey;
    convertToGenericRecord = configs.get("convertToGenericRecord") != null;
    configure(new KafkaAvroDeserializerConfig(configs));
  }

  @Override
  public Object deserialize(String s, byte[] bytes) {
    Object message = deserialize(bytes);
    if (convertToGenericRecord) {
      return TBaseToGenericRecord.convert((TBase) message, writerSchema);
    }
    return message;
  }

  /**
   * Pass a reader schema to get an Avro projection
   */
  public Object deserialize(String s, byte[] bytes, Schema readerSchema) {
    return deserialize(bytes, readerSchema);
  }

  @Override
  public void close() {

  }

  @Override
  protected DatumReader getDatumReader(Schema writerSchema, Schema readerSchema) {
    this.writerSchema = writerSchema;
    return new ThriftReader(writerSchema);
  }

  @Override
  protected BinaryDecoder getBinaryDecoder(byte[] bytes, int start, int length) {
    return new GenericBinaryDecoder(bytes, start, length);
  }
}
