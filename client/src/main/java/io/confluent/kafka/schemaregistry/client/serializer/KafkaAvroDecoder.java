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

import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.errors.SerializationException;

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import kafka.serializer.Decoder;
import kafka.utils.VerifiableProperties;

public class KafkaAvroDecoder extends AbstractKafkaAvroDeserializer implements Decoder<Object>  {

  public KafkaAvroDecoder(SchemaRegistryClient schemaRegistry) {
    this.schemaRegistry = schemaRegistry;
  }

  public KafkaAvroDecoder(VerifiableProperties props) {
    if (props == null) {
      throw new ConfigException("Missing schema registry url!");
    }
    String url = props.getProperty(SCHEMA_REGISTRY_URL);
    if (url == null) {
      throw new ConfigException("Missing schema registry url!");
    }
    int maxSchemaObject = props.getInt(MAX_SCHEMAS_PER_SUBJECT, DEFAULT_MAX_SCHEMAS_PER_SUBJECT);
    schemaRegistry = new CachedSchemaRegistryClient(url, maxSchemaObject);
  }

  @Override
  public Object fromBytes(byte[] bytes){
    try {
      return deserialize(bytes);
    } catch (SerializationException e) {
      e.printStackTrace();
    }
    return null;
  }
}
