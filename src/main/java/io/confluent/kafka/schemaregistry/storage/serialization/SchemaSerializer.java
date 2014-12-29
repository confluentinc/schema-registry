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
package io.confluent.kafka.schemaregistry.storage.serialization;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.util.Map;

import io.confluent.kafka.schemaregistry.rest.entities.Schema;
import io.confluent.kafka.schemaregistry.storage.exceptions.SerializationException;

public class SchemaSerializer implements Serializer<Schema> {

  public SchemaSerializer() {

  }

  @Override
  public byte[] toBytes(Schema data) throws SerializationException {
    try {
      return new ObjectMapper().writeValueAsBytes(data);
    } catch (IOException e) {
      throw new SerializationException("Error while serializing schema " + data.toString(),
                                       e);
    }
  }

  @Override
  public Schema fromBytes(byte[] data) throws SerializationException {
    Schema schema = null;
    try {
      schema = new ObjectMapper().readValue(data, Schema.class);
    } catch (IOException e) {
      throw new SerializationException("Error while deserializing schema", e);
    }
    return schema;
  }

  @Override
  public void close() {

  }

  @Override
  public void configure(Map<String, ?> stringMap) {

  }
}
