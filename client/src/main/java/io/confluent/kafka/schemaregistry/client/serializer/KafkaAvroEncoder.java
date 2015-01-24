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

import org.apache.avro.generic.IndexedRecord;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.errors.SerializationException;

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import kafka.serializer.Encoder;
import kafka.utils.VerifiableProperties;

/**
  Only works for IndexedRecord
  For now, this only works for value.
  To register for a topic, user will need to provide a list of record_name:topic_name map as
  a CSV format. By default, will use record name as subject.
*/
public class KafkaAvroEncoder extends AbstracKafkaAvroSerializer implements Encoder<Object>  {

  public KafkaAvroEncoder(SchemaRegistryClient schemaRegistry){
    this.schemaRegistry = schemaRegistry;
  }

  public KafkaAvroEncoder(VerifiableProperties props) {
    if (props != null) {
      String url = props.getString(SCHEMA_REGISTRY_URL);
      if (url == null) {
        throw new ConfigException("Missing schema registry url!");
      }
      String maxSchemaObject = props.getString(MAX_SCHEMA_OBJECT);
      if (maxSchemaObject == null) {
        schemaRegistry = new CachedSchemaRegistryClient(url);
      } else {
        schemaRegistry = new CachedSchemaRegistryClient(url, Integer.parseInt(maxSchemaObject));
      }
    } else {
      throw new ConfigException("Missing schema registry url!");
    }
  }

  @Override
  public byte[] toBytes(Object object) {
    if (object instanceof IndexedRecord) {
      String subject = ((IndexedRecord) object).getSchema().getName() + "-value";
      return serializeImpl(subject, object);
    } else {
      throw new SerializationException("Primitive types are not supported yet");
    }
  }
}
