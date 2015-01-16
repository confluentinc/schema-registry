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

import org.apache.avro.generic.IndexedRecord;

import io.confluent.kafka.schemaregistry.SchemaRegistryClient;
import kafka.serializer.Encoder;
import kafka.utils.VerifiableProperties;

// Only works for IndexedRecord
// For now, this only works for value.
// To register for a topic, user will need to provide a list of record_name:topic_name map as
// a CSV format. By default, will use record name as subject
// Key uses primitive types.

public class KafkaAvroEncoder extends AbstracKafkaAvroSerializer implements Encoder<Object>  {

  private VerifiableProperties props = null;

  public KafkaAvroEncoder(VerifiableProperties props) {
    this.props = props;
    if (props != null) {
      String url = props.getProperty(propertyName);
      schemaRegistry = new SchemaRegistryClient(url);
    } else {
      throw new IllegalArgumentException("Props should not be null");
    }
  }

  @Override
  public byte[] toBytes(Object object) {
    if (object instanceof IndexedRecord) {
      String subject = ((IndexedRecord) object).getSchema().getName();
      return serializeImpl(subject, object);
    } else {
      throw new IllegalArgumentException("Primitive types are not supported yet");
    }
  }
}
