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

import java.io.IOException;

import io.confluent.kafka.schemaregistryclient.SchemaRegistryClient;
import kafka.serializer.Decoder;
import kafka.utils.VerifiableProperties;

public class KafkaAvroDecoder extends AbstractKafkaAvroDeserializer implements Decoder<Object>  {

  public KafkaAvroDecoder(VerifiableProperties props) {
    if (props == null) {
      throw new IllegalArgumentException("Props should not be null!");
    }
    String url = props.getProperty(SCHEMA_REGISTRY_URL);
    if (url == null) {
      throw new IllegalArgumentException("Missing schema registry url!");
    }
    schemaRegistry = new SchemaRegistryClient(url);
  }

  @Override
  public Object fromBytes(byte[] bytes){
    try {
      return deserialize(bytes);
    } catch (IOException e) {
      e.printStackTrace();
    }
    return null;
  }
}
