/*
 * Copyright 2017 Confluent Inc.
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

package io.confluent.kafka;

import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import avro.shaded.com.google.common.collect.ImmutableList;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;

public class SecureConfigParserTest {

  @Test
  public void testSingleUrlWithUserInfo(){
    Map<String, Object> originals = new HashMap<>();
    originals.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG,
        "http://user:password@localhost:2181");
    SecureConfigParser.parseSchemaRegistryUrl(originals);
    Assert.assertEquals(ImmutableList.of("http://localhost:2181"), originals.get(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG));
    Assert.assertNotNull(originals.get(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_USER_INFO));
  }

  @Test
  public void testSingleUrlWithoutUserInfo(){
    Map<String, Object> originals = new HashMap<>();
    originals.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG,
        "http://localhost:2181");
    SecureConfigParser.parseSchemaRegistryUrl(originals);
    Assert.assertEquals("http://localhost:2181", originals.get(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG));
    Assert.assertNull(originals.get(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_USER_INFO));
  }
}
