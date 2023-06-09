/*
 * Copyright 2022 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.kafka.schemaregistry.storage.encoder;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.confluent.kafka.schemaregistry.client.rest.entities.Metadata;
import io.confluent.kafka.schemaregistry.rest.SchemaRegistryConfig;
import io.confluent.kafka.schemaregistry.storage.KafkaSchemaRegistry;
import io.confluent.kafka.schemaregistry.storage.SchemaValue;
import io.kcache.Cache;
import io.kcache.utils.InMemoryCache;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import org.junit.Test;

public class MetadataEncoderServiceTest {

  @Test
  public void testEncoding() throws Exception {
    KafkaSchemaRegistry schemaRegistry = mock(KafkaSchemaRegistry.class);
    Properties props = new Properties();
    props.setProperty(SchemaRegistryConfig.METADATA_ENCODER_SECRET_CONFIG, "mysecret");
    SchemaRegistryConfig config = new SchemaRegistryConfig(props);
    when(schemaRegistry.config()).thenReturn(config);
    Cache<String, KeysetWrapper> encoders = new InMemoryCache<>();
    MetadataEncoderService encoderService = new MetadataEncoderService(schemaRegistry, encoders);
    encoderService.init();

    Map<String, String> properties = new HashMap<>();
    properties.put("nonsensitive", "foo");
    properties.put("sensitive", "foo");
    Metadata metadata = new Metadata(null, properties, Collections.singleton("sensitive"));

    SchemaValue schema = new SchemaValue(
        "mysubject", null, null, null, null, null,
        new io.confluent.kafka.schemaregistry.storage.Metadata(metadata), null, "true", false);
    encoderService.encodeMetadata(schema);
    assertEquals(schema.getMetadata().getProperties().get("nonsensitive"), "foo");
    // the value of "sensitive" is encrypted
    assertNotEquals(schema.getMetadata().getProperties().get("sensitive"), "foo");
    assertNotNull(schema.getMetadata().getProperties().get(SchemaValue.ENCODED_PROPERTY));

    encoderService.decodeMetadata(schema);
    assertEquals(schema.getMetadata().getProperties().get("nonsensitive"), "foo");
    // the value of "sensitive" is decrypted
    assertEquals(schema.getMetadata().getProperties().get("sensitive"), "foo");
    assertNull(schema.getMetadata().getProperties().get(SchemaValue.ENCODED_PROPERTY));
  }
}
