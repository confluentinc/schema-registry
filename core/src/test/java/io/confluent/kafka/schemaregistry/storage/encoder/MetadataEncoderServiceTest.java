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

import io.confluent.kafka.schemaregistry.client.rest.entities.Metadata;
import io.confluent.kafka.schemaregistry.rest.SchemaRegistryConfig;
import io.confluent.kafka.schemaregistry.storage.SchemaRegistry;
import io.confluent.kafka.schemaregistry.storage.SchemaValue;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public abstract class MetadataEncoderServiceTest {
  protected SchemaRegistry schemaRegistry;
  protected MetadataEncoderService encoderService;

  public MetadataEncoderServiceTest() throws Exception {
    this.schemaRegistry = mock(SchemaRegistry.class);
    Properties props = new Properties();
    props.setProperty(SchemaRegistryConfig.METADATA_ENCODER_SECRET_CONFIG, "mysecret");
    SchemaRegistryConfig config = new SchemaRegistryConfig(props);
    when(schemaRegistry.config()).thenReturn(config);
  }

  @BeforeEach
  public void setup() throws Exception {
    encoderService.init();
  }

  @AfterEach
  public void teardown() {
    encoderService.close();
  }

  @Test
  public void testEncoding() throws Exception {
    Map<String, String> properties = new HashMap<>();
    properties.put("nonsensitive", "foo");
    properties.put("sensitive", "foo");
    Metadata metadata = new Metadata(null, properties, Collections.singleton("sensitive"));

    SchemaValue schema = new SchemaValue(
        "mysubject", null, null, null, null, null,
        new io.confluent.kafka.schemaregistry.storage.Metadata(metadata), null, "true", false);
    encoderService.encodeMetadata(schema);
    assertEquals("foo", schema.getMetadata().getProperties().get("nonsensitive"));
    // the value of "sensitive" is encrypted
    assertNotEquals("foo", schema.getMetadata().getProperties().get("sensitive"));
    assertNotNull(schema.getMetadata().getProperties().get(SchemaValue.ENCODED_PROPERTY));

    SchemaValue schema2 = new SchemaValue(
        "mysubject", null, null, null, null, null,
        new io.confluent.kafka.schemaregistry.storage.Metadata(
            schema.getMetadata().toMetadataEntity()), null, "true", false);
    encoderService.decodeMetadata(schema2);
    assertEquals("foo", schema2.getMetadata().getProperties().get("nonsensitive"));
    // the value of "sensitive" is decrypted
    assertEquals("foo", schema2.getMetadata().getProperties().get("sensitive"));
    assertNull(schema2.getMetadata().getProperties().get(SchemaValue.ENCODED_PROPERTY));
  }
}
