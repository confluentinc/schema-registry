/*
 * Copyright 2026 Confluent Inc.
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
import io.kcache.Cache;
import io.kcache.utils.InMemoryCache;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class MetadataEncoderServiceStrictValidationTest {

  private SchemaRegistry createMockRegistry(String secret, boolean strictValidation)
      throws Exception {
    SchemaRegistry registry = mock(SchemaRegistry.class);
    Properties props = new Properties();
    if (secret != null) {
      props.setProperty(SchemaRegistryConfig.METADATA_ENCODER_SECRET_CONFIG, secret);
    }
    props.setProperty(
        SchemaRegistryConfig.METADATA_ENCODER_SECRET_STRICT_VALIDATION_CONFIG,
        String.valueOf(strictValidation));
    SchemaRegistryConfig config = new SchemaRegistryConfig(props);
    when(registry.config()).thenReturn(config);
    return registry;
  }

  private KafkaMetadataEncoderService createService(SchemaRegistry registry) {
    Cache<String, KeysetWrapper> encoders = new InMemoryCache<>();
    return new KafkaMetadataEncoderService(registry, encoders);
  }

  private SchemaValue createSchemaWithSensitiveMetadata() {
    Map<String, String> properties = new HashMap<>();
    properties.put("description", "test");
    properties.put("api_secret", "sensitive-value");
    Metadata metadata = new Metadata(null, properties, Collections.singleton("api_secret"));
    return new SchemaValue(
        "mysubject", null, null, null, null, null,
        new io.confluent.kafka.schemaregistry.storage.Metadata(metadata), null, "true", false);
  }

  @Test
  public void testEmptySecret_StrictDisabled_EncoderServiceInitializes() throws Exception {
    // With strict validation disabled (default), empty secret should still initialize
    // the encoder service (backward compatible behavior)
    SchemaRegistry registry = createMockRegistry("", false);
    KafkaMetadataEncoderService service = createService(registry);
    service.init();

    SchemaValue schema = createSchemaWithSensitiveMetadata();
    service.encodeMetadata(schema);

    // Encoder initialized with empty secret — metadata IS encoded (with wrong key, but functional)
    assertNotNull(schema.getMetadata().getProperties().get(SchemaValue.ENCODED_PROPERTY));

    service.close();
  }

  @Test
  public void testEmptySecret_StrictEnabled_EncoderServiceDisabled() throws Exception {
    // With strict validation enabled, empty secret should disable the encoder service
    SchemaRegistry registry = createMockRegistry("", true);
    KafkaMetadataEncoderService service = createService(registry);
    service.init();

    SchemaValue schema = createSchemaWithSensitiveMetadata();
    service.encodeMetadata(schema);

    // Encoder disabled — metadata should NOT be encoded (no-op)
    assertNull(schema.getMetadata().getProperties().get(SchemaValue.ENCODED_PROPERTY));
    assertEquals("sensitive-value", schema.getMetadata().getProperties().get("api_secret"));

    service.close();
  }

  @Test
  public void testValidSecret_StrictEnabled_EncoderServiceInitializes() throws Exception {
    // With strict validation enabled, a non-empty secret should still work normally
    SchemaRegistry registry = createMockRegistry("mysecret", true);
    KafkaMetadataEncoderService service = createService(registry);
    service.init();

    SchemaValue schema = createSchemaWithSensitiveMetadata();
    service.encodeMetadata(schema);

    // Encoder initialized — metadata IS encoded
    assertNotNull(schema.getMetadata().getProperties().get(SchemaValue.ENCODED_PROPERTY));

    service.close();
  }

  @Test
  public void testNullSecret_StrictEnabled_EncoderServiceDisabled() throws Exception {
    // Null secret should disable the encoder regardless of strict validation
    SchemaRegistry registry = createMockRegistry(null, true);
    KafkaMetadataEncoderService service = createService(registry);
    service.init();

    SchemaValue schema = createSchemaWithSensitiveMetadata();
    service.encodeMetadata(schema);

    // Encoder disabled — metadata should NOT be encoded
    assertNull(schema.getMetadata().getProperties().get(SchemaValue.ENCODED_PROPERTY));
    assertEquals("sensitive-value", schema.getMetadata().getProperties().get("api_secret"));

    service.close();
  }
}
