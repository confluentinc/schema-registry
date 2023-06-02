/*
 * Copyright 2023 Confluent Inc.
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

package io.confluent.kafka.schemaregistry.encryption.gcp;

import static io.confluent.kafka.schemaregistry.rules.RuleBase.DEFAULT_NAME;

import com.google.common.collect.ImmutableList;
import io.confluent.kafka.schemaregistry.client.rest.entities.Metadata;
import io.confluent.kafka.schemaregistry.encryption.FieldEncryptionExecutorTest;
import io.confluent.kafka.schemaregistry.encryption.FieldEncryptionProperties;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class GcpFieldEncryptionServiceLoaderTest extends FieldEncryptionExecutorTest {

  public GcpFieldEncryptionServiceLoaderTest() throws Exception {
    super();
  }

  @Override
  protected FieldEncryptionProperties getFieldEncryptionProperties(List<String> ruleNames) {
    return new GcpFieldEncryptionProperties(ImmutableList.of(DEFAULT_NAME));
  }

  @Override
  protected Metadata getMetadata(Map<String, String> properties) {
    properties.put("encrypt.kms.type", "gcp");
    return new Metadata(Collections.emptyMap(), properties, Collections.emptySet());
  }
}