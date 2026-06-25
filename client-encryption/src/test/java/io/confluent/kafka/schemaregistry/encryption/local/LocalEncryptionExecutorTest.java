/*
 * Copyright 2025 Confluent Inc.
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

package io.confluent.kafka.schemaregistry.encryption.local;

import io.confluent.kafka.schemaregistry.encryption.EncryptionExecutorTest;
import io.confluent.kafka.schemaregistry.encryption.EncryptionProperties;
import java.util.List;

public class LocalEncryptionExecutorTest extends EncryptionExecutorTest {

  public LocalEncryptionExecutorTest() throws Exception {
    super();
  }

  @Override
  protected EncryptionProperties getEncryptionProperties(
      List<String> ruleNames, Class<?> ruleExecutor) {
    return new LocalEncryptionProperties(ruleNames, ruleExecutor);
  }
}

