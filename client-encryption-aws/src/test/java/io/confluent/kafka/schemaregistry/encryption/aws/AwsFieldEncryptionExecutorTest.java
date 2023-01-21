/*
 * Copyright 2022 Confluent Inc.
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

package io.confluent.kafka.schemaregistry.encryption.aws;

import static io.confluent.kafka.schemaregistry.encryption.FieldEncryptionExecutor.TEST_CLIENT;
import static io.confluent.kafka.schemaregistry.encryption.aws.AwsFieldEncryptionExecutor.DEFAULT_KMS_KEY_ID;

import com.amazonaws.services.kms.AWSKMS;
import io.confluent.kafka.schemaregistry.encryption.FieldEncryptionExecutorTest;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class AwsFieldEncryptionExecutorTest extends FieldEncryptionExecutorTest {

  public AwsFieldEncryptionExecutorTest() throws Exception {
    super();
  }

  protected Map<String, Object> getClientProperties() throws Exception {
    String keyId = "arn:aws:kms:us-west-2:111122223333:key/1234abcd-12ab-34cd-56ef-1234567890ab";
    AWSKMS testClient = new FakeAwsKms(Collections.singletonList(keyId));
    Map<String, Object> props = new HashMap<>();
    props.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "bogus");
    props.put(AbstractKafkaSchemaSerDeConfig.AUTO_REGISTER_SCHEMAS, "false");
    props.put(AbstractKafkaSchemaSerDeConfig.USE_LATEST_VERSION, "true");
    props.put(AbstractKafkaSchemaSerDeConfig.RULE_EXECUTORS, "aws");
    props.put(AbstractKafkaSchemaSerDeConfig.RULE_EXECUTORS + ".aws.class",
        AwsFieldEncryptionExecutor.class.getName());
    props.put(AbstractKafkaSchemaSerDeConfig.RULE_EXECUTORS + ".aws.param." + DEFAULT_KMS_KEY_ID,
        keyId);
    props.put(AbstractKafkaSchemaSerDeConfig.RULE_EXECUTORS + ".aws.param." + TEST_CLIENT,
        testClient);
    return props;
  }
}