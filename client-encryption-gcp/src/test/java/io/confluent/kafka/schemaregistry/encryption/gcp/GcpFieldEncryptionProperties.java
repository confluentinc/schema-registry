/*
 * Copyright 2023 Confluent Inc.
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
package io.confluent.kafka.schemaregistry.encryption.gcp;

import static io.confluent.kafka.schemaregistry.encryption.gcp.GcpKmsDriver.CLIENT_EMAIL;
import static io.confluent.kafka.schemaregistry.encryption.gcp.GcpKmsDriver.CLIENT_ID;
import static io.confluent.kafka.schemaregistry.encryption.gcp.GcpKmsDriver.PRIVATE_KEY;
import static io.confluent.kafka.schemaregistry.encryption.gcp.GcpKmsDriver.PRIVATE_KEY_ID;
import static io.confluent.kafka.schemaregistry.encryption.tink.KmsDriver.TEST_CLIENT;

import io.confluent.kafka.schemaregistry.encryption.FieldEncryptionProperties;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class GcpFieldEncryptionProperties extends FieldEncryptionProperties {

  public GcpFieldEncryptionProperties(List<String> ruleNames) {
    super(ruleNames);
  }

  public GcpFieldEncryptionProperties(List<String> ruleNames, Class<?> ruleExecutor) {
    super(ruleNames, ruleExecutor);
  }

  @Override
  public String getKmsType() {
    return "gcp-kms";
  }

  @Override
  public String getKmsKeyId() {
    return "projects/tink-test/locations/global/keyRings/unit-test/cryptoKeys/aead-key";
  }

  @Override
  public Map<String, Object> getClientProperties(String baseUrls)
      throws Exception {
    List<String> ruleNames = getRuleNames();
    // The following dummy values are borrowed from Google Tink tests
    String clientId = "111876397550362269561";
    String clientEmail = "unit-and-integration-testing@tink-test-infrastructure.iam.gserviceaccount.com";
    String privateKeyId = "1b5021e241ac26833fcd1ced64509d447ff0a25a";
    String privateKey = "-----BEGIN PRIVATE KEY-----\nMIICdwIBADANBgkqhkiG9w0BAQEFAASCAmEwggJdAgEAAoGBAMtJlaQD79xGIC28\nowTpj7wkdi34piSubtDKttgC3lL00ioyQf/WMqLnyDWySNufCjhavQ7/sxXQAUCL\n5B3WDwM8+mFqQM2wJB18NBWBSfGOFSMwVQyWv7Y/1AFr+PvNKVlw4RZ4G8VuJzXZ\n9v/+5zyKv8py66sGVoHPI+LGfIprAgMBAAECgYEAxcgX8PVrnrITiKwpJxReJbyL\nxnpOmw2i/zza3BseVzOebjNrhw/NQDWl0qhcvmBjvyR5IGiiwiwXq8bu8CBdhRiE\nw3vKf1iuVOKhH07RB2wvCaGbVlB/p15gYau3sTRn5nej0tjYHX7xa/St/DwPk2H/\nxYGTRhyYtNL6wdtMjYECQQD+LVVJf0rLnxyPADTcz7Wdb+FUX79nWtMlzQOEB09+\nJj4ie0kD0cIvTQFjV3pOsg3uW2khFpjg110TXpJJfPjhAkEAzL7RhhfDdL7Dn2zl\n1orUthcGa2pzEAmg1tGBNb1pOg7LbVHKSa3GOOwyPRsActoyrPw18/fXaJdEfByY\ne9kwywJAB7rHMjH9y01uZ+bgtKpYYo5JcvBqeLEpZKfkaHp0b2ioURIguU4Csr+L\nwEKjxIrjo5ECFHCEe6nw+arRlgyH4QJBAIfQmEn733LEzB0n7npXU2yKb363eSYN\nTPzSsoREZdXWVIjqtWYUeKXvwA+apryJEw5+qwdvwxslJI+zpE6bLusCQE6M1lO9\nN6A3PtQv7Z3XwrEE/sPEVv4M4VHj0YHLs/32UuSXq5taMizKILfis1Stry4WjRHp\nQxEqdLrIkb13NH8=\n-----END PRIVATE KEY-----\n";
    Map<String, Object> props = new HashMap<>();
    props.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, baseUrls);
    props.put(AbstractKafkaSchemaSerDeConfig.AUTO_REGISTER_SCHEMAS, "false");
    props.put(AbstractKafkaSchemaSerDeConfig.USE_LATEST_VERSION, "true");
    props.put(AbstractKafkaSchemaSerDeConfig.LATEST_CACHE_TTL, "60");
    props.put(AbstractKafkaSchemaSerDeConfig.RULE_EXECUTORS, String.join(",", ruleNames));
    for (String ruleName : ruleNames) {
      props.put(AbstractKafkaSchemaSerDeConfig.RULE_EXECUTORS + "." + ruleName + ".class",
          getRuleExecutor().getName());
      props.put(AbstractKafkaSchemaSerDeConfig.RULE_EXECUTORS + "." + ruleName
              + ".param." + CLIENT_ID,
          clientId);
      props.put(AbstractKafkaSchemaSerDeConfig.RULE_EXECUTORS + "." + ruleName
              + ".param." + CLIENT_EMAIL,
          clientEmail);
      props.put(AbstractKafkaSchemaSerDeConfig.RULE_EXECUTORS + "." + ruleName
              + ".param." + PRIVATE_KEY_ID,
          privateKeyId);
      props.put(AbstractKafkaSchemaSerDeConfig.RULE_EXECUTORS + "." + ruleName
              + ".param." + PRIVATE_KEY,
          privateKey);
      props.put(AbstractKafkaSchemaSerDeConfig.RULE_EXECUTORS + "." + ruleName
              + ".param." + TEST_CLIENT,
          getTestClient());
    }
    return props;
  }

  @Override
  public Object getTestClient() throws Exception {
    return new FakeCloudKms(Collections.singletonList(getKmsKeyId()));
  }
}

