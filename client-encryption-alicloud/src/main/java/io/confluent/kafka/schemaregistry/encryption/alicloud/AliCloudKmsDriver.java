/*
 * Copyright 2026 Confluent Inc.
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

package io.confluent.kafka.schemaregistry.encryption.alicloud;

import com.aliyun.tea.TeaException;
import com.google.crypto.tink.KmsClient;
import io.confluent.kafka.schemaregistry.encryption.tink.KmsDriver;
import java.security.GeneralSecurityException;
import java.util.Map;
import java.util.Optional;

public final class AliCloudKmsDriver implements KmsDriver {

  public static final String KMS_TYPE = "alicloud-kms";
  public static final String ENDPOINT_CONFIG = AliCloudKmsConfig.ENDPOINT_CONFIG;
  public static final String CA_FILE_CONFIG = AliCloudKmsConfig.CA_FILE_CONFIG;
  public static final String ACCESS_KEY_ID_CONFIG = AliCloudKmsConfig.ACCESS_KEY_ID_CONFIG;
  public static final String ACCESS_KEY_SECRET_CONFIG = AliCloudKmsConfig.ACCESS_KEY_SECRET_CONFIG;
  public static final String SECURITY_TOKEN_CONFIG = AliCloudKmsConfig.SECURITY_TOKEN_CONFIG;
  public static final String CREDENTIAL_TYPE_CONFIG = AliCloudKmsConfig.CREDENTIAL_TYPE_CONFIG;
  public static final String ROLE_ARN_CONFIG = AliCloudKmsConfig.ROLE_ARN_CONFIG;
  public static final String ROLE_SESSION_NAME_CONFIG = AliCloudKmsConfig.ROLE_SESSION_NAME_CONFIG;
  public static final String ROLE_SESSION_EXPIRATION_CONFIG =
      AliCloudKmsConfig.ROLE_SESSION_EXPIRATION_CONFIG;
  public static final String POLICY_CONFIG = AliCloudKmsConfig.POLICY_CONFIG;
  public static final String STS_ENDPOINT_CONFIG = AliCloudKmsConfig.STS_ENDPOINT_CONFIG;
  public static final String EXTERNAL_ID_CONFIG = AliCloudKmsConfig.EXTERNAL_ID_CONFIG;

  @Override
  public String getKeyUrlPrefix() {
    return AliCloudKmsClient.PREFIX;
  }

  @Override
  public boolean isAccessDeniedException(Throwable t) {
    if (!(t instanceof TeaException)) {
      return false;
    }
    TeaException e = (TeaException) t;
    Integer statusCode = e.getStatusCode();
    if (statusCode != null && isAccessDeniedStatus(statusCode)) {
      return true;
    }
    String code = e.getCode();
    return code != null && (code.contains("AccessDenied") || code.contains("Unauthorized"));
  }

  @Override
  public KmsClient newKmsClient(Map<String, ?> configs, Optional<String> kekUrl)
      throws GeneralSecurityException {
    Object testClient = getTestClient(configs);
    if (testClient instanceof AliCloudKmsOperationsFactory) {
      return newKmsClientWithOperationsFactory(
          configs,
          kekUrl,
          (AliCloudKmsOperationsFactory) testClient);
    }
    if (testClient instanceof AliCloudKmsOperations) {
      AliCloudKmsOperations operations = (AliCloudKmsOperations) testClient;
      return newKmsClientWithOperationsFactory(configs, kekUrl, config -> operations);
    }
    return newKmsClientWithOperationsFactory(
        configs,
        kekUrl,
        new AliCloudKmsSdkOperations.Factory());
  }

  static KmsClient newKmsClientWithOperationsFactory(
      Map<String, ?> configs,
      Optional<String> kekUrl,
      AliCloudKmsOperationsFactory operationsFactory) {
    return new AliCloudKmsClient(configs, kekUrl, operationsFactory, System::getenv);
  }
}
