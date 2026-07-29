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

import com.aliyun.kms20160120.Client;
import com.aliyun.kms20160120.models.DecryptRequest;
import com.aliyun.kms20160120.models.DecryptResponse;
import com.aliyun.kms20160120.models.EncryptRequest;
import com.aliyun.kms20160120.models.EncryptResponse;
import com.aliyun.teaopenapi.models.Config;
import java.security.GeneralSecurityException;
import java.util.Map;

final class AliCloudKmsSdkOperations implements AliCloudKmsOperations {

  private final Client client;

  private AliCloudKmsSdkOperations(Client client) {
    this.client = client;
  }

  @Override
  public String encrypt(String keyId, String plaintextBase64, Map<String, ?> encryptionContext)
      throws Exception {
    EncryptRequest request = new EncryptRequest()
        .setKeyId(keyId)
        .setPlaintext(plaintextBase64)
        .setEncryptionContext(encryptionContext);
    EncryptResponse response = client.encrypt(request);
    return response == null || response.getBody() == null
        ? null
        : response.getBody().getCiphertextBlob();
  }

  @Override
  public DecryptResult decrypt(String ciphertextBlob, Map<String, ?> encryptionContext)
      throws Exception {
    DecryptRequest request = new DecryptRequest()
        .setCiphertextBlob(ciphertextBlob)
        .setEncryptionContext(encryptionContext);
    DecryptResponse response = client.decrypt(request);
    return response == null || response.getBody() == null
        ? null
        : new DecryptResult(response.getBody().getPlaintext(), response.getBody().getKeyId());
  }

  static final class Factory implements AliCloudKmsOperationsFactory {

    @Override
    public AliCloudKmsOperations create(AliCloudKmsConfig config) throws GeneralSecurityException {
      try {
        Config sdkConfig = new Config()
            .setCredential(config.credentialsClient())
            .setRegionId(config.regionId())
            .setEndpoint(config.endpoint())
            .setCa(config.caContent())
            .setProtocol("https");
        return new AliCloudKmsSdkOperations(new Client(sdkConfig));
      } catch (Exception e) {
        throw new GeneralSecurityException("failed to initialize Alibaba Cloud KMS client", e);
      }
    }
  }
}
