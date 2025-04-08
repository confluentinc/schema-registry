/*
 * Copyright 2025 Confluent Inc.
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
package io.confluent.dekregistry.client.rest.entities;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import org.junit.Test;

public class CreateKekRequestTest {

  @Test
  public void testValidKmsKeyIds() {
    CreateKekRequest request = new CreateKekRequest();
    request.setKmsType("aws-kms");
    request.setKmsKeyId("arn:aws:kms:us-east-1:123456789012:key/12345678-1234-1234-1234-123456789012");
    assertTrue(validate(request));

    request.setKmsType("azure-kms");
    request.setKmsKeyId("https://myvault.vault.azure.net/keys/mykey/1234567890abcdef");
    assertTrue(validate(request));

    request.setKmsType("gcp-kms");
    request.setKmsKeyId("projects/my-project/locations/global/keyRings/my-keyring/cryptoKeys/my-key");
    assertTrue(validate(request));

    request.setKmsType("hcvault");
    request.setKmsKeyId("http://localhost:8200/transit/keys/my-key");
    assertTrue(validate(request));
  }

  @Test
  public void testInvalidKmsKeyIds() {
    CreateKekRequest request = new CreateKekRequest();
    request.setKmsType("aws-kms");
    request.setKmsKeyId("bad key id");
    assertFalse(validate(request));

    request.setKmsType("azure-kms");
    request.setKmsKeyId("bad key id");
    assertFalse(validate(request));

    request.setKmsType("gcp-kms");
    request.setKmsKeyId("bad key id");
    assertFalse(validate(request));

    request.setKmsType("hcvault");
    request.setKmsKeyId("bad key id");
    assertFalse(validate(request));
  }

  private boolean validate(CreateKekRequest request) {
    try {
      request.validate();
      return true;
    } catch (Exception e) {
      return false;
    }
  }
}

