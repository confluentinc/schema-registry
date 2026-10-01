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

import java.net.URI;
import java.security.GeneralSecurityException;
import java.util.Locale;

final class AliCloudKmsKeyUri {

  private final String original;
  private final String regionId;
  private final String keyId;

  private AliCloudKmsKeyUri(String original, String regionId, String keyId) {
    this.original = original;
    this.regionId = regionId;
    this.keyId = keyId;
  }

  static AliCloudKmsKeyUri parse(String uri) throws GeneralSecurityException {
    if (uri == null || !uri.toLowerCase(Locale.US).startsWith(AliCloudKmsClient.PREFIX)) {
      throw new GeneralSecurityException(
          "Alibaba Cloud KMS URI must start with " + AliCloudKmsClient.PREFIX);
    }

    URI parsed;
    try {
      parsed = URI.create(uri);
    } catch (IllegalArgumentException e) {
      throw new GeneralSecurityException("Invalid Alibaba Cloud KMS URI: " + uri, e);
    }

    String regionId = parsed.getHost();
    String path = parsed.getPath();
    if (regionId == null || regionId.isBlank()) {
      throw new GeneralSecurityException("Alibaba Cloud KMS URI is missing region id");
    }
    if (path == null || path.length() <= 1) {
      throw new GeneralSecurityException("Alibaba Cloud KMS URI is missing key id");
    }

    String keyId = path.substring(1);
    if (keyId.isBlank()) {
      throw new GeneralSecurityException("Alibaba Cloud KMS URI has an empty key id");
    }
    return new AliCloudKmsKeyUri(uri, regionId, keyId);
  }

  String original() {
    return original;
  }

  String regionId() {
    return regionId;
  }

  String keyId() {
    return keyId;
  }
}
