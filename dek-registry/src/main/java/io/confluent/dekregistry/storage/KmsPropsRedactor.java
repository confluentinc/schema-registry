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

package io.confluent.dekregistry.storage;

import com.google.common.collect.ImmutableSet;
import io.confluent.dekregistry.client.rest.entities.Kek;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 * Drops KMS auth secrets from a shared KEK's {@code kmsProps} before it's output. Only
 * shared KEKs are redacted: their secrets are used solely by the server, whereas clients
 * of a non-shared KEK call the KMS themselves and need them.
 *
 * <p>Secrets are omitted rather than replaced with a placeholder: every KMS driver falls
 * back to an ambient credential when the config key is absent, and a non-null placeholder
 * would defeat that fallback.
 */
public final class KmsPropsRedactor {

  // Secret-bearing kmsProps keys across the supported KMS drivers.
  private static final Set<String> SENSITIVE_KEYS = ImmutableSet.of(
      "token.id",              // hcvault: Vault auth token
      "approle.secret.id",     // hcvault: Vault AppRole secret id
      "ssl.keystore.password", // hcvault: keystore password for mTLS to Vault
      "secret.access.key",     // aws: IAM secret access key
      "client.secret",         // azure: service principal secret
      "private.key",           // gcp: service account private key
      "secret",                // local: local KMS secret
      "alicloud.kms.accessKeySecret", // alicloud: access key secret
      "alicloud.kms.securityToken"    // alicloud: STS security token
  );

  private KmsPropsRedactor() {
  }

  public static SortedMap<String, String> redact(Map<String, String> kmsProps) {
    if (kmsProps == null || kmsProps.isEmpty()) {
      return Collections.emptySortedMap();
    }
    SortedMap<String, String> redacted = new TreeMap<>(kmsProps);
    redacted.keySet().removeAll(SENSITIVE_KEYS);
    return redacted;
  }

  public static Kek redact(Kek kek) {
    if (kek == null || !kek.isShared()) {
      return kek;
    }
    return new Kek(kek.getName(), kek.getKmsType(), kek.getKmsKeyId(),
        redact(kek.getKmsProps()), kek.getDoc(), kek.isShared(),
        kek.getTimestamp(), kek.getDeleted());
  }

  public static KeyEncryptionKey redact(KeyEncryptionKey kek) {
    if (kek == null || !kek.isShared()) {
      return kek;
    }
    KeyEncryptionKey redacted = new KeyEncryptionKey(kek.getName(), kek.getKmsType(),
        kek.getKmsKeyId(), redact(kek.getKmsProps()),
        kek.getDoc(), kek.isShared(), kek.isDeleted());
    redacted.setOffset(kek.getOffset());
    redacted.setTimestamp(kek.getTimestamp());
    return redacted;
  }

  /**
   * Resolves a requested {@code kmsProps} update against the stored value: an omitted
   * secret key means "unchanged" and is restored from {@code existingKmsProps}. An explicit
   * {@code null} clears the secret. Any other value is a genuine update.
   */
  public static SortedMap<String, String> merge(
      Map<String, String> requestedKmsProps, Map<String, String> existingKmsProps) {
    SortedMap<String, String> merged = new TreeMap<>(requestedKmsProps);
    for (String key : SENSITIVE_KEYS) {
      if (merged.containsKey(key)) {
        if (merged.get(key) == null) {
          merged.remove(key);
        }
        continue;
      }
      String existingValue = existingKmsProps != null ? existingKmsProps.get(key) : null;
      if (existingValue != null) {
        merged.put(key, existingValue);
      }
    }
    return merged;
  }
}
