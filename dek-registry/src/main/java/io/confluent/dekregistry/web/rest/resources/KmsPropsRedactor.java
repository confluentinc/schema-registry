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

package io.confluent.dekregistry.web.rest.resources;

import com.google.common.collect.ImmutableSet;
import io.confluent.dekregistry.client.rest.entities.Kek;
import io.confluent.dekregistry.storage.KeyEncryptionKey;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 * Masks KMS authentication secrets before a KEK is returned to a client. KEK
 * {@code kmsProps} carry per-driver KMS auth material (e.g. a Vault token); only the
 * write/KMS-client-construction path needs the real values, never a read response.
 *
 * <p>Secrets are dropped from the map entirely rather than replaced with a placeholder:
 * every supported KMS driver (hcvault, aws, azure, gcp, local) falls back to an ambient
 * credential (an IAM role, managed identity, workload identity, a local env var, etc.)
 * precisely when the corresponding config key is absent/{@code null}. Substituting a
 * non-null placeholder would defeat that fallback and cause the driver to try
 * authenticating with the literal placeholder string instead.
 */
public final class KmsPropsRedactor {

  public static final String REDACTED_VALUE = "[hidden]";

  // Secret-bearing kmsProps keys across the supported KMS drivers.
  private static final Set<String> SENSITIVE_KEYS = ImmutableSet.of(
      "token.id",              // hcvault: Vault auth token
      "approle.secret.id",     // hcvault: Vault AppRole secret id
      "ssl.keystore.password", // hcvault: keystore password for mTLS to Vault
      "secret.access.key",     // aws: IAM secret access key
      "client.secret",         // azure: service principal secret
      "private.key",           // gcp: service account private key
      "secret"                 // local: local KMS secret
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
    if (kek == null) {
      return null;
    }
    return new Kek(kek.getName(), kek.getKmsType(), kek.getKmsKeyId(),
        redact(kek.getKmsProps()), kek.getDoc(), kek.isShared(),
        kek.getTimestamp(), kek.getDeleted());
  }

  public static KeyEncryptionKey redact(KeyEncryptionKey kek) {
    if (kek == null) {
      return null;
    }
    KeyEncryptionKey redacted = new KeyEncryptionKey(kek.getName(), kek.getKmsType(),
        kek.getKmsKeyId(), redact(kek.getKmsProps()), kek.getDoc(), kek.isShared(),
        kek.isDeleted());
    redacted.setOffset(kek.getOffset());
    redacted.setTimestamp(kek.getTimestamp());
    return redacted;
  }

  /**
   * Resolves an incoming {@code kmsProps} update against the currently stored value. A
   * client that read back a redacted kek (via {@link #redact}, which omits secret keys)
   * and later submits that map back verbatim naturally omits those keys too; for each
   * known secret key, an omitted value (or, defensively, one still carrying the legacy
   * {@link #REDACTED_VALUE} placeholder) is treated as "unchanged" and resolved against
   * the stored value rather than clearing/overwriting the real secret. A secret key
   * submitted with any other value is treated as a genuine update.
   */
  public static SortedMap<String, String> merge(
      Map<String, String> requestedKmsProps, Map<String, String> existingKmsProps) {
    SortedMap<String, String> merged = new TreeMap<>(requestedKmsProps);
    if (existingKmsProps != null) {
      for (String key : SENSITIVE_KEYS) {
        if (!existingKmsProps.containsKey(key)) {
          continue;
        }
        if (!merged.containsKey(key) || REDACTED_VALUE.equals(merged.get(key))) {
          merged.put(key, existingKmsProps.get(key));
        }
      }
    }
    return merged;
  }
}
