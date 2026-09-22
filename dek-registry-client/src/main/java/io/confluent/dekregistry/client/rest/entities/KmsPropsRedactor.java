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

package io.confluent.dekregistry.client.rest.entities;

import com.google.common.collect.ImmutableSet;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 * Masks KMS authentication secrets before a KEK is returned over the wire. KEK
 * {@code kmsProps} carry per-driver KMS auth material (e.g. a Vault token); only the
 * write/KMS-client-construction path needs the real values, never a read response.
 *
 * <p>Secrets are dropped from the map entirely rather than replaced with a placeholder:
 * every supported KMS driver (hcvault, aws, azure, gcp, local) falls back to an ambient
 * credential (an IAM role, managed identity, workload identity, a local env var, etc.)
 * precisely when the corresponding config key is absent/{@code null}. Substituting a
 * non-null placeholder would defeat that fallback and cause the driver to try
 * authenticating with the literal placeholder string instead.
 *
 * <p>Used by both the DEK Registry server (to redact outbound REST responses) and this
 * client library (to restore the real value a caller just supplied on a create/update,
 * into its own local cache of that server response, since the wire response itself is
 * always redacted).
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

  /**
   * Backfills any secret key the server redacted from {@code response} with the real
   * value the caller just supplied in {@code request}, so a client's local cache of a
   * create/update response retains a credential it can actually use. Unlike
   * {@link #merge}, a placeholder or missing value in {@code request} is never copied
   * over: {@code request} is what a caller just wrote, not a previously-stored value, so
   * a placeholder there means the caller doesn't actually have the real secret either
   * (e.g. a legacy caller resubmitting a displayed placeholder), and copying it in would
   * cache that literal placeholder string as if it were a usable credential.
   */
  public static Kek restoreWriteTimeSecrets(Kek response, Map<String, String> request) {
    if (response == null) {
      return null;
    }
    SortedMap<String, String> restored = new TreeMap<>(response.getKmsProps());
    if (request != null) {
      for (String key : SENSITIVE_KEYS) {
        String value = request.get(key);
        if (value != null && !REDACTED_VALUE.equals(value)) {
          restored.put(key, value);
        }
      }
    }
    return new Kek(response.getName(), response.getKmsType(), response.getKmsKeyId(),
        restored, response.getDoc(), response.isShared(),
        response.getTimestamp(), response.getDeleted());
  }
}
