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
 * Drops KMS auth secrets from a KEK's {@code kmsProps} before it's output.
 *
 * <p>Secrets are omitted rather than replaced with a placeholder: every KMS driver falls
 * back to an ambient credential when the config key is absent, and a non-null placeholder
 * would defeat that fallback.
 *
 * <p>Used by the server (to redact responses) and this client (to restore a just-written
 * secret into its own cache, since the wire response is always redacted).
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
   * Resolves a requested {@code kmsProps} update against the stored value: an omitted (or
   * legacy {@link #REDACTED_VALUE}) secret key means "unchanged" and is restored from
   * {@code existingKmsProps} (or dropped if nothing is stored). Any other value is a
   * genuine update.
   */
  public static SortedMap<String, String> merge(
      Map<String, String> requestedKmsProps, Map<String, String> existingKmsProps) {
    SortedMap<String, String> merged = new TreeMap<>(requestedKmsProps);
    for (String key : SENSITIVE_KEYS) {
      if (merged.containsKey(key) && !REDACTED_VALUE.equals(merged.get(key))) {
        continue;
      }
      String existingValue = existingKmsProps != null ? existingKmsProps.get(key) : null;
      if (existingValue != null) {
        merged.put(key, existingValue);
      } else {
        merged.remove(key);
      }
    }
    return merged;
  }

  /**
   * Backfills secrets the server redacted from {@code response} using the caller's own
   * {@code request}, so this client's cache keeps a usable credential. Unlike
   * {@link #merge}, a placeholder or omitted key in {@code request} is never copied in --
   * the caller doesn't have the real value either in that case.
   */
  public static Kek restoreWriteTimeSecrets(Kek response, Map<String, String> request) {
    if (response == null) {
      return null;
    }
    SortedMap<String, String> restored = new TreeMap<>(response.getKmsProps());
    if (request != null) {
      for (String key : SENSITIVE_KEYS) {
        // Omitted means unchanged; explicit null means clear.
        if (!request.containsKey(key)) {
          continue;
        }
        String value = request.get(key);
        if (value == null) {
          restored.remove(key);
        } else if (!REDACTED_VALUE.equals(value)) {
          restored.put(key, value);
        }
      }
    }
    return new Kek(response.getName(), response.getKmsType(), response.getKmsKeyId(),
        restored, response.getDoc(), response.isShared(),
        response.getTimestamp(), response.getDeleted());
  }
}
