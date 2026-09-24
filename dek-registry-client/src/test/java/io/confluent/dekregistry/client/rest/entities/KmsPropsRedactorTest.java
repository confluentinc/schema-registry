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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.HashMap;
import java.util.Map;
import java.util.SortedMap;
import org.junit.Test;

public class KmsPropsRedactorTest {

  @Test
  public void testRedactOmitsSensitiveKeys() {
    Map<String, String> kmsProps = new HashMap<>();
    kmsProps.put("token.id", "s.supersecrettoken");
    kmsProps.put("namespace", "my-namespace");

    SortedMap<String, String> redacted = KmsPropsRedactor.redact(kmsProps);

    assertFalse(redacted.containsKey("token.id"));
    assertEquals("my-namespace", redacted.get("namespace"));
  }

  @Test
  public void testMergeRestoresRealSecretWhenRequestOmitsKey() {
    Map<String, String> existing = new HashMap<>();
    existing.put("token.id", "s.supersecrettoken");
    existing.put("namespace", "my-namespace");

    // The client PUTs a GET response straight back, plus a genuine unrelated change.
    Map<String, String> requested = new HashMap<>();
    requested.put("namespace", "updated-namespace");

    SortedMap<String, String> merged = KmsPropsRedactor.merge(requested, existing);

    assertEquals("s.supersecrettoken", merged.get("token.id"));
    assertEquals("updated-namespace", merged.get("namespace"));
  }

  @Test
  public void testMergeRestoresRealSecretWhenRequestCarriesLegacyPlaceholder() {
    Map<String, String> existing = new HashMap<>();
    existing.put("token.id", "s.supersecrettoken");

    // Defensive: some caller sends the literal placeholder back instead of omitting it.
    Map<String, String> requested = new HashMap<>();
    requested.put("token.id", KmsPropsRedactor.REDACTED_VALUE);

    SortedMap<String, String> merged = KmsPropsRedactor.merge(requested, existing);

    assertEquals("s.supersecrettoken", merged.get("token.id"));
  }

  @Test
  public void testMergeKeepsRequestedValueWhenGenuinelyUpdated() {
    Map<String, String> existing = new HashMap<>();
    existing.put("token.id", "s.oldtoken");

    Map<String, String> requested = new HashMap<>();
    requested.put("token.id", "s.newtoken");

    SortedMap<String, String> merged = KmsPropsRedactor.merge(requested, existing);

    assertEquals("s.newtoken", merged.get("token.id"));
  }

  @Test
  public void testMergeWithNoExistingValueLeavesRequestedMapUntouched() {
    Map<String, String> requested = new HashMap<>();
    requested.put("namespace", "my-namespace");

    SortedMap<String, String> merged = KmsPropsRedactor.merge(requested, null);

    assertFalse(merged.containsKey("token.id"));
    assertTrue(merged.containsKey("namespace"));
  }

  @Test
  public void testMergeNormalizesPlaceholderToOmittedWhenNoStoredValue() {
    // Genuinely new create: a placeholder must never be persisted verbatim.
    Map<String, String> requested = new HashMap<>();
    requested.put("token.id", KmsPropsRedactor.REDACTED_VALUE);

    SortedMap<String, String> mergedWithNoExisting = KmsPropsRedactor.merge(requested, null);
    assertFalse(mergedWithNoExisting.containsKey("token.id"));

    Map<String, String> existingWithoutThisKey = new HashMap<>();
    existingWithoutThisKey.put("namespace", "my-namespace");
    SortedMap<String, String> mergedWithUnrelatedExisting =
        KmsPropsRedactor.merge(requested, existingWithoutThisKey);
    assertFalse(mergedWithUnrelatedExisting.containsKey("token.id"));
  }

  @Test
  public void testRestoreWriteTimeSecretsBackfillsFromOriginalRequest() {
    // Simulates the server's create/update response, which always redacts secrets.
    Map<String, String> responseProps = new HashMap<>();
    responseProps.put("namespace", "my-namespace");
    Kek response = new Kek("kek1", "hcvault", "key1", responseProps, null, true, 1L, null);

    // The caller's original request, which carried the real secret.
    Map<String, String> requestProps = new HashMap<>();
    requestProps.put("token.id", "s.supersecrettoken");
    requestProps.put("namespace", "my-namespace");

    Kek restored = KmsPropsRedactor.restoreWriteTimeSecrets(response, requestProps);

    assertEquals("s.supersecrettoken", restored.getKmsProps().get("token.id"));
    assertEquals("my-namespace", restored.getKmsProps().get("namespace"));
  }

  @Test
  public void testRestoreWriteTimeSecretsClearsValueOnExplicitNull() {
    // A value a prior cache-fallback layer already restored onto the base for this call.
    Map<String, String> responseProps = new HashMap<>();
    responseProps.put("token.id", "s.oldcachedtoken");
    responseProps.put("namespace", "my-namespace");
    Kek response = new Kek("kek1", "hcvault", "key1", responseProps, null, true, 1L, null);

    // The caller explicitly clears the secret -- distinct from omitting it entirely.
    Map<String, String> requestProps = new HashMap<>();
    requestProps.put("token.id", null);

    Kek restored = KmsPropsRedactor.restoreWriteTimeSecrets(response, requestProps);

    assertFalse(restored.getKmsProps().containsKey("token.id"));
    assertEquals("my-namespace", restored.getKmsProps().get("namespace"));
  }

  @Test
  public void testRestoreWriteTimeSecretsPreservesValueWhenKeyOmitted() {
    Map<String, String> responseProps = new HashMap<>();
    responseProps.put("token.id", "s.oldcachedtoken");
    Kek response = new Kek("kek1", "hcvault", "key1", responseProps, null, true, 1L, null);

    // token.id is entirely absent here -- distinct from an explicit null.
    Map<String, String> requestProps = new HashMap<>();
    requestProps.put("namespace", "my-namespace");

    Kek restored = KmsPropsRedactor.restoreWriteTimeSecrets(response, requestProps);

    assertEquals("s.oldcachedtoken", restored.getKmsProps().get("token.id"));
  }

  @Test
  public void testRestoreWriteTimeSecretsIsNoOpWithoutOriginalRequest() {
    Map<String, String> responseProps = new HashMap<>();
    responseProps.put("namespace", "my-namespace");
    Kek response = new Kek("kek1", "hcvault", "key1", responseProps, null, true, 1L, null);

    Kek restored = KmsPropsRedactor.restoreWriteTimeSecrets(response, null);

    assertFalse(restored.getKmsProps().containsKey("token.id"));
    assertEquals("my-namespace", restored.getKmsProps().get("namespace"));
  }

  @Test
  public void testRestoreWriteTimeSecretsIgnoresPlaceholderInOriginalRequest() {
    // A legacy caller resubmitting the placeholder doesn't have the real value either.
    Map<String, String> responseProps = new HashMap<>();
    responseProps.put("namespace", "my-namespace");
    Kek response = new Kek("kek1", "hcvault", "key1", responseProps, null, true, 1L, null);

    Map<String, String> requestProps = new HashMap<>();
    requestProps.put("token.id", KmsPropsRedactor.REDACTED_VALUE);

    Kek restored = KmsPropsRedactor.restoreWriteTimeSecrets(response, requestProps);

    assertFalse(restored.getKmsProps().containsKey("token.id"));
  }
}
