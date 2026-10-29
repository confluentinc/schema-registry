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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

import io.confluent.dekregistry.storage.exceptions.DekGenerationException;
import io.confluent.dekregistry.web.rest.exceptions.DekRegistryErrors;
import io.confluent.kafka.schemaregistry.encryption.tink.DekFormat;
import io.confluent.rest.exceptions.RestException;
import io.kcache.Cache;
import io.kcache.KeyValueIterator;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for the default-delegation behavior of {@code AbstractDekRegistry#getKey}
 * and {@code AbstractDekRegistry#rangeKeys}.
 *
 * <p>The defaults route through the (deprecated) {@code keys()} view so kafka-based
 * subclasses inherit working behavior with no override. Non-kafka subclasses are
 * expected to override both methods and never need a real {@code keys()} — the
 * existing {@code UnsupportedOperationException} default for {@code keys()} stays
 * correct in that case.
 */
public class AbstractDekRegistryDefaultMethodsTest {

  @Test
  public void getKey_defaultDelegatesToKeysGet() {
    @SuppressWarnings("unchecked")
    Cache<EncryptionKeyId, EncryptionKey> cache = mock(Cache.class);

    AbstractDekRegistry registry = mock(
        AbstractDekRegistry.class, withSettings().defaultAnswer(CALLS_REAL_METHODS));
    // Use doReturn to stub keys() without invoking the real (UOE-throwing) impl.
    doReturn(cache).when(registry).keys();

    DataEncryptionKeyId id = new DataEncryptionKeyId(
        "tenant", "kek", "subject", DekFormat.AES128_GCM, 1);
    DataEncryptionKey expected = new DataEncryptionKey(
        "kek", "subject", DekFormat.AES128_GCM, 1, "material", false);
    when(cache.get(id)).thenReturn(expected);

    assertSame(expected, registry.getKey(id));
    verify(cache).get(id);
  }

  @Test
  public void rangeKeys_defaultDelegatesToKeysRange() {
    @SuppressWarnings("unchecked")
    Cache<EncryptionKeyId, EncryptionKey> cache = mock(Cache.class);
    @SuppressWarnings("unchecked")
    KeyValueIterator<EncryptionKeyId, EncryptionKey> iter = mock(KeyValueIterator.class);

    AbstractDekRegistry registry = mock(
        AbstractDekRegistry.class, withSettings().defaultAnswer(CALLS_REAL_METHODS));
    doReturn(cache).when(registry).keys();
    when(cache.range(any(), anyBoolean(), any(), anyBoolean())).thenReturn(iter);

    DataEncryptionKeyId start = new DataEncryptionKeyId(
        "tenant", "kek", "a", DekFormat.AES128_GCM, 1);
    DataEncryptionKeyId end = new DataEncryptionKeyId(
        "tenant", "kek", "z", DekFormat.AES128_GCM, 1);

    assertSame(iter, registry.rangeKeys(start, true, end, false));
    verify(cache).range(eq(start), eq(true), eq(end), eq(false));
  }

  /**
   * If a subclass doesn't have a real {@code keys()}, the default {@code getKey}
   * delegation surfaces the same {@code UnsupportedOperationException} — caller
   * sees a single, consistent failure mode regardless of which method they hit.
   */
  @Test
  public void getKey_defaultPropagatesUnsupportedOperationFromKeys() {
    AbstractDekRegistry registry = mock(
        AbstractDekRegistry.class, withSettings().defaultAnswer(CALLS_REAL_METHODS));
    DataEncryptionKeyId id = new DataEncryptionKeyId(
        "tenant", "kek", "subject", DekFormat.AES128_GCM, 1);

    assertThrows(UnsupportedOperationException.class, () -> registry.getKey(id));
  }

  // ---- KMS access-denied classification (HTTP 403 vs 500) ----

  @Test
  public void dekGenerationException_accessDeniedFlagDefaultsFalse() {
    assertFalse(new DekGenerationException("boom").isAccessDenied());
    assertFalse(new DekGenerationException("boom", new RuntimeException()).isAccessDenied());
  }

  @Test
  public void dekGenerationException_accessDeniedFlagSet() {
    DekGenerationException denied =
        new DekGenerationException("denied", new RuntimeException(), true);
    assertTrue(denied.isAccessDenied());

    DekGenerationException notDenied =
        new DekGenerationException("other", new RuntimeException(), false);
    assertFalse(notDenied.isAccessDenied());
  }

  @Test
  public void dekRegistryErrors_mapsAccessDeniedTo403() {
    RestException e = DekRegistryErrors.dekGenerationException(
        new DekGenerationException("denied", new RuntimeException(), true));
    assertEquals(403, e.getStatus());
    assertEquals(DekRegistryErrors.DEK_GENERATION_FORBIDDEN_ERROR_CODE, e.getErrorCode());
  }

  @Test
  public void dekRegistryErrors_mapsOtherFailuresTo500() {
    RestException e = DekRegistryErrors.dekGenerationException(
        new DekGenerationException("boom"));
    assertEquals(500, e.getStatus());
    assertEquals(DekRegistryErrors.DEK_GENERATION_ERROR_CODE, e.getErrorCode());
  }
}
