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

package io.confluent.dekregistry.client;

import com.google.common.testing.FakeTicker;
import io.confluent.dekregistry.client.rest.DekRegistryRestService;
import io.confluent.dekregistry.client.rest.entities.CreateDekRequest;
import io.confluent.dekregistry.client.rest.entities.CreateKekRequest;
import io.confluent.dekregistry.client.rest.entities.Dek;
import io.confluent.dekregistry.client.rest.entities.Kek;
import io.confluent.dekregistry.client.rest.entities.UpdateKekRequest;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.schemaregistry.encryption.tink.DekFormat;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class CachedDekRegistryClientTest {

  private static final int CACHE_CAPACITY = 5;
  private static final String KEK_NAME = "kek1";
  private static final String SUBJECT = "foo";
  private static final int VERSION = 1;
  private static final int LATEST_VERSION = -1;
  private static final DekFormat ALGORITHM = DekFormat.AES256_GCM;
  private static final int KEY_NOT_FOUND_ERROR_CODE = 40470;

  private DekRegistryRestService restService;

  @Before
  public void setUp() {
    restService = mock(DekRegistryRestService.class);
  }

  private CachedDekRegistryClient newClient(Map<String, ?> configs, FakeTicker ticker) {
    return new CachedDekRegistryClient(
        restService,
        CACHE_CAPACITY,
        -1,
        configs,
        null,
        ticker
    );
  }

  private static Kek kek() {
    return new Kek(KEK_NAME, "aws-kms", "key-id", null, null, false, 0L, false);
  }

  private static Dek dek() {
    return new Dek(KEK_NAME, SUBJECT, VERSION, ALGORITHM, "encrypted", null, 0L, false);
  }

  private static Map<String, Object> kekTtlConfig() {
    Map<String, Object> configs = new HashMap<>();
    configs.put(DekRegistryClientConfig.MISSING_KEK_CACHE_TTL_CONFIG, 60L);
    return configs;
  }

  private static Map<String, Object> dekTtlConfig() {
    Map<String, Object> configs = new HashMap<>();
    configs.put(DekRegistryClientConfig.MISSING_DEK_CACHE_TTL_CONFIG, 60L);
    return configs;
  }

  private static RestClientException notFound() {
    return new RestClientException("Key not found", 404, KEY_NOT_FOUND_ERROR_CODE);
  }

  // -- KEK negative cache --

  @Test
  public void testMissingKekCache() throws Exception {
    FakeTicker fakeTicker = new FakeTicker();
    CachedDekRegistryClient client = newClient(kekTtlConfig(), fakeTicker);

    when(restService.getKek(KEK_NAME, false))
        .thenThrow(notFound())
        .thenReturn(kek());

    expect404(() -> client.getKek(KEK_NAME, false));

    fakeTicker.advance(59, TimeUnit.SECONDS);
    expect404(() -> client.getKek(KEK_NAME, false));
    verify(restService, times(1)).getKek(KEK_NAME, false);

    fakeTicker.advance(2, TimeUnit.SECONDS);
    Thread.sleep(100);
    assertNotNull(client.getKek(KEK_NAME, false));
    verify(restService, times(2)).getKek(KEK_NAME, false);
  }

  @Test
  public void testMissingKekCacheOffByDefault() throws Exception {
    FakeTicker fakeTicker = new FakeTicker();
    CachedDekRegistryClient client = newClient(new HashMap<>(), fakeTicker);

    when(restService.getKek(KEK_NAME, false)).thenThrow(notFound());

    for (int i = 0; i < 2; i++) {
      expect404(() -> client.getKek(KEK_NAME, false));
    }
    verify(restService, times(2)).getKek(KEK_NAME, false);
  }

  @Test
  public void testCreateKekInvalidatesMissingCache() throws Exception {
    FakeTicker fakeTicker = new FakeTicker();
    CachedDekRegistryClient client = newClient(kekTtlConfig(), fakeTicker);

    // Populate negative cache for (KEK_NAME, lookupDeleted=true). createKek populates the
    // positive cache only for (KEK_NAME, deleted=false), so a follow-up getKek with
    // lookupDeleted=true exercises the negative-cache invalidation rather than being
    // shadowed by a positive-cache hit.
    when(restService.getKek(KEK_NAME, true))
        .thenThrow(notFound())
        .thenReturn(kek());
    when(restService.createKek(any(), any(CreateKekRequest.class))).thenReturn(kek());

    expect404(() -> client.getKek(KEK_NAME, true));

    client.createKek(KEK_NAME, "aws-kms", "key-id", null, null, false, false);

    assertNotNull(client.getKek(KEK_NAME, true));
    verify(restService, times(2)).getKek(KEK_NAME, true);
  }

  @Test
  public void testUpdateKekInvalidatesMissingCache() throws Exception {
    FakeTicker fakeTicker = new FakeTicker();
    CachedDekRegistryClient client = newClient(kekTtlConfig(), fakeTicker);

    when(restService.getKek(KEK_NAME, true))
        .thenThrow(notFound())
        .thenReturn(kek());
    when(restService.updateKek(any(), eq(KEK_NAME), any(UpdateKekRequest.class)))
        .thenReturn(kek());

    expect404(() -> client.getKek(KEK_NAME, true));

    client.updateKek(KEK_NAME, null, null, false);

    assertNotNull(client.getKek(KEK_NAME, true));
    verify(restService, times(2)).getKek(KEK_NAME, true);
  }

  @Test
  public void testUndeleteKekInvalidatesMissingCache() throws Exception {
    FakeTicker fakeTicker = new FakeTicker();
    CachedDekRegistryClient client = newClient(kekTtlConfig(), fakeTicker);

    when(restService.getKek(KEK_NAME, true))
        .thenThrow(notFound())
        .thenReturn(kek());

    expect404(() -> client.getKek(KEK_NAME, true));

    client.undeleteKek(KEK_NAME);

    assertNotNull(client.getKek(KEK_NAME, true));
    verify(restService, times(2)).getKek(KEK_NAME, true);
  }

  @Test
  public void testKekNon404NotCached() throws Exception {
    FakeTicker fakeTicker = new FakeTicker();
    CachedDekRegistryClient client = newClient(kekTtlConfig(), fakeTicker);

    when(restService.getKek(KEK_NAME, false))
        .thenThrow(new RestClientException("server error", 500, 50001));

    for (int i = 0; i < 2; i++) {
      try {
        client.getKek(KEK_NAME, false);
        fail();
      } catch (RestClientException rce) {
        assertEquals(500, rce.getStatus());
      }
    }
    // Both calls must hit REST — non-404 errors must not populate the negative cache.
    verify(restService, times(2)).getKek(KEK_NAME, false);
  }

  @Test
  public void testKek404WithDifferentErrorCodeNotCached() throws Exception {
    FakeTicker fakeTicker = new FakeTicker();
    CachedDekRegistryClient client = newClient(kekTtlConfig(), fakeTicker);

    // 404 status but a non-KEY_NOT_FOUND error code — should not poison the negative cache.
    when(restService.getKek(KEK_NAME, false))
        .thenThrow(new RestClientException("other 404", 404, 40499));

    for (int i = 0; i < 2; i++) {
      try {
        client.getKek(KEK_NAME, false);
        fail();
      } catch (RestClientException rce) {
        assertEquals(40499, rce.getErrorCode());
      }
    }
    verify(restService, times(2)).getKek(KEK_NAME, false);
  }

  // -- DEK negative cache --

  @Test
  public void testMissingDekCache() throws Exception {
    FakeTicker fakeTicker = new FakeTicker();
    CachedDekRegistryClient client = newClient(dekTtlConfig(), fakeTicker);

    when(restService.getDek(eq(KEK_NAME), eq(SUBJECT), eq(ALGORITHM), anyBoolean()))
        .thenThrow(notFound())
        .thenReturn(dek());

    expect404(() -> client.getDek(KEK_NAME, SUBJECT, ALGORITHM, false));

    fakeTicker.advance(59, TimeUnit.SECONDS);
    expect404(() -> client.getDek(KEK_NAME, SUBJECT, ALGORITHM, false));
    verify(restService, times(1)).getDek(KEK_NAME, SUBJECT, ALGORITHM, false);

    fakeTicker.advance(2, TimeUnit.SECONDS);
    Thread.sleep(100);
    assertNotNull(client.getDek(KEK_NAME, SUBJECT, ALGORITHM, false));
    verify(restService, times(2)).getDek(KEK_NAME, SUBJECT, ALGORITHM, false);
  }

  @Test
  public void testMissingDekVersionCache() throws Exception {
    FakeTicker fakeTicker = new FakeTicker();
    CachedDekRegistryClient client = newClient(dekTtlConfig(), fakeTicker);

    when(restService.getDekVersion(eq(KEK_NAME), eq(SUBJECT), eq(VERSION),
        eq(ALGORITHM), anyBoolean()))
        .thenThrow(notFound())
        .thenReturn(dek());

    expect404(() -> client.getDekVersion(KEK_NAME, SUBJECT, VERSION, ALGORITHM, false));

    fakeTicker.advance(59, TimeUnit.SECONDS);
    expect404(() -> client.getDekVersion(KEK_NAME, SUBJECT, VERSION, ALGORITHM, false));
    verify(restService, times(1)).getDekVersion(KEK_NAME, SUBJECT, VERSION, ALGORITHM, false);

    fakeTicker.advance(2, TimeUnit.SECONDS);
    Thread.sleep(100);
    assertNotNull(client.getDekVersion(KEK_NAME, SUBJECT, VERSION, ALGORITHM, false));
    verify(restService, times(2)).getDekVersion(KEK_NAME, SUBJECT, VERSION, ALGORITHM, false);
  }

  @Test
  public void testMissingDekLatestVersionCache() throws Exception {
    FakeTicker fakeTicker = new FakeTicker();
    CachedDekRegistryClient client = newClient(dekTtlConfig(), fakeTicker);

    when(restService.getDekVersion(eq(KEK_NAME), eq(SUBJECT), eq(LATEST_VERSION),
        eq(ALGORITHM), anyBoolean()))
        .thenThrow(notFound())
        .thenReturn(dek());

    expect404(() -> client.getDekLatestVersion(KEK_NAME, SUBJECT, ALGORITHM, false));

    fakeTicker.advance(59, TimeUnit.SECONDS);
    expect404(() -> client.getDekLatestVersion(KEK_NAME, SUBJECT, ALGORITHM, false));
    verify(restService, times(1))
        .getDekVersion(KEK_NAME, SUBJECT, LATEST_VERSION, ALGORITHM, false);

    fakeTicker.advance(2, TimeUnit.SECONDS);
    Thread.sleep(100);
    assertNotNull(client.getDekLatestVersion(KEK_NAME, SUBJECT, ALGORITHM, false));
    verify(restService, times(2))
        .getDekVersion(KEK_NAME, SUBJECT, LATEST_VERSION, ALGORITHM, false);
  }

  @Test
  public void testMissingDekCacheOffByDefault() throws Exception {
    FakeTicker fakeTicker = new FakeTicker();
    CachedDekRegistryClient client = newClient(new HashMap<>(), fakeTicker);

    when(restService.getDek(eq(KEK_NAME), eq(SUBJECT), eq(ALGORITHM), anyBoolean()))
        .thenThrow(notFound());

    for (int i = 0; i < 2; i++) {
      expect404(() -> client.getDek(KEK_NAME, SUBJECT, ALGORITHM, false));
    }
    verify(restService, times(2)).getDek(KEK_NAME, SUBJECT, ALGORITHM, false);
  }

  @Test
  public void testCreateDekInvalidatesMissingCache() throws Exception {
    FakeTicker fakeTicker = new FakeTicker();
    CachedDekRegistryClient client = newClient(dekTtlConfig(), fakeTicker);

    // Populate negative cache for lookupDeleted=true; createDek puts to positive cache
    // for (deleted=false) only, so the follow-up getDek with lookupDeleted=true is not
    // shadowed by a positive hit and exercises negative-cache invalidation.
    when(restService.getDek(eq(KEK_NAME), eq(SUBJECT), eq(ALGORITHM), eq(true)))
        .thenThrow(notFound())
        .thenReturn(dek());
    when(restService.createDek(any(), eq(KEK_NAME), anyBoolean(), any(CreateDekRequest.class)))
        .thenReturn(dek());

    expect404(() -> client.getDek(KEK_NAME, SUBJECT, ALGORITHM, true));

    client.createDek(KEK_NAME, SUBJECT, ALGORITHM, "encrypted");

    assertNotNull(client.getDek(KEK_NAME, SUBJECT, ALGORITHM, true));
    verify(restService, times(2)).getDek(KEK_NAME, SUBJECT, ALGORITHM, true);
  }

  @Test
  public void testUndeleteDekInvalidatesMissingCache() throws Exception {
    FakeTicker fakeTicker = new FakeTicker();
    CachedDekRegistryClient client = newClient(dekTtlConfig(), fakeTicker);

    when(restService.getDek(eq(KEK_NAME), eq(SUBJECT), eq(ALGORITHM), anyBoolean()))
        .thenThrow(notFound())
        .thenReturn(dek());

    expect404(() -> client.getDek(KEK_NAME, SUBJECT, ALGORITHM, false));

    client.undeleteDek(KEK_NAME, SUBJECT, ALGORITHM);

    assertNotNull(client.getDek(KEK_NAME, SUBJECT, ALGORITHM, false));
    verify(restService, times(2)).getDek(KEK_NAME, SUBJECT, ALGORITHM, false);
  }

  @Test
  public void testUndeleteDekVersionInvalidatesMissingCache() throws Exception {
    FakeTicker fakeTicker = new FakeTicker();
    CachedDekRegistryClient client = newClient(dekTtlConfig(), fakeTicker);

    when(restService.getDekVersion(eq(KEK_NAME), eq(SUBJECT), eq(VERSION),
        eq(ALGORITHM), anyBoolean()))
        .thenThrow(notFound())
        .thenReturn(dek());

    expect404(() -> client.getDekVersion(KEK_NAME, SUBJECT, VERSION, ALGORITHM, false));

    client.undeleteDekVersion(KEK_NAME, SUBJECT, VERSION, ALGORITHM);

    assertNotNull(client.getDekVersion(KEK_NAME, SUBJECT, VERSION, ALGORITHM, false));
    verify(restService, times(2)).getDekVersion(KEK_NAME, SUBJECT, VERSION, ALGORITHM, false);
  }

  @Test
  public void testDekNon404NotCached() throws Exception {
    FakeTicker fakeTicker = new FakeTicker();
    CachedDekRegistryClient client = newClient(dekTtlConfig(), fakeTicker);

    when(restService.getDek(eq(KEK_NAME), eq(SUBJECT), eq(ALGORITHM), anyBoolean()))
        .thenThrow(new RestClientException("server error", 500, 50001));

    for (int i = 0; i < 2; i++) {
      try {
        client.getDek(KEK_NAME, SUBJECT, ALGORITHM, false);
        fail();
      } catch (RestClientException rce) {
        assertEquals(500, rce.getStatus());
      }
    }
    verify(restService, times(2)).getDek(KEK_NAME, SUBJECT, ALGORITHM, false);
  }

  @Test
  public void testDek404WithDifferentErrorCodeNotCached() throws Exception {
    FakeTicker fakeTicker = new FakeTicker();
    CachedDekRegistryClient client = newClient(dekTtlConfig(), fakeTicker);

    when(restService.getDek(eq(KEK_NAME), eq(SUBJECT), eq(ALGORITHM), anyBoolean()))
        .thenThrow(new RestClientException("other 404", 404, 40499));

    for (int i = 0; i < 2; i++) {
      try {
        client.getDek(KEK_NAME, SUBJECT, ALGORITHM, false);
        fail();
      } catch (RestClientException rce) {
        assertEquals(40499, rce.getErrorCode());
      }
    }
    verify(restService, times(2)).getDek(KEK_NAME, SUBJECT, ALGORITHM, false);
  }

  @FunctionalInterface
  private interface ThrowingRunnable {
    void run() throws IOException, RestClientException;
  }

  private static void expect404(ThrowingRunnable r) throws IOException {
    try {
      r.run();
      fail("Expected RestClientException");
    } catch (RestClientException rce) {
      assertEquals(KEY_NOT_FOUND_ERROR_CODE, rce.getErrorCode());
    }
  }
}
