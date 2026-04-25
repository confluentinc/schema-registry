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
import io.confluent.dekregistry.client.rest.entities.CreateKekRequest;
import io.confluent.dekregistry.client.rest.entities.Dek;
import io.confluent.dekregistry.client.rest.entities.Kek;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.schemaregistry.encryption.tink.DekFormat;
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

  @Test
  public void testMissingKekCache() throws Exception {
    Map<String, Object> configs = new HashMap<>();
    configs.put(DekRegistryClientConfig.MISSING_KEK_CACHE_TTL_CONFIG, 60L);

    FakeTicker fakeTicker = new FakeTicker();
    CachedDekRegistryClient client = newClient(configs, fakeTicker);

    when(restService.getKek(KEK_NAME, false))
        .thenThrow(new RestClientException("Key " + KEK_NAME + " not found",
            404, KEY_NOT_FOUND_ERROR_CODE))
        .thenReturn(kek());

    try {
      client.getKek(KEK_NAME, false);
      fail();
    } catch (RestClientException rce) {
      assertEquals(KEY_NOT_FOUND_ERROR_CODE, rce.getErrorCode());
    }

    fakeTicker.advance(59, TimeUnit.SECONDS);

    // Should hit the negative cache (rest service is not consulted)
    try {
      client.getKek(KEK_NAME, false);
      fail();
    } catch (RestClientException rce) {
      assertEquals(KEY_NOT_FOUND_ERROR_CODE, rce.getErrorCode());
    }
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

    // Default TTL is 0, so the negative cache is effectively disabled and
    // each call should hit the rest service.
    when(restService.getKek(KEK_NAME, false))
        .thenThrow(new RestClientException("Key " + KEK_NAME + " not found",
            404, KEY_NOT_FOUND_ERROR_CODE));

    for (int i = 0; i < 2; i++) {
      try {
        client.getKek(KEK_NAME, false);
        fail();
      } catch (RestClientException rce) {
        assertEquals(KEY_NOT_FOUND_ERROR_CODE, rce.getErrorCode());
      }
    }
    verify(restService, times(2)).getKek(KEK_NAME, false);
  }

  @Test
  public void testMissingKekCacheInvalidatedOnCreate() throws Exception {
    Map<String, Object> configs = new HashMap<>();
    configs.put(DekRegistryClientConfig.MISSING_KEK_CACHE_TTL_CONFIG, 60L);

    FakeTicker fakeTicker = new FakeTicker();
    CachedDekRegistryClient client = newClient(configs, fakeTicker);

    Kek created = kek();
    when(restService.getKek(KEK_NAME, false))
        .thenThrow(new RestClientException("Key " + KEK_NAME + " not found",
            404, KEY_NOT_FOUND_ERROR_CODE))
        .thenReturn(created);
    when(restService.createKek(any(), any(CreateKekRequest.class))).thenReturn(created);

    try {
      client.getKek(KEK_NAME, false);
      fail();
    } catch (RestClientException rce) {
      assertEquals(KEY_NOT_FOUND_ERROR_CODE, rce.getErrorCode());
    }

    // createKek should clear the negative cache so the subsequent positive
    // lookup actually re-hits the rest service.
    client.createKek(KEK_NAME, "aws-kms", "key-id", null, null, false, false);
    // Force the positive kek cache to miss so the rest service is consulted.
    client.reset();

    assertNotNull(client.getKek(KEK_NAME, false));
    verify(restService, times(2)).getKek(KEK_NAME, false);
  }

  @Test
  public void testMissingDekCache() throws Exception {
    Map<String, Object> configs = new HashMap<>();
    configs.put(DekRegistryClientConfig.MISSING_DEK_CACHE_TTL_CONFIG, 60L);

    FakeTicker fakeTicker = new FakeTicker();
    CachedDekRegistryClient client = newClient(configs, fakeTicker);

    when(restService.getDek(eq(KEK_NAME), eq(SUBJECT), eq(ALGORITHM), anyBoolean()))
        .thenThrow(new RestClientException("Key " + SUBJECT + " not found",
            404, KEY_NOT_FOUND_ERROR_CODE))
        .thenReturn(dek());

    try {
      client.getDek(KEK_NAME, SUBJECT, ALGORITHM, false);
      fail();
    } catch (RestClientException rce) {
      assertEquals(KEY_NOT_FOUND_ERROR_CODE, rce.getErrorCode());
    }

    fakeTicker.advance(59, TimeUnit.SECONDS);

    // Should hit the negative cache
    try {
      client.getDek(KEK_NAME, SUBJECT, ALGORITHM, false);
      fail();
    } catch (RestClientException rce) {
      assertEquals(KEY_NOT_FOUND_ERROR_CODE, rce.getErrorCode());
    }
    verify(restService, times(1)).getDek(KEK_NAME, SUBJECT, ALGORITHM, false);

    fakeTicker.advance(2, TimeUnit.SECONDS);
    Thread.sleep(100);
    assertNotNull(client.getDek(KEK_NAME, SUBJECT, ALGORITHM, false));
    verify(restService, times(2)).getDek(KEK_NAME, SUBJECT, ALGORITHM, false);
  }

  @Test
  public void testMissingDekVersionCache() throws Exception {
    Map<String, Object> configs = new HashMap<>();
    configs.put(DekRegistryClientConfig.MISSING_DEK_CACHE_TTL_CONFIG, 60L);

    FakeTicker fakeTicker = new FakeTicker();
    CachedDekRegistryClient client = newClient(configs, fakeTicker);

    when(restService.getDekVersion(eq(KEK_NAME), eq(SUBJECT), eq(VERSION),
        eq(ALGORITHM), anyBoolean()))
        .thenThrow(new RestClientException("Key " + SUBJECT + " not found",
            404, KEY_NOT_FOUND_ERROR_CODE))
        .thenReturn(dek());

    try {
      client.getDekVersion(KEK_NAME, SUBJECT, VERSION, ALGORITHM, false);
      fail();
    } catch (RestClientException rce) {
      assertEquals(KEY_NOT_FOUND_ERROR_CODE, rce.getErrorCode());
    }

    fakeTicker.advance(59, TimeUnit.SECONDS);

    try {
      client.getDekVersion(KEK_NAME, SUBJECT, VERSION, ALGORITHM, false);
      fail();
    } catch (RestClientException rce) {
      assertEquals(KEY_NOT_FOUND_ERROR_CODE, rce.getErrorCode());
    }
    verify(restService, times(1)).getDekVersion(KEK_NAME, SUBJECT, VERSION, ALGORITHM, false);

    fakeTicker.advance(2, TimeUnit.SECONDS);
    Thread.sleep(100);
    assertNotNull(client.getDekVersion(KEK_NAME, SUBJECT, VERSION, ALGORITHM, false));
    verify(restService, times(2)).getDekVersion(KEK_NAME, SUBJECT, VERSION, ALGORITHM, false);
  }

  @Test
  public void testMissingDekCacheOffByDefault() throws Exception {
    FakeTicker fakeTicker = new FakeTicker();
    CachedDekRegistryClient client = newClient(new HashMap<>(), fakeTicker);

    when(restService.getDek(eq(KEK_NAME), eq(SUBJECT), eq(ALGORITHM), anyBoolean()))
        .thenThrow(new RestClientException("Key " + SUBJECT + " not found",
            404, KEY_NOT_FOUND_ERROR_CODE));

    for (int i = 0; i < 2; i++) {
      try {
        client.getDek(KEK_NAME, SUBJECT, ALGORITHM, false);
        fail();
      } catch (RestClientException rce) {
        assertEquals(KEY_NOT_FOUND_ERROR_CODE, rce.getErrorCode());
      }
    }
    verify(restService, times(2)).getDek(KEK_NAME, SUBJECT, ALGORITHM, false);
  }
}
