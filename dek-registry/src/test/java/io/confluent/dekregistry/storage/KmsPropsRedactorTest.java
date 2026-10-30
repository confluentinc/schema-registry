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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import io.confluent.dekregistry.client.rest.entities.Kek;
import java.util.HashMap;
import java.util.Map;
import java.util.SortedMap;
import org.junit.Test;

public class KmsPropsRedactorTest {

  @Test
  public void testRedactOmitsSensitiveKeys() {
    Map<String, String> kmsProps = new HashMap<>();
    kmsProps.put("token.id", "s.supersecrettoken");
    kmsProps.put("alicloud.kms.accessKeySecret", "alisecret");
    kmsProps.put("alicloud.kms.securityToken", "alitoken");
    kmsProps.put("namespace", "my-namespace");

    SortedMap<String, String> redacted = KmsPropsRedactor.redact(kmsProps);

    assertFalse(redacted.containsKey("token.id"));
    assertFalse(redacted.containsKey("alicloud.kms.accessKeySecret"));
    assertFalse(redacted.containsKey("alicloud.kms.securityToken"));
    assertEquals("my-namespace", redacted.get("namespace"));
  }

  @Test
  public void testRedactKekOnlyRedactsSharedKeks() {
    Map<String, String> kmsProps = new HashMap<>();
    kmsProps.put("token.id", "s.supersecrettoken");

    Kek shared = new Kek("kek1", "hcvault", "key1", kmsProps, null, true, 1L, null);
    assertFalse(KmsPropsRedactor.redact(shared).getKmsProps().containsKey("token.id"));

    Kek nonShared = new Kek("kek2", "hcvault", "key1", kmsProps, null, false, 1L, null);
    assertEquals("s.supersecrettoken",
        KmsPropsRedactor.redact(nonShared).getKmsProps().get("token.id"));
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
  public void testMergeKeepsRequestedValueWhenGenuinelyUpdated() {
    Map<String, String> existing = new HashMap<>();
    existing.put("token.id", "s.oldtoken");

    Map<String, String> requested = new HashMap<>();
    requested.put("token.id", "s.newtoken");

    SortedMap<String, String> merged = KmsPropsRedactor.merge(requested, existing);

    assertEquals("s.newtoken", merged.get("token.id"));
  }

  @Test
  public void testMergeRemovesSecretOnExplicitNull() {
    Map<String, String> existing = new HashMap<>();
    existing.put("token.id", "s.oldtoken");

    Map<String, String> requested = new HashMap<>();
    requested.put("token.id", null);

    SortedMap<String, String> merged = KmsPropsRedactor.merge(requested, existing);

    assertFalse(merged.containsKey("token.id"));
  }

  @Test
  public void testMergeWithNoExistingValueLeavesRequestedMapUntouched() {
    Map<String, String> requested = new HashMap<>();
    requested.put("namespace", "my-namespace");

    SortedMap<String, String> merged = KmsPropsRedactor.merge(requested, null);

    assertFalse(merged.containsKey("token.id"));
    assertTrue(merged.containsKey("namespace"));
  }
}
