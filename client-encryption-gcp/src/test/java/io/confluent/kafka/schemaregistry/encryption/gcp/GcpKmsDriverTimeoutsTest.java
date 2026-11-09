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

package io.confluent.kafka.schemaregistry.encryption.gcp;

import static io.confluent.kafka.schemaregistry.encryption.tink.KmsTimeouts.KMS_CONNECT_TIMEOUT_MS;
import static io.confluent.kafka.schemaregistry.encryption.tink.KmsTimeouts.KMS_REQUEST_TIMEOUT_MS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import com.google.api.client.http.GenericUrl;
import com.google.api.client.http.HttpRequest;
import com.google.api.services.cloudkms.v1.CloudKMS;
import com.google.auth.oauth2.AccessToken;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.crypto.tink.KmsClient;
import com.google.crypto.tink.integration.gcpkms.GcpKmsClient;
import io.confluent.kafka.schemaregistry.encryption.tink.KmsTimeouts;
import java.lang.reflect.Field;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.junit.Test;

public class GcpKmsDriverTimeoutsTest {

  private static final String KEY_URI =
      "gcp-kms://projects/p/locations/global/keyRings/r/cryptoKeys/k";

  private static GoogleCredentials fakeCredentials() {
    // A non-expiring token, so building a request never tries to refresh over the network.
    return GoogleCredentials.create(
        new AccessToken("token", new Date(System.currentTimeMillis() + 3_600_000L)));
  }

  private static HttpRequest sampleRequest(CloudKMS cloudKms) throws Exception {
    return cloudKms.getRequestFactory()
        .buildGetRequest(new GenericUrl("https://cloudkms.googleapis.com/"));
  }

  private static CloudKMS cloudKmsOf(KmsClient client) throws Exception {
    Field field = GcpKmsClient.class.getDeclaredField("cloudKms");
    field.setAccessible(true);
    return (CloudKMS) field.get(client);
  }

  @Test
  public void timeoutsAreAppliedToEveryRequest() throws Exception {
    Map<String, Object> configs = new HashMap<>();
    configs.put(KMS_CONNECT_TIMEOUT_MS, "1500");
    configs.put(KMS_REQUEST_TIMEOUT_MS, "4000");
    KmsClient client = GcpKmsDriver.newKmsClientWithCloudKms(Optional.of(KEY_URI),
        Optional.of(fakeCredentials()), null, KmsTimeouts.fromConfigs(configs));

    HttpRequest request = sampleRequest(cloudKmsOf(client));
    assertEquals(1500, request.getConnectTimeout());
    assertEquals(4000, request.getReadTimeout());
    assertEquals(4000, request.getWriteTimeout());
  }

  @Test
  public void noTimeoutsKeepsTinkDefaults() throws Exception {
    KmsClient client = GcpKmsDriver.newKmsClientWithCloudKms(Optional.of(KEY_URI),
        Optional.of(fakeCredentials()), null, KmsTimeouts.NONE);

    // Built by Tink itself, so google-http-client's defaults apply (20s connect and read).
    CloudKMS cloudKms = cloudKmsOf(client);
    assertNotNull(cloudKms);
    HttpRequest request = sampleRequest(cloudKms);
    assertEquals(20_000, request.getConnectTimeout());
    assertEquals(20_000, request.getReadTimeout());
  }
}
