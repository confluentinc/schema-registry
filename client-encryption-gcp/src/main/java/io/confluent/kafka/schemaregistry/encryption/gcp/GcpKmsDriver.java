/*
 * Copyright 2022 Confluent Inc.
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

import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.http.HttpRequestInitializer;
import com.google.api.client.http.HttpResponseException;
import com.google.api.client.json.gson.GsonFactory;
import com.google.api.services.cloudkms.v1.CloudKMS;
import com.google.api.services.cloudkms.v1.CloudKMSScopes;
import com.google.auth.http.HttpCredentialsAdapter;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.crypto.tink.KmsClient;
import com.google.crypto.tink.integration.gcpkms.GcpKmsClient;
import io.confluent.kafka.schemaregistry.encryption.tink.KmsDriver;
import io.confluent.kafka.schemaregistry.encryption.tink.KmsTimeouts;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;

public class GcpKmsDriver implements KmsDriver {

  public static final String ACCOUNT_TYPE = "account.type";
  public static final String CLIENT_ID = "client.id";
  public static final String CLIENT_EMAIL = "client.email";
  public static final String PRIVATE_KEY_ID = "private.key.id";
  public static final String PRIVATE_KEY = "private.key";

  public GcpKmsDriver() {
  }

  @Override
  public String getKeyUrlPrefix() {
    return GcpKmsClient.PREFIX;
  }

  @Override
  public boolean isAccessDeniedException(Throwable t) {
    if (!(t instanceof HttpResponseException)) {
      return false;
    }
    return isAccessDeniedStatus(((HttpResponseException) t).getStatusCode());
  }

  private GoogleCredentials getCredentials(Map<String, ?> configs)
      throws GeneralSecurityException {
    try {
      String accountType = (String) configs.get(ACCOUNT_TYPE);
      if (accountType == null) {
        accountType = "service_account";
      }
      String clientId = (String) configs.get(CLIENT_ID);
      String clientEmail = (String) configs.get(CLIENT_EMAIL);
      String privateKeyId = (String) configs.get(PRIVATE_KEY_ID);
      String privateKey = (String) configs.get(PRIVATE_KEY);
      if (clientId != null && clientEmail != null && privateKeyId != null && privateKey != null) {
        String keys = "{ \"type\": \"" + accountType
            + "\", \"client_id\": \"" + clientId
            + "\", \"client_email\": \"" + clientEmail
            + "\", \"private_key_id\": \"" + privateKeyId
            + "\", \"private_key\": \"" + privateKey + "\" }";
        return GoogleCredentials.fromStream(
            new ByteArrayInputStream(keys.getBytes(StandardCharsets.UTF_8)));
      } else {
        return GoogleCredentials.getApplicationDefault();
      }
    } catch (Exception e) {
      throw new GeneralSecurityException("cannot load credentials", e);
    }
  }

  @Override
  public KmsClient newKmsClient(Map<String, ?> configs, Optional<String> kekUrl)
      throws GeneralSecurityException {
    CloudKMS testClient = (CloudKMS) getTestClient(configs);
    Optional<GoogleCredentials> creds = testClient != null
        ? Optional.empty()
        : Optional.ofNullable(getCredentials(configs));
    return newKmsClientWithCloudKms(kekUrl, creds, testClient, KmsTimeouts.fromConfigs(configs));
  }

  protected static KmsClient newKmsClientWithCloudKms(
      Optional<String> keyUri, Optional<GoogleCredentials> credentials, CloudKMS cloudKms)
      throws GeneralSecurityException {
    return newKmsClientWithCloudKms(keyUri, credentials, cloudKms, KmsTimeouts.NONE);
  }

  protected static KmsClient newKmsClientWithCloudKms(
      Optional<String> keyUri, Optional<GoogleCredentials> credentials, CloudKMS cloudKms,
      KmsTimeouts timeouts)
      throws GeneralSecurityException {
    GcpKmsClient client;
    if (keyUri.isPresent()) {
      client = new GcpKmsClient(keyUri.get());
    } else {
      client = new GcpKmsClient();
    }
    if (cloudKms == null && !timeouts.isEmpty()) {
      // Tink's withCredentials builds its CloudKMS with the transport's default timeouts and no
      // hook to change them, so build an equivalent one here and inject it instead.
      GoogleCredentials creds;
      try {
        creds = credentials.isPresent()
            ? credentials.get()
            : GoogleCredentials.getApplicationDefault();
      } catch (IOException e) {
        throw new GeneralSecurityException("cannot load default credentials", e);
      }
      setCloudKms(client, buildCloudKms(creds, timeouts));
    } else if (cloudKms == null) {
      if (credentials.isPresent()) {
        client.withCredentials(credentials.get());
      } else {
        client.withDefaultCredentials();
      }
    } else {
      setCloudKms(client, cloudKms);
    }
    return client;
  }

  /**
   * Builds the same {@link CloudKMS} that Tink's {@code GcpKmsClient.withCredentials} builds
   * (scoped credentials, trusted transport, Gson, Tink's application name), plus the given
   * timeouts on every request.
   *
   * <p>The operation timeout needs no separate enforcement here: google-http-client only retries
   * when a back-off handler is installed, and Tink installs none (the credentials adapter retries
   * once on a 401 after refreshing the token). One attempt therefore bounds the whole operation
   * at connect + request, which {@link KmsTimeouts#fromConfigs} keeps below the operation timeout
   * for the request part.
   */
  static CloudKMS buildCloudKms(GoogleCredentials credentials, KmsTimeouts timeouts)
      throws GeneralSecurityException {
    GoogleCredentials scoped = credentials.createScopedRequired()
        ? credentials.createScoped(CloudKMSScopes.all())
        : credentials;
    HttpRequestInitializer auth = new HttpCredentialsAdapter(scoped);
    HttpRequestInitializer initializer = request -> {
      auth.initialize(request);
      timeouts.connectTimeout().ifPresent(t -> request.setConnectTimeout(toMillis(t)));
      timeouts.requestTimeout().ifPresent(t -> {
        request.setReadTimeout(toMillis(t));
        request.setWriteTimeout(toMillis(t));
      });
    };
    try {
      return new CloudKMS.Builder(
          GoogleNetHttpTransport.newTrustedTransport(), new GsonFactory(), initializer)
          .setApplicationName(tinkApplicationName())
          .build();
    } catch (IOException e) {
      throw new GeneralSecurityException("cannot build GCP KMS client", e);
    }
  }

  private static int toMillis(Duration d) {
    return (int) Math.min(d.toMillis(), Integer.MAX_VALUE);
  }

  /** Tink's own user agent for CloudKMS, so requests look the same as without timeouts. */
  private static String tinkApplicationName() {
    try {
      Field field = GcpKmsClient.class.getDeclaredField("APPLICATION_NAME");
      field.setAccessible(true);
      return (String) field.get(null);
    } catch (Exception e) {
      return null;
    }
  }

  private static void setCloudKms(GcpKmsClient client, CloudKMS cloudKms) {
    try {
      Field field = GcpKmsClient.class.getDeclaredField("cloudKms");
      field.setAccessible(true);
      field.set(client, cloudKms);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}

