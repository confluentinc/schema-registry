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

import com.google.api.services.cloudkms.v1.CloudKMS;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.crypto.tink.KmsClient;
import com.google.crypto.tink.KmsClients;
import io.confluent.kafka.schemaregistry.encryption.FieldEncryptionExecutor;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.util.Map;
import java.util.Optional;

public class GcpFieldEncryptionExecutor extends FieldEncryptionExecutor {

  public static final String ACCOUNT_TYPE = "account.type";
  public static final String CLIENT_ID = "client.id";
  public static final String CLIENT_EMAIL = "client.email";
  public static final String PRIVATE_KEY_ID = "private.key.id";
  public static final String PRIVATE_KEY = "private.key";

  private GoogleCredentials credentials;

  public GcpFieldEncryptionExecutor() {
  }

  @Override
  public void configure(Map<String, ?> configs) {
    try {
      super.configure(configs);
      String keyId = (String) configs.get(DEFAULT_KMS_KEY_ID);
      // Key id is not mandatory for decryption
      String keyUri = keyId != null ? GcpKmsClient.PREFIX + keyId : null;
      setDefaultKekId(keyUri);
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
        credentials = GoogleCredentials.fromStream(
            new ByteArrayInputStream(keys.getBytes(StandardCharsets.UTF_8)));
      } else {
        credentials = GoogleCredentials.getApplicationDefault();
      }
      // register client w/o keyUri so it can be overridden
      registerKmsClient(Optional.empty());
    } catch (GeneralSecurityException | IOException e) {
      throw new IllegalArgumentException(e);
    }
  }

  @Override
  public KmsClient registerKmsClient(Optional<String> kekId) throws GeneralSecurityException {
    return registerWithCloudKms(kekId, Optional.ofNullable(credentials),
        (CloudKMS) getTestClient());
  }

  public static KmsClient registerWithCloudKms(
      Optional<String> keyUri, Optional<GoogleCredentials> credentials, CloudKMS cloudKms)
      throws GeneralSecurityException {
    GcpKmsClient client;
    if (keyUri.isPresent()) {
      client = new GcpKmsClient(keyUri.get());
    } else {
      client = new GcpKmsClient();
    }
    if (credentials.isPresent()) {
      client.withCredentials(credentials.get());
    } else {
      client.withDefaultCredentials();
    }
    if (cloudKms != null) {
      setCloudKms(client, cloudKms);
    }
    KmsClients.add(client);
    return client;
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

