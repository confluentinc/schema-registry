/*
 * Copyright 2023 Confluent Inc.
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

package io.confluent.kafka.schemaregistry.encryption.tink;

import com.google.crypto.tink.KeyManager;
import com.google.crypto.tink.KmsClient;
import java.security.GeneralSecurityException;
import java.util.Iterator;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * A container for {@link KmsClient}-objects that are needed by {@link KeyManager}-objects for
 * primitives that use KMS-managed keys.
 *
 * <p>This class consists exclusively of static methods that register and load {@link
 * KmsClient}-objects.
 */
public final class KmsClients {

  private static final CopyOnWriteArrayList<KmsClient> clients = new CopyOnWriteArrayList<>();

  /**
   * Adds a client to the list of known {@link KmsClient}-objects.
   */
  public static void add(KmsClient client) {
    clients.add(client);
  }

  /**
   * Returns the first {@link KmsClient} registered with {@link KmsClients#add} that supports
   * {@code keyUri}.
   *
   * @throws GeneralSecurityException if cannot find any KMS clients that support {@code keyUri}
   */
  public static KmsClient get(String keyUri) throws GeneralSecurityException {
    for (KmsClient client : clients) {
      if (client.doesSupport(keyUri)) {
        return client;
      }
    }
    throw new GeneralSecurityException("No KMS client does support: " + keyUri);
  }

  /**
   * Removes the {@link KmsClient} instances that support {@code keyUri}.
   */
  public static void remove(String keyUri) {
    clients.removeIf(client -> client.doesSupport(keyUri));
  }

  private KmsClients() {
  }
}
