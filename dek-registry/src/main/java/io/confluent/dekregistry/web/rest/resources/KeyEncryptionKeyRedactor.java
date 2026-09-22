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

package io.confluent.dekregistry.web.rest.resources;

import io.confluent.dekregistry.client.rest.entities.KmsPropsRedactor;
import io.confluent.dekregistry.storage.KeyEncryptionKey;

/**
 * Server-only extension of {@link KmsPropsRedactor} for the internal {@link KeyEncryptionKey}
 * storage entity, which the client library has no visibility into. The shared
 * {@code Map}/{@code Kek}-based redact/merge logic lives in the client library so
 * {@code CachedDekRegistryClient} can apply the same semantics to its own cache.
 */
public final class KeyEncryptionKeyRedactor {

  private KeyEncryptionKeyRedactor() {
  }

  public static KeyEncryptionKey redact(KeyEncryptionKey kek) {
    if (kek == null) {
      return null;
    }
    KeyEncryptionKey redacted = new KeyEncryptionKey(kek.getName(), kek.getKmsType(),
        kek.getKmsKeyId(), KmsPropsRedactor.redact(kek.getKmsProps()),
        kek.getDoc(), kek.isShared(), kek.isDeleted());
    redacted.setOffset(kek.getOffset());
    redacted.setTimestamp(kek.getTimestamp());
    return redacted;
  }
}
