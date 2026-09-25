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

package io.confluent.kafka.schemaregistry.type.logical.provenance;

import java.util.Objects;

/**
 * An entity's {@link Identity} together with the first version of the chain of matches it belongs
 * to — the answer to "which lifetime of that entity is this?".
 *
 * <p>Each version is matched against the one before it alone, and an entity keeps its provenance
 * for as long as each version matches it to the previous one. An entity absent from a version
 * starts a new chain when it returns, and so takes a new provenance, even when its identity is the
 * same, as a Protobuf field number re-added is.
 *
 * <p>This, not {@link Identity}, is the cross-schema correspondence key. Two occurrences correspond
 * under projection exactly when their provenance values are equal. Since nothing before a version's
 * predecessor is consulted, a sub-history pairs its versions exactly as the whole history does;
 * only {@link #getPresenceStartVersion()} is relative to the first version supplied.
 */
public final class Provenance {

  private final Identity identity;
  private final int presenceStartVersion;

  Provenance(Identity identity, int presenceStartVersion) {
    this.identity = Objects.requireNonNull(identity, "identity");
    this.presenceStartVersion = presenceStartVersion;
  }

  /** What logical entity this is. */
  public Identity getIdentity() {
    return identity;
  }

  /**
   * The index, within the supplied sequence, of the version that began this chain. Never later
   * than the version this provenance was read from.
   */
  public int getPresenceStartVersion() {
    return presenceStartVersion;
  }

  /**
   * What kind of entity this is; only {@link EntityKind#isMember()} kinds carry data.
   */
  public EntityKind getKind() {
    return identity.getKind();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof Provenance)) {
      return false;
    }
    Provenance that = (Provenance) o;
    return presenceStartVersion == that.presenceStartVersion && identity.equals(that.identity);
  }

  @Override
  public int hashCode() {
    return identity.hashCode() * 31 + presenceStartVersion;
  }

  @Override
  public String toString() {
    return identity + "~v" + presenceStartVersion;
  }
}
