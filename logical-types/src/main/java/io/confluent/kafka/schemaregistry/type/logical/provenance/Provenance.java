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
 * An entity's {@link Identity} together with the first version of its current continuous presence
 * interval — the answer to "which lifetime of that entity is this?".
 *
 * <p>An entity keeps its provenance for as long as it stays continuously present. If it disappears
 * and later reappears, the reappearance starts a new interval and so takes a new provenance, even
 * when an alias reconnects it to the same logical identity. Provenance is therefore not the version
 * an entity first ever existed in; it is the first version of its current uninterrupted lifespan.
 *
 * <p>This, not {@link Identity}, is the cross-schema correspondence key. Two occurrences correspond
 * under projection exactly when their provenance values are equal, which happens exactly when they
 * belong to the same continuous presence interval of the same logical entity.
 *
 * <p><b>Values are only as global as the history they were computed from.</b> Computed from a
 * sub-history they are relative — sufficient to determine correspondence within that window and
 * nothing more. Only a run anchored at the first version yields absolute values.
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
   * The index, within the supplied sequence, of the version that began this presence interval.
   * Never later than the version this provenance was read from.
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
