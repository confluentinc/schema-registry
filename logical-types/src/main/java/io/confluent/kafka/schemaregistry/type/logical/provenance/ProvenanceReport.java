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

import java.util.Collections;
import java.util.List;

/**
 * Every version's members with an id allocated per location — the data a provenance endpoint
 * serves, independent of how it is serialised.
 *
 * <p>Ids are allocated by walking versions in the order supplied and, within a version, members in
 * path order, taking the next integer the first time a {@link LocatedProvenance} is seen. That
 * order is part of the contract: it determines the numbers, so two derivations over the same
 * history agree.
 *
 * <p>Versions are identified by their index in the supplied history, not by a registry version
 * number. Mapping one to the other belongs to whoever assembled the history, and keeping it out of
 * here is what makes the ids a function of the history alone.
 */
public final class ProvenanceReport {

  private final List<Version> versions;
  private final int lastId;

  ProvenanceReport(List<Version> versions, int lastId) {
    this.versions = Collections.unmodifiableList(versions);
    this.lastId = lastId;
  }

  /**
   * One entry per supplied version, in order.
   */
  public List<Version> getVersions() {
    return versions;
  }

  /**
   * The highest id allocated, or {@code 0} if none were. The next would be one greater — which is
   * not derivable from the last version alone, since a dropped column's id is retired rather than
   * reused.
   */
  public int getLastId() {
    return lastId;
  }

  /**
   * One version's members, in path order.
   */
  public static final class Version {

    private final int index;
    private final List<Member> members;

    Version(int index, List<Member> members) {
      this.index = index;
      this.members = Collections.unmodifiableList(members);
    }

    /**
     * This version's position in the supplied history.
     */
    public int getIndex() {
      return index;
    }

    /**
     * The members, in path order — lexicographic on the index sequences, which is the pre-order
     * walk and the order ids were allocated in.
     */
    public List<Member> getMembers() {
      return members;
    }

    @Override
    public String toString() {
      return "v" + index + " " + members;
    }
  }

  /**
   * One member: where it is, what it is called, and which logical column it is.
   */
  public static final class Member {

    private final List<Integer> path;
    private final List<String> names;
    private final int id;
    private final Object defaultValue;

    Member(List<Integer> path, List<String> names, int id, Object defaultValue) {
      this.path = path;
      this.names = names;
      this.id = id;
      this.defaultValue = defaultValue;
    }

    public List<Integer> getPath() {
      return path;
    }

    /**
     * Informational; see {@link InlinedMember#getNames()}.
     */
    public List<String> getNames() {
      return names;
    }

    /**
     * The provenance id: unique per <em>location</em>, stable for the life of a presence interval.
     * A rename keeps it, a drop retires it permanently, and a column re-added under an old name
     * takes a fresh one.
     */
    /**
     * What reading this member yields when a writer has no counterpart for it, or {@code null}
     * where there is nothing to read. See {@link InlinedMember#getDefaultValue()}.
     */
    public Object getDefaultValue() {
      return defaultValue;
    }

    public int getId() {
      return id;
    }

    @Override
    public String toString() {
      return path + "=" + id;
    }
  }
}
