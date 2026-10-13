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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Every version's members with an id allocated per location — the data a provenance endpoint
 * serves, independent of how it is serialised.
 *
 * <p>Ids are allocated by walking versions in the order supplied and, within a version, members in
 * path order, taking the next integer for each member matched to nothing in the previous version
 * (see {@link ProvenanceComputer}). That order is part of the contract: it determines the numbers,
 * so two derivations over the same history agree.
 *
 * <p>Versions are identified by their index in the supplied history, not by a registry version
 * number. Mapping one to the other belongs to whoever assembled the history, and keeping it out of
 * here is what makes the ids a function of the history alone.
 *
 * <p>Internal to Schema Registry: not a supported API, and it may change in any release.
 */
public final class ProvenanceReport {

  private final List<Version> versions;
  private final int lastId;

  ProvenanceReport(List<Version> versions, int lastId) {
    this.versions = Collections.unmodifiableList(versions);
    this.lastId = lastId;
  }

  /**
   * The reported versions, in the order supplied, each with its index in the supplied history.
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
    private final String kind;
    private final List<Member> members;

    Version(int index, String kind, List<Member> members) {
      this.index = index;
      this.kind = kind;
      this.members = Collections.unmodifiableList(members);
    }

    /**
     * This version's position in the supplied history.
     */
    public int getIndex() {
      return index;
    }

    /**
     * The root's kind, spelled as {@link Member#getKind()} is; null for a version with no root.
     */
    public String getKind() {
      return kind;
    }

    /**
     * The members, in path order — lexicographic on the index sequences, which is the pre-order
     * walk and the order ids were allocated in.
     */
    public List<Member> getMembers() {
      return members;
    }

    /**
     * Each member's check id by its path: what {@code FieldPairing.byIds} pairs by.
     */
    public Map<List<Integer>, Integer> checkIds() {
      Map<List<Integer>, Integer> ids = new HashMap<>();
      for (Member member : members) {
        ids.put(member.getPath(), member.getCheckId());
      }
      return ids;
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
    private final String kind;
    private final int id;
    private final int checkId;

    Member(List<Integer> path, List<String> names, String kind, int id, int checkId) {
      this.path = path;
      this.names = names;
      this.kind = kind;
      this.id = id;
      this.checkId = checkId;
    }

    /**
     * The member's index path, every named type inlined: the Metastore's key.
     */
    public List<Integer> getPath() {
      return path;
    }

    /**
     * The native schema's name for each step of {@link #getPath()}, with its entry steps, as the
     * converter recorded them; null for a step with no native name, or null altogether where an
     * edge recorded none.
     */
    public List<String> getNames() {
      return names;
    }

    /**
     * What the member's type is, references resolved: {@code SCALAR}, {@code STRUCT},
     * {@code UNION}, or {@code ARRAY<k>}, {@code MULTISET<k>} or {@code MAP<k, k>} of the kinds
     * they hold. A member whose kind changes is new, with everything under it.
     */
    public String getKind() {
      return kind;
    }

    /**
     * The provenance id: unique per <em>location</em>, stable along a chain of matches.
     * A rename keeps it, a drop retires it permanently, and a column re-added under an old name
     * takes a fresh one.
     */
    public int getId() {
      return id;
    }

    /**
     * The id the logical compatibility checks pair by: {@link #getId()} but for a kind change,
     * where a location and everything under it keep the check id of what they would have
     * continued. Allocated as ids are, from its own sequence; not served, and not stable across
     * releases.
     */
    public int getCheckId() {
      return checkId;
    }

    @Override
    public String toString() {
      return path + "=" + id;
    }
  }
}
