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

import io.confluent.kafka.schemaregistry.type.logical.LogicalType;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * A report's pids, per version, by inlined path.
 */
final class Pids {

  private final List<Map<List<Integer>, Integer>> byVersion = new ArrayList<>();

  private Pids(ProvenanceReport report) {
    for (ProvenanceReport.Version version : report.getVersions()) {
      Map<List<Integer>, Integer> pids = new LinkedHashMap<>();
      for (ProvenanceReport.Member member : version.getMembers()) {
        pids.put(member.getPath(), member.getId());
      }
      byVersion.add(Collections.unmodifiableMap(pids));
    }
  }

  static Pids of(ProvenanceReport report) {
    return new Pids(report);
  }

  static Pids of(List<LogicalType> versions, IdentityPolicy policy) {
    return new Pids(ProvenanceComputer.report(versions, policy));
  }

  /**
   * The pid at {@code path} in {@code version}, or null where the version has no member there.
   */
  Integer at(int version, Integer... path) {
    return byVersion.get(version).get(Arrays.asList(path));
  }

  /**
   * Every path of {@code version} and its pid, in path order.
   */
  Map<List<Integer>, Integer> version(int version) {
    return byVersion.get(version);
  }

  /**
   * The member paths of {@code version} whose pid {@code other} also has.
   */
  List<List<Integer>> shared(int version, int other) {
    List<List<Integer>> shared = new ArrayList<>();
    byVersion.get(version).forEach((path, pid) -> {
      if (byVersion.get(other).containsValue(pid)) {
        shared.add(path);
      }
    });
    return shared;
  }

  /**
   * Whether {@code version}'s member at {@code path} is new: no earlier version has its pid.
   */
  boolean isNew(int version, Integer... path) {
    Integer pid = at(version, path);
    for (int earlier = 0; earlier < version; earlier++) {
      if (byVersion.get(earlier).containsValue(pid)) {
        return false;
      }
    }
    return pid != null;
  }

  /**
   * The first version of the unbroken run of versions holding the pid at {@code path}.
   */
  int chainStart(int version, Integer... path) {
    Integer pid = at(version, path);
    int start = version;
    while (start > 0 && byVersion.get(start - 1).containsValue(pid)) {
      start--;
    }
    return start;
  }

  @Override
  public String toString() {
    return byVersion.toString();
  }
}
