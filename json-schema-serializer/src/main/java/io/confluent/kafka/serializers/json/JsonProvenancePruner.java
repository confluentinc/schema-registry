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

package io.confluent.kafka.serializers.json;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.confluent.kafka.serializers.provenance.ProvenanceMapping;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * Carries provenance into a JSON document by pruning it.
 *
 * <p>JSON Schema identifies a property by its name alone, so provenance cannot turn a rename into
 * a match. What it can tell is a property dropped and later re-added under its old name: the
 * reader's property is then new, and reading by name would hand it data written for the old one.
 * Every such property is removed from the document, so a reader converting by name finds nothing
 * there, as it would for any property the writer never had.
 */
final class JsonProvenancePruner {

  private JsonProvenancePruner() {
  }

  /**
   * The name paths of every reader property provenance gives no writer counterpart, outermost
   * first, leaving out any that sits inside one already listed.
   */
  static List<List<String>> removals(ProvenanceMapping mapping) {
    List<List<String>> paths = new ArrayList<>();
    for (List<Integer> path : mapping.readerPaths()) {
      List<String> names = mapping.readerNamesOf(path);
      if (names != null && isProperty(mapping, path, names) && mapping.writerPathOf(path) == null) {
        paths.add(names);
      }
    }
    paths.sort(Comparator.comparingInt(List::size));
    List<List<String>> outermost = new ArrayList<>();
    for (List<String> path : paths) {
      if (outermost.stream().noneMatch(o -> path.subList(0, Math.min(o.size(), path.size()))
          .equals(o))) {
        outermost.add(path);
      }
    }
    return outermost;
  }

  /**
   * Whether the location at {@code path} is a property of its own. A union branch has no step in
   * the document, so it spells the same names as the location holding it and is never removed.
   */
  private static boolean isProperty(ProvenanceMapping mapping, List<Integer> path,
      List<String> names) {
    if (names.isEmpty() || names.get(names.size() - 1) == null) {
      return false;
    }
    for (int k = 1; k < path.size(); k++) {
      if (names.equals(mapping.readerNamesOf(path.subList(0, k)))) {
        return false;
      }
    }
    return true;
  }

  /**
   * Removes every path in {@code removals} from {@code document}, in place.
   */
  static void prune(JsonNode document, List<List<String>> removals) {
    for (List<String> path : removals) {
      prune(document, path, 0);
    }
  }

  private static void prune(JsonNode node, List<String> path, int step) {
    if (node == null) {
      return;
    }
    String name = path.get(step);
    if (name == null) {
      // An unnamed step: each element of an array, or each value of an object keyed by string.
      if (step + 1 < path.size()) {
        node.elements().forEachRemaining(child -> prune(child, path, step + 1));
      }
      return;
    }
    if (!node.isObject()) {
      return;
    }
    if (step + 1 == path.size()) {
      ((ObjectNode) node).remove(name);
    } else {
      prune(node.get(name), path, step + 1);
    }
  }
}
