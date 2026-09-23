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
import java.util.Iterator;
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

  private static final String ELEMENT = "[]";
  private static final String VALUE = "{value}";

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
      if (names != null && mapping.writerPathOf(path) == null) {
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
    if (ELEMENT.equals(name) || VALUE.equals(name)) {
      if (step + 1 < path.size()) {
        Iterator<JsonNode> children = node.elements();
        while (children.hasNext()) {
          prune(children.next(), path, step + 1);
        }
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
