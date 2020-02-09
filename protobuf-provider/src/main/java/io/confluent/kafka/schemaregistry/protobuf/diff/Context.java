/*
 * Copyright 2020 Confluent Inc.
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
 *
 */

package io.confluent.kafka.schemaregistry.protobuf.diff;

import com.squareup.wire.schema.ProtoType;
import com.squareup.wire.schema.internal.parser.FieldElement;
import com.squareup.wire.schema.internal.parser.MessageElement;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

public class Context {
  private final Set<Difference.Type> compatibleChanges;
  private final Set<MessageElement> schemas;
  private final Set<String> originalEnums;
  private final Set<String> updateEnums;
  private final Map<String, ProtoType> originalMaps;
  private final Map<String, ProtoType> updateMaps;
  private final Deque<String> fullPath;
  private final List<Difference> diffs;

  public Context(Set<Difference.Type> compatibleChanges) {
    this.compatibleChanges = compatibleChanges;
    this.schemas = Collections.newSetFromMap(new IdentityHashMap<>());
    this.originalEnums = new HashSet<>();
    this.updateEnums = new HashSet<>();
    this.originalMaps = new HashMap<>();
    this.updateMaps = new HashMap<>();
    this.fullPath = new ArrayDeque<>();
    this.diffs = new ArrayList<>();
  }

  public Context getSubcontext() {
    Context ctx = new Context(this.compatibleChanges);
    ctx.schemas.addAll(this.schemas);
    ctx.fullPath.addAll(this.fullPath);
    return ctx;
  }

  public SchemaScope enterSchema(final MessageElement schema) {
    return !schemas.contains(schema) ? new SchemaScope(schema) : null;
  }

  public class SchemaScope implements AutoCloseable {
    private final MessageElement schema;

    public SchemaScope(final MessageElement schema) {
      this.schema = schema;
      schemas.add(schema);
    }

    public void close() {
      schemas.remove(schema);
    }
  }

  public void addEnum(final String name, final boolean isOriginal) {
    if (isOriginal) {
      originalEnums.add(name);
    } else {
      updateEnums.add(name);
    }
  }

  public boolean containsEnum(final String name, final boolean isOriginal) {
    return isOriginal ? originalEnums.contains(name) : updateEnums.contains(name);
  }

  public void addMap(final String name,
                     final FieldElement key,
                     final FieldElement value,
                     final boolean isOriginal) {
    ProtoType type = ProtoType.get("map<" + key.getType() + ", " + value.getType() + ">");
    if (isOriginal) {
      originalMaps.put(name, type);
    } else {
      updateMaps.put(name, type);
    }
  }

  public Optional<ProtoType> getMap(final String name, final boolean isOriginal) {
    return Optional.ofNullable(isOriginal ? originalMaps.get(name) : updateMaps.get(name));
  }

  public PathScope enterPath(final String path) {
    return new PathScope(path);
  }

  public class PathScope implements AutoCloseable {
    public PathScope(final String path) {
      fullPath.addLast(path);
    }

    public void close() {
      fullPath.removeLast();
    }
  }

  public boolean isCompatible() {
    boolean notCompatible = getDifferences().stream()
        .map(Difference::getType)
        .anyMatch(t -> !compatibleChanges.contains(t));
    return !notCompatible;
  }

  public List<Difference> getDifferences() {
    return diffs;
  }

  public void addDifference(final Difference.Type type) {
    diffs.add(new Difference(type, fullPathString(fullPath)));
  }

  public void addDifference(final String attribute, final Difference.Type type) {
    fullPath.addLast(attribute);
    addDifference(type);
    fullPath.removeLast();
  }

  public void addDifferences(final List<Difference> differences) {
    diffs.addAll(differences);
  }

  private static String fullPathString(final Deque<String> fullPath) {
    return "#/" + String.join("/", fullPath);
  }
}
