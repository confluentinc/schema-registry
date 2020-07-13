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

import com.squareup.wire.schema.internal.parser.TypeElement;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaReference;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class Context {
  private final Set<Difference.Type> compatibleChanges;
  private final Set<MessageElement> schemas;
  private final Map<String, TypeElementInfo> originalTypes;
  private final Map<String, TypeElementInfo> updateTypes;
  private String originalPackageName;
  private String updatePackageName;
  private final Deque<String> fullPath;
  private final Deque<String> fullName;  // subset of path used for fully-qualified names
  private final List<Difference> diffs;

  public Context(Set<Difference.Type> compatibleChanges) {
    this.compatibleChanges = compatibleChanges;
    this.schemas = Collections.newSetFromMap(new IdentityHashMap<>());
    this.originalTypes = new HashMap<>();
    this.updateTypes = new HashMap<>();
    this.fullPath = new ArrayDeque<>();
    this.fullName = new ArrayDeque<>();
    this.diffs = new ArrayList<>();
  }

  public Context getSubcontext() {
    Context ctx = new Context(this.compatibleChanges);
    ctx.schemas.addAll(this.schemas);
    ctx.originalTypes.putAll(this.originalTypes);
    ctx.updateTypes.putAll(this.updateTypes);
    ctx.originalPackageName = this.originalPackageName;
    ctx.updatePackageName = this.updatePackageName;
    ctx.fullPath.addAll(this.fullPath);
    ctx.fullName.addAll(this.fullName);
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

  public void addType(final String name, final String packageName, final SchemaReference ref,
      final TypeElement type, final boolean isMap, final FieldElement key, final FieldElement value,
      final boolean isOriginal) {
    if (isOriginal) {
      originalTypes.put(name, new TypeElementInfo(packageName, ref, type, isMap, key, value));
    } else {
      updateTypes.put(name, new TypeElementInfo(packageName, ref, type, isMap, key, value));
    }
  }

  public TypeElementInfo getType(final String name, final boolean isOriginal) {
    String fullName = resolve(name, isOriginal);
    return getTypeForFullName(fullName, isOriginal);
  }

  public void setPackageName(final String packageName, final boolean isOriginal) {
    if (isOriginal) {
      originalPackageName = packageName;
    } else {
      updatePackageName = packageName;
    }
  }

  public void setFullName(final String name) {
    List<String> parts = Arrays.asList(name.split("\\."));
    fullPath.clear();
    fullName.clear();
    fullPath.addAll(parts);
    fullName.addAll(parts);
  }

  public PathScope enterPath(final String path) {
    return new PathScope(path);
  }

  public NamedScope enterName(final String name) {
    return new NamedScope(name);
  }

  public String resolve(String name, boolean isOriginal) {
    if (name.startsWith(".")) {
      String n = name.substring(1);
      TypeElementInfo type = getTypeForFullName(n, isOriginal);
      if (type != null) {
        return n;
      }
    } else {
      Deque<String> prefix = new ArrayDeque<>(fullName);
      if (isOriginal) {
        if (!originalPackageName.isEmpty()) {
          prefix.addFirst(originalPackageName);
        }
      } else {
        if (!updatePackageName.isEmpty()) {
          prefix.addFirst(updatePackageName);
        }
      }
      while (!prefix.isEmpty()) {
        String n = String.join(".", prefix) + "." + name;
        TypeElementInfo type = getTypeForFullName(n, isOriginal);
        if (type != null) {
          return n;
        }
        prefix.removeLast();
      }
      TypeElementInfo type = getTypeForFullName(name, isOriginal);
      if (type != null) {
        return name;
      }
    }
    return null;
  }

  private TypeElementInfo getTypeForFullName(final String fullName, final boolean isOriginal) {
    if (isOriginal) {
      return originalTypes.get(fullName);
    } else {
      return updateTypes.get(fullName);
    }
  }

  public class PathScope implements AutoCloseable {
    public PathScope(final String path) {
      fullPath.addLast(path);
    }

    public void close() {
      fullPath.removeLast();
    }
  }

  public class NamedScope extends PathScope {
    public NamedScope(final String name) {
      super(name);
      fullName.addLast(name);
    }

    public void close() {
      fullName.removeLast();
      super.close();
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

  static class TypeElementInfo {
    private final String packageName;
    private final SchemaReference ref;
    private final TypeElement type;
    private final boolean isMap;
    private final FieldElement key;
    private final FieldElement value;

    public TypeElementInfo(String packageName, SchemaReference ref, TypeElement type,
        boolean isMap, FieldElement key, FieldElement value) {
      this.packageName = packageName;
      this.ref = ref;
      this.type = type;
      this.isMap = isMap;
      this.key = key;
      this.value = value;
    }

    public String packageName() {
      return packageName;
    }

    public SchemaReference reference() {
      return ref;
    }

    public TypeElement type() {
      return type;
    }

    public boolean isMap() {
      return isMap;
    }

    public FieldElement key() {
      return key;
    }

    public FieldElement value() {
      return value;
    }

    public ProtoType getMapType() {
      return isMap ? ProtoType.get("map<" + key.getType() + ", " + value.getType() + ">") : null;
    }
  }
}
