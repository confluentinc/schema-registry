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

import com.squareup.wire.schema.Field;
import com.squareup.wire.schema.ProtoType;
import com.squareup.wire.schema.internal.parser.ExtendElement;
import com.squareup.wire.schema.internal.parser.FieldElement;
import com.squareup.wire.schema.internal.parser.MessageElement;

import com.squareup.wire.schema.internal.parser.ProtoFileElement;
import com.squareup.wire.schema.internal.parser.TypeElement;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaReference;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

public class Context {
  private final Set<Difference.Type> compatibleChanges;
  private final Set<MessageElement> schemas;
  private final Map<String, TypeElementInfo> originalTypes;
  private final Map<String, TypeElementInfo> updateTypes;
  private final Map<String, ExtendFieldElementInfo> originalExtendFields;
  private final Map<String, ExtendFieldElementInfo> updateExtendFields;
  private String originalPackageName;
  private String updatePackageName;
  private final Deque<String> fullPath;
  private final Deque<String> fullName;  // subset of path used for fully-qualified names
  private final List<Difference> diffs;

  public Context() {
    this(Collections.emptySet());
  }

  public Context(Set<Difference.Type> compatibleChanges) {
    this.compatibleChanges = compatibleChanges;
    this.schemas = Collections.newSetFromMap(new IdentityHashMap<>());
    this.originalTypes = new HashMap<>();
    this.updateTypes = new HashMap<>();
    this.originalExtendFields = new HashMap<>();
    this.updateExtendFields = new HashMap<>();
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

  public void collectTypeInfo(ProtobufSchema schema, boolean isOriginal) {
    Map<String, SchemaReference> references = schema.references().stream()
        .collect(Collectors.toMap(
            SchemaReference::getName,
            r -> r,
            (existing, replacement) -> replacement));
    Map<String, ProtoFileElement> dependencies = schema.dependenciesWithLogicalTypes();
    for (Map.Entry<String, ProtoFileElement> entry : dependencies.entrySet()) {
      String refName = entry.getKey();
      ProtoFileElement protoFile = entry.getValue();
      SchemaReference ref = references.get(refName);
      collectTypeInfo(ref, protoFile, isOriginal);
    }
    SchemaReference dummyRef = new SchemaReference("", "", -1);
    collectTypeInfo(dummyRef, schema.rawSchema(), isOriginal);
  }

  private void collectTypeInfo(
      final SchemaReference ref,
      final ProtoFileElement protoFile,
      boolean isOriginal
  ) {
    setPackageName(protoFile.getPackageName(), isOriginal);
    collectExtendInfo(ref, protoFile.getExtendDeclarations(), isOriginal);
    collectTypeInfo(ref, protoFile.getTypes(), isOriginal);
  }

  private void collectTypeInfo(
      final SchemaReference ref,
      final List<TypeElement> types,
      boolean isOriginal
  ) {
    for (TypeElement typeElement : types) {
      try (Context.NamedScope namedScope = enterName(typeElement.getName())) {
        boolean isMap = false;
        Optional<FieldElement> key = Optional.empty();
        Optional<FieldElement> value = Optional.empty();
        if (typeElement instanceof MessageElement) {
          MessageElement messageElement = (MessageElement) typeElement;
          collectExtendInfo(ref, messageElement.getExtendDeclarations(), isOriginal);
          isMap = ProtobufSchema.findOption("map_entry", messageElement.getOptions())
              .map(o -> Boolean.valueOf(o.getValue().toString())).orElse(false);
          if (isMap) {
            key = findField(ProtobufSchema.KEY_FIELD,
                messageElement.getFields());
            value = findField(ProtobufSchema.VALUE_FIELD,
                messageElement.getFields());
          }
        }
        addType(ref, typeElement, isMap, key.orElse(null), value.orElse(null), isOriginal);
        collectTypeInfo(ref, typeElement.getNestedTypes(), isOriginal);
      }
    }
  }

  private void addType(final SchemaReference ref, final TypeElement type, final boolean isMap,
      final FieldElement key, final FieldElement value, final boolean isOriginal) {
    String name = String.join(".", fullPath);
    String packageName = isOriginal ? originalPackageName : updatePackageName;
    if (!packageName.isEmpty()) {
      name = packageName + "." + name;
    }
    TypeElementInfo typeInfo = new TypeElementInfo(packageName, ref, type, isMap, key, value);
    if (isOriginal) {
      originalTypes.put(name, typeInfo);
    } else {
      updateTypes.put(name, typeInfo);
    }
  }

  private void collectExtendInfo(
      final SchemaReference ref,
      final List<ExtendElement> extendElements,
      boolean isOriginal
  ) {
    for (ExtendElement extendElement : extendElements) {
      for (FieldElement fieldElement : extendElement.getFields()) {
        try (Context.NamedScope namedScope = enterName(fieldElement.getName())) {
          addExtendField(ref, fieldElement, isOriginal);
        }
      }
    }
  }

  private void addExtendField(
      final SchemaReference ref, final FieldElement field, final boolean isOriginal) {
    String name = String.join(".", fullPath);
    String packageName = isOriginal ? originalPackageName : updatePackageName;
    if (!packageName.isEmpty()) {
      name = packageName + "." + name;
    }
    ExtendFieldElementInfo fieldInfo = new ExtendFieldElementInfo(packageName, ref, field);
    if (isOriginal) {
      originalExtendFields.put(name, fieldInfo);
    } else {
      updateExtendFields.put(name, fieldInfo);
    }
  }

  private static Optional<FieldElement> findField(String name, List<FieldElement> options) {
    return options.stream().filter(o -> o.getName().equals(name)).findFirst();
  }

  public TypeElementInfo getType(final String name, final boolean isOriginal) {
    String fullName = resolve(this::getTypeForFullName, name, isOriginal);
    return fullName != null ? getTypeForFullName(fullName, isOriginal) : null;
  }

  public ExtendFieldElementInfo getExtendField(final String name, final boolean isOriginal) {
    String fullName = resolve(this::getExtendFieldForFullName, name, isOriginal);
    return fullName != null ? getExtendFieldForFullName(fullName, isOriginal) : null;
  }

  public void setPackageName(final String packageName, final boolean isOriginal) {
    String name = packageName;
    if (name == null) {
      name = "";
    }
    if (isOriginal) {
      originalPackageName = name;
    } else {
      updatePackageName = name;
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

  public SchemaScope enterSchema(final MessageElement schema) {
    return !schemas.contains(schema) ? new SchemaScope(schema) : null;
  }

  public String resolve(String name, boolean isOriginal) {
    return resolve(this::getTypeForFullName, name, isOriginal);
  }

  public <T> String resolve(Resolver<T> resolver, String name, boolean isOriginal) {
    if (name.startsWith(".")) {
      String n = name.substring(1);
      T elem = resolver.resolve(n, isOriginal);
      if (elem != null) {
        return n;
      }
    } else {
      Deque<String> prefix = new ArrayDeque<>();
      if (isOriginal) {
        if (!originalPackageName.isEmpty()) {
          String[] parts = originalPackageName.split("\\.");
          prefix.addAll(Arrays.asList(parts));
        }
      } else {
        if (!updatePackageName.isEmpty()) {
          String[] parts = updatePackageName.split("\\.");
          prefix.addAll(Arrays.asList(parts));
        }
      }
      prefix.addAll(fullName);
      while (!prefix.isEmpty()) {
        String n = String.join(".", prefix) + "." + name;
        T elem = resolver.resolve(n, isOriginal);
        if (elem != null) {
          return n;
        }
        prefix.removeLast();
      }
      T elem = resolver.resolve(name, isOriginal);
      if (elem != null) {
        return name;
      }
    }
    return null;
  }

  public TypeElementInfo getTypeForFullName(final String fullName, final boolean isOriginal) {
    String name = fullName;
    if (name.startsWith(".")) {
      name = name.substring(1);
    }
    if (isOriginal) {
      return originalTypes.get(name);
    } else {
      return updateTypes.get(name);
    }
  }

  public ExtendFieldElementInfo getExtendFieldForFullName(
      final String fullName, final boolean isOriginal) {
    String name = fullName;
    if (name.startsWith(".")) {
      name = name.substring(1);
    }
    if (isOriginal) {
      return originalExtendFields.get(name);
    } else {
      return updateExtendFields.get(name);
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

  public class PathScope implements AutoCloseable {
    public PathScope(final String path) {
      fullPath.addLast(path);
    }

    @Override
    public void close() {
      fullPath.removeLast();
    }
  }

  public class NamedScope extends PathScope {
    public NamedScope(final String name) {
      super(name);
      fullName.addLast(name);
    }

    @Override
    public void close() {
      fullName.removeLast();
      super.close();
    }
  }

  public class SchemaScope implements AutoCloseable {
    private final MessageElement schema;

    public SchemaScope(final MessageElement schema) {
      this.schema = schema;
      schemas.add(schema);
    }

    @Override
    public void close() {
      schemas.remove(schema);
    }
  }

  public interface Resolver<T> {
    T resolve(String name, boolean isOriginal);
  }

  public static class TypeElementInfo {
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

  public static class ExtendFieldElementInfo {
    private final String packageName;
    private final SchemaReference ref;
    private final FieldElement field;
    private final boolean repeated;

    public ExtendFieldElementInfo(String packageName, SchemaReference ref, FieldElement field) {
      this.packageName = packageName;
      this.ref = ref;
      this.field = field;
      this.repeated = Field.Label.REPEATED == field.getLabel();
    }

    public String packageName() {
      return packageName;
    }

    public SchemaReference reference() {
      return ref;
    }

    public FieldElement field() {
      return field;
    }

    public boolean isRepeated() {
      return repeated;
    }
  }
}
