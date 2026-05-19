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

package io.confluent.kafka.schemaregistry.type.logical.protobuf;

import com.google.common.base.CaseFormat;
import io.confluent.kafka.schemaregistry.client.rest.entities.Metadata;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import io.confluent.kafka.schemaregistry.type.logical.LogicalType;
import io.confluent.kafka.schemaregistry.type.logical.Schema;
import io.confluent.kafka.schemaregistry.type.logical.Schema.EnumValue;
import io.confluent.kafka.schemaregistry.type.logical.Schema.Field;
import io.confluent.kafka.schemaregistry.type.logical.Schema.UnionBranch;
import io.confluent.kafka.schemaregistry.type.logical.ValidationException;
import io.confluent.kafka.schemaregistry.type.logical.common.FromLogicalContext;
import io.confluent.kafka.schemaregistry.type.logical.common.LogicalTypeVersion;
import com.google.protobuf.DescriptorProtos.DescriptorProto;
import com.google.protobuf.DescriptorProtos.EnumDescriptorProto;
import com.google.protobuf.DescriptorProtos.EnumValueDescriptorProto;
import com.google.protobuf.DescriptorProtos.EnumValueOptions;
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto;
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto.Label;
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto.Type;
import com.google.protobuf.DescriptorProtos.EnumOptions;
import com.google.protobuf.DescriptorProtos.FieldOptions;
import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import com.google.protobuf.DescriptorProtos.MessageOptions;
import com.google.protobuf.DescriptorProtos.OneofDescriptorProto;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.DescriptorValidationException;
import com.google.protobuf.Descriptors.EnumDescriptor;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.Descriptors.GenericDescriptor;
import com.google.protobuf.Timestamp;
import com.google.type.Date;
import com.google.type.TimeOfDay;
import io.confluent.protobuf.MetaProto;
import io.confluent.protobuf.MetaProto.Meta;
import io.confluent.protobuf.type.Decimal;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Collections;
import java.util.EnumMap;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Stream;

/**
 * Converts logical type {@link Schema} to Protobuf {@link Descriptor}.
 */
public class LogicalTypeToProtoConverter {

  private static final Set<Schema.Type> PROTO_MESSAGE_TYPES =
      EnumSet.of(
          Schema.Type.DECIMAL,
          Schema.Type.DATE,
          Schema.Type.TIME,
          Schema.Type.TIMESTAMP,
          Schema.Type.TIMESTAMP_LTZ,
          Schema.Type.VARIANT,
          Schema.Type.STRUCT);

  /**
   * LT leaf types that wrap as their canonical well-known proto message at
   * root. Mirrors ProtobufData's wrapper-at-root pattern: the resulting
   * ProtobufSchema's root descriptor IS the well-known wrapper, with no
   * synthetic envelope.
   *
   * <p>Round-trip notes:
   *  - TINYINT/SMALLINT widen to INT (both share Int32Value).
   *  - VARCHAR(n) widens to unbounded STRING (length lost).
   *  - VARBINARY(n) widens to unbounded BYTES (length lost).
   *  - All wrap-at-root types become nullable on read (wrappers are inherently
   *    optional). NOT NULL → NULL is silent nullability promotion.
   * Excluded (still throw): CHAR(n), BINARY(n) (fixed-vs-variable shift),
   * DATE/TIME/TIMESTAMP/TIMESTAMP_LTZ/DECIMAL (parameter loss), and all
   * composite types.
   */
  private static final Map<Schema.Type, Descriptor> ROOT_WRAPPER_DESCRIPTORS =
      buildRootWrapperDescriptors();

  private static Map<Schema.Type, Descriptor> buildRootWrapperDescriptors() {
    Map<Schema.Type, Descriptor> m = new EnumMap<>(Schema.Type.class);
    m.put(Schema.Type.BOOLEAN, com.google.protobuf.BoolValue.getDescriptor());
    m.put(Schema.Type.TINYINT, com.google.protobuf.Int32Value.getDescriptor());
    m.put(Schema.Type.SMALLINT, com.google.protobuf.Int32Value.getDescriptor());
    m.put(Schema.Type.INT, com.google.protobuf.Int32Value.getDescriptor());
    m.put(Schema.Type.BIGINT, com.google.protobuf.Int64Value.getDescriptor());
    m.put(Schema.Type.FLOAT, com.google.protobuf.FloatValue.getDescriptor());
    m.put(Schema.Type.DOUBLE, com.google.protobuf.DoubleValue.getDescriptor());
    m.put(Schema.Type.VARCHAR, com.google.protobuf.StringValue.getDescriptor());
    m.put(Schema.Type.VARBINARY, com.google.protobuf.BytesValue.getDescriptor());
    m.put(Schema.Type.VARIANT, io.confluent.protobuf.type.Variant.getDescriptor());
    return Collections.unmodifiableMap(m);
  }


  public static ProtobufSchema fromLogicalType(
      LogicalType logicalType, String rowName) {
    return fromLogicalType(logicalType, rowName, LogicalTypeVersion.V2);
  }

  public static ProtobufSchema fromLogicalType(
      LogicalType logicalType, String rowName, LogicalTypeVersion version) {
    if (!logicalType.getExternalImports().isEmpty()) {
      throw new ValidationException(
          "Cannot emit to Proto: LogicalType carries JSON-specific synthetic "
              + "external imports " + logicalType.getExternalImports().keySet()
              + ". Re-construct the LT against Proto-compatible externals.");
    }
    Schema schema = logicalType.getRootSchema();

    // Multi-root sugar: when the root is a UNION whose members are all
    // NAMED_TYPE_REFs to local types, the visitor's sugar inferred it from
    // multiple unreferenced top-level named-type declarations. Proto's wire
    // model has no native equivalent of a discriminated union at the file
    // root, so we treat it as a multi-message file: the first union member
    // becomes the primary root, the rest fall through to the existing
    // namedTypes peer-emission path. (Avro/JSON writers emit the union
    // structurally; only proto repurposes it.)
    if (isMultiRootUnion(schema, logicalType.getNamedTypes())) {
      schema = Schema.createNamedTypeRef(
          schema.getBranches().get(0).getSchema().getQualifiedName())
          .setNullable(false);
    }

    // External-only public import: the registered root is a NAMED_TYPE_REF to
    // a type defined in an external (referenced) schema, with no local types.
    // Proto's `import public` is the natural representation — produces an
    // otherwise-empty proto file that publicly imports the referenced type,
    // allowing downstream consumers to reach it via this schema's name.
    if (schema.getType() == Schema.Type.NAMED_TYPE_REF) {
      String fullName = schema.getQualifiedName();
      if (!logicalType.getNamedTypes().containsKey(fullName)
          && logicalType.getResolvedReferences().containsKey(fullName)) {
        return buildPublicImportFile(logicalType, fullName, version);
      }
    }

    // If root is a NAMED_TYPE_REF pointing at a locally-defined STRUCT, resolve
    // it: the named struct becomes the file's root message, the simple name
    // becomes the message name (rowName), and the namespace prefix is dropped
    // from the message name (it lives on the file's package declaration).
    // The resolved FQN is excluded from the file-level peer pass below — the
    // root IS that named type's definition, so we don't emit it twice.
    String rootResolvedFromFqn = null;
    if (schema.getType() == Schema.Type.NAMED_TYPE_REF) {
      String fullName = schema.getQualifiedName();
      Schema named = logicalType.getNamedTypes().get(fullName);
      if (named != null && named.getType() == Schema.Type.STRUCT) {
        schema = named;
        rowName = simpleName(fullName);
        rootResolvedFromFqn = fullName;
      }
    }

    // If root is a wrappable leaf type, emit the canonical well-known proto
    // message as the schema's root descriptor. No local message definition,
    // no rowName usage, no namedTypes/references processing.
    if (ROOT_WRAPPER_DESCRIPTORS.containsKey(schema.getType())) {
      validateWrappableRoot(logicalType, schema);
      return applyEditionMetadata(
          new ProtobufSchema(ROOT_WRAPPER_DESCRIPTORS.get(schema.getType())), version);
    }

    // A proto file declares a single `package`; every LOCAL named type lives in
    // it. (Externals live in imported files with their own packages — they're
    // emitted as `import` statements via the resolvedReferences pre-walk
    // below.) Validate against locals only.
    Map<String, Schema> localNamedTypes = logicalType.getLocalNamedTypes();
    validateNamespacesMatchFilePackage(
        localNamedTypes, logicalType.getNamespace());

    // Reject cross-namespace simple-name collisions before building. Proto
    // type names at file level must be unique simple identifiers.
    validateNoSimpleNameCollisions(
        localNamedTypes, rootResolvedFromFqn, rowName);

    FromLogicalContext<GenericDescriptor> ctx =
        new FromLogicalContext<>(logicalType, version);

    // Pre-populate cache with all named types (messages and enums, including
    // nested) from each external schema. Each registered type gets a renamed
    // FileDescriptor clone so it can be imported by its qualified name. The
    // source-ref name (the resolvedReferences key) is the dedup identifier:
    // two types from the same source file would produce duplicate-symbol
    // errors if each got its own renamed clone, so subsequent FQNs from a
    // single source share the first FQN's rename.
    for (Map.Entry<String, String> entry :
            logicalType.getResolvedReferences().entrySet()) {
      ProtobufSchema parsed = new ProtobufSchema(entry.getValue(),
          logicalType.getReferences(), logicalType.getResolvedReferences(),
          null, null, null, null);
      FileDescriptor file = parsed.toDescriptor().getFile();
      registerExternalFile(file, entry.getKey(), ctx);
    }

    ProtobufSchema result = new ProtobufSchema(
        buildFile(schema, rowName, logicalType.getNamespace(),
            localNamedTypes, rootResolvedFromFqn, logicalType, ctx),
        logicalType.getReferences());
    // V1 must stay byte-equivalent to Flink's writer output, which does not
    // normalize. Normalize would expand type-name references to fully-qualified
    // form and reorder fields/imports — both visible deviations from Flink's
    // raw descriptor emission.
    if (version != LogicalTypeVersion.V1) {
      result = result.normalize();
    }
    return applyEditionMetadata(result, version);
  }

  private static ProtobufSchema applyEditionMetadata(
      ProtobufSchema schema, LogicalTypeVersion version) {
    Map<String, String> metadataProps = new LinkedHashMap<>();
    metadataProps.put("confluent:edition", version == LogicalTypeVersion.V1 ? "1" : "2");
    return schema.copy(new Metadata(null, metadataProps, null), null);
  }

  /**
   * Build a proto file that publicly imports a single externally-referenced
   * type via {@code import public}. Result has zero local messages/enums;
   * consumers resolve the root through {@code schema.toDescriptor()} which
   * uses {@link ProtobufSchema#name()}'s public-import fall-through.
   *
   * <p>Matches the behavior expected when a script declares
   * {@code REFERENCE TYPE com.Foo; TYPE com.Foo;} — the registered
   * root IS the external type, with no local body to define.
   */
  private static ProtobufSchema buildPublicImportFile(
      LogicalType logicalType, String externalTypeFullName,
      LogicalTypeVersion version) {
    String externalProtoText =
        logicalType.getResolvedReferences().get(externalTypeFullName);
    ProtobufSchema externalSchema = new ProtobufSchema(
        externalProtoText,
        logicalType.getReferences(),
        logicalType.getResolvedReferences(),
        null, null, null, null);
    FileDescriptor externalFile = externalSchema.toDescriptor().getFile();

    String externalFileName = externalTypeFullName;
    try {
      FileDescriptor renamedExternal = FileDescriptor.buildFrom(
          externalFile.toProto().toBuilder().setName(externalFileName).build(),
          externalFile.getDependencies().toArray(new FileDescriptor[0]));

      FileDescriptorProto.Builder fileBuilder = FileDescriptorProto.newBuilder()
          .setSyntax("proto3")
          .addDependency(externalFileName)
          .addPublicDependency(0);
      String packageName = logicalType.getNamespace();
      if (packageName != null && !packageName.isEmpty()) {
        fileBuilder.setPackage(packageName);
      }

      FileDescriptor file = FileDescriptor.buildFrom(
          fileBuilder.build(),
          new FileDescriptor[] {renamedExternal});
      return applyEditionMetadata(
          new ProtobufSchema(file, logicalType.getReferences()), version);
    } catch (DescriptorValidationException e) {
      throw new ValidationException(
          "Failed to build public-import proto for '"
              + externalTypeFullName + "'", e);
    }
  }

  private static String simpleName(String fqn) {
    int dot = fqn.lastIndexOf('.');
    return dot < 0 ? fqn : fqn.substring(dot + 1);
  }

  /**
   * Detect the visitor's multi-root sugar shape: a non-nullable
   * {@code UNION} whose every branch is a {@code NAMED_TYPE_REF} resolving to
   * a locally-defined type. Proto repurposes this as "the first member is the
   * root and the rest are file-level peer messages" — see callsite for
   * rationale.
   */
  private static boolean isMultiRootUnion(
      Schema schema, Map<String, Schema> namedTypes) {
    if (schema.getType() != Schema.Type.UNION || schema.isNullable()) {
      return false;
    }
    if (schema.getBranches().isEmpty()) {
      return false;
    }
    for (UnionBranch branch : schema.getBranches()) {
      Schema member = branch.getSchema();
      if (member.getType() != Schema.Type.NAMED_TYPE_REF) {
        return false;
      }
      if (!namedTypes.containsKey(member.getQualifiedName())) {
        return false;
      }
    }
    return true;
  }

  /**
   * Build a synthesized message/enum name from a field name and a suffix.
   *
   * <p>V2 (canonical): {@code UpperCamelCase(field) + suffix} — e.g.,
   *   {@code foo_bar → FooBarWrapper}, {@code xs → XsRow}.
   *
   * <p>V1 (Flink-compatible) varies by suffix to match Flink's per-suffix
   * conventions:
   * <ul>
   *   <li>{@code Row}: {@code <field>_Row} (verbatim + underscore)</li>
   *   <li>{@code Enum}: {@code <field>_Enum} (symmetry with Row)</li>
   *   <li>{@code RepeatedWrapper}/{@code ElementWrapper}: {@code <field><Suffix>}
   *     (verbatim, no underscore)</li>
   *   <li>{@code Entry} (map entries): {@code UpperCamelCase(field) + Entry}
   *     (matches both Flink and protoc)</li>
   * </ul>
   */
  private static String toMessageName(String fieldName, String suffix,
                                      FromLogicalContext<?> ctx) {
    if (ctx.isV1()) {
      if (suffix.equals(CommonConstants.MAP_ENTRY_SUFFIX)) {
        // Match Flink's exact algorithm: concatenate field + "_" + suffix,
        // then run the whole thing through LOWER_UNDERSCORE → UPPER_CAMEL.
        // Has the side effect of lowercasing any embedded uppercase letters
        // (e.g., "mapOfArrays_Entry" → "Mapofarrays Entry"), which mangles
        // camelCase field names but mirrors Flink's behavior exactly.
        return CaseFormat.LOWER_UNDERSCORE.to(CaseFormat.UPPER_CAMEL, fieldName + "_" + suffix);
      }
      if (suffix.equals(CommonConstants.FLINK_ROW_SUFFIX)
          || suffix.equals(CommonConstants.FLINK_ENUM_SUFFIX)) {
        return fieldName + "_" + suffix;
      }
      // Wrappers: verbatim field name + suffix (no separator).
      return fieldName + suffix;
    }
    return toUpperCamel(fieldName) + suffix;
  }

  private static String toUpperCamel(String s) {
    if (s == null || s.isEmpty()) {
      return s;
    }
    if (s.contains("_")) {
      return CaseFormat.LOWER_UNDERSCORE.to(CaseFormat.UPPER_CAMEL, s);
    }
    return Character.toUpperCase(s.charAt(0)) + s.substring(1);
  }

  private static String namespaceOf(String fqn) {
    int dot = fqn.lastIndexOf('.');
    return dot < 0 ? "" : fqn.substring(0, dot);
  }

  private static void validateNamespacesMatchFilePackage(
      Map<String, Schema> namedTypes, String filePackage) {
    String expected = filePackage == null ? "" : filePackage;
    Set<String> defined = namedTypes.keySet();
    for (String fqn : defined) {
      // Nested types (those with a parent in `namedTypes`) inherit their
      // namespace from their top-level ancestor — only that ancestor's
      // namespace needs to match the file package.
      if (LogicalType.parentOf(fqn, defined) != null) {
        continue;
      }
      String ns = namespaceOf(fqn);
      if (!ns.equals(expected)) {
        throw new ValidationException(
            "Proto requires all named types to share the file's package, but "
                + "'" + fqn + "' is in namespace '"
                + (ns.isEmpty() ? "<none>" : ns) + "' while the file's package "
                + "is '" + (expected.isEmpty() ? "<none>" : expected) + "'. "
                + "Either align the namespaces, or split cross-namespace types "
                + "into separate schemas referenced via REFERENCE TYPE.");
      }
    }
  }

  private static void validateNoSimpleNameCollisions(
      Map<String, Schema> namedTypes, String rootResolvedFromFqn,
      String rowName) {
    Map<String, List<String>> bySimpleName = new LinkedHashMap<>();
    Set<String> defined = namedTypes.keySet();
    for (String fqn : defined) {
      if (fqn.equals(rootResolvedFromFqn)) {
        continue;
      }
      // Nested types live in their parent's scope, not the file's, so they
      // don't participate in file-level simple-name collision checks.
      if (LogicalType.parentOf(fqn, defined) != null) {
        continue;
      }
      bySimpleName.computeIfAbsent(simpleName(fqn),
          k -> new ArrayList<>()).add(fqn);
    }
    // The root's own name is also a file-level identifier — guard against a
    // named type whose simple name matches it.
    if (rowName != null) {
      List<String> matching = bySimpleName.get(rowName);
      if (matching != null && !matching.isEmpty()) {
        throw new ValidationException(
            "Named type '" + matching.get(0) + "' has the same simple name '"
                + rowName + "' as the root message.");
      }
    }
    for (Map.Entry<String, List<String>> e : bySimpleName.entrySet()) {
      if (e.getValue().size() > 1) {
        throw new ValidationException(
            "Named types share the simple name '" + e.getKey() + "': "
                + e.getValue() + ". Proto requires unique simple names at file "
                + "level.");
      }
    }
  }

  /**
   * Wrapped-root schemas can't carry schema-level metadata, named types, or
   * external references — the root descriptor is an external well-known
   * message we can't annotate. Reject loudly rather than silently dropping.
   */
  private static void validateWrappableRoot(LogicalType logicalType, Schema schema) {
    if (schema.getDoc() != null
        || !schema.getTags().isEmpty()
        || !schema.getParams().isEmpty()) {
      throw new ValidationException(
          "Schema-level doc/tags/params are not supported on a wrapped root "
              + "(type " + schema.getType() + "). Wrap the value in a STRUCT instead.");
    }
    if (!logicalType.getNamedTypes().isEmpty()) {
      throw new ValidationException(
          "Local namedTypes are not supported on a wrapped root "
              + "(type " + schema.getType() + "). Wrap the value in a STRUCT instead.");
    }
    if (!logicalType.getResolvedReferences().isEmpty()) {
      throw new ValidationException(
          "External references are not supported on a wrapped root "
              + "(type " + schema.getType() + "). Wrap the value in a STRUCT instead.");
    }
  }

  private static FileDescriptor buildFile(
      Schema schema, String rowName, String packageName,
      Map<String, Schema> namedTypes, String rootResolvedFromFqn,
      LogicalType logicalType,
      FromLogicalContext<GenericDescriptor> ctx) {
    if (schema.getType() != Schema.Type.STRUCT) {
      throw new ValidationException(
          "Top-level schema must be STRUCT, got " + schema.getType());
    }
    try {
      final Set<String> dependencies = new TreeSet<>();
      Map<String, List<String>> nestingTree = logicalType.nestingTree();
      // Build the root message first so it lands at index 0 — the reader
      // identifies it by position. If the root resolved to a named type,
      // embed its nested children too. An anonymous root has no nested
      // children — note that `nestedChildrenOf(null, ...)` would return the
      // top-level types (which live under the `null` key in the tree), so we
      // must guard against that.
      List<NestedChild> rootChildren = rootResolvedFromFqn == null
          ? List.of()
          : nestedChildrenOf(rootResolvedFromFqn, namedTypes, nestingTree);
      final DescriptorProto rootMessage = fromStructType(
          schema, rowName, dependencies, rootChildren, ctx);
      FileDescriptorProto.Builder fileBuilder = FileDescriptorProto.newBuilder()
          .addMessageType(rootMessage)
          .setSyntax("proto3");

      // Hoist every TOP-LEVEL local named type as a file-level peer of the
      // root. Nested types are embedded inside their parent message, not
      // hoisted. Order is preserved from the LogicalType's namedTypes map.
      // Skip the entry whose definition was inlined as the root.
      for (Map.Entry<String, Schema> entry : namedTypes.entrySet()) {
        String fqn = entry.getKey();
        if (fqn.equals(rootResolvedFromFqn)) {
          continue;
        }
        if (LogicalType.parentOf(fqn, namedTypes.keySet()) != null) {
          // Nested — handled by its parent's recursive emission.
          continue;
        }
        Schema typeDef = entry.getValue();
        String name = simpleName(fqn);
        if (typeDef.getType() == Schema.Type.STRUCT) {
          fileBuilder.addMessageType(fromStructType(
              typeDef, name, dependencies,
              nestedChildrenOf(fqn, namedTypes, nestingTree), ctx));
        } else if (typeDef.getType() == Schema.Type.ENUM) {
          fileBuilder.addEnumType(buildEnumDescriptor(typeDef, name, ctx));
        } else {
          throw new ValidationException(
              "Unsupported named type kind for proto: " + typeDef.getType()
                  + " (" + fqn + ")");
        }
      }

      fileBuilder.addAllDependency(dependencies);
      if (packageName != null && !packageName.isEmpty()) {
        fileBuilder.setPackage(packageName);
      }
      final FileDescriptorProto fileDescriptorProto = fileBuilder.build();
      return FileDescriptor.buildFrom(
              fileDescriptorProto,
              Stream.concat(
                  Stream.of(
                          Meta.getDescriptor(),
                          Date.getDescriptor(),
                          TimeOfDay.getDescriptor(),
                          Timestamp.getDescriptor(),
                          Decimal.getDescriptor(),
                          io.confluent.protobuf.type.Variant.getDescriptor())
                      .map(Descriptor::getFile),
                  ctx.getExternalDependencies().stream())
              .toArray(FileDescriptor[]::new))
          .getFile();
    } catch (DescriptorValidationException e) {
      throw new ValidationException(
          "Failed to translate the provided schema to a Protobuf descriptor", e);
    }
  }

  private static EnumDescriptorProto buildEnumDescriptor(
      Schema enumSchema, String enumName, FromLogicalContext<?> ctx) {
    EnumDescriptorProto.Builder enumBuilder = EnumDescriptorProto.newBuilder()
        .setName(enumName);
    int enumIndex = 0;
    for (EnumValue ev : enumSchema.getEnumValues()) {
      EnumValueDescriptorProto.Builder evBuilder =
          EnumValueDescriptorProto.newBuilder()
              .setName(ev.getSymbol())
              .setNumber(enumIndex++);
      if (!ctx.isV1()) {
        Meta.Builder evMeta = buildEnumValueMeta(ev);
        if (evMeta != null) {
          evBuilder.setOptions(EnumValueOptions.newBuilder()
              .setExtension(MetaProto.enumValueMeta, evMeta.build())
              .build());
        }
      }
      enumBuilder.addValue(evBuilder.build());
    }
    if (!ctx.isV1()) {
      addEnumMeta(enumBuilder, enumSchema);
    }
    return enumBuilder.build();
  }

  /**
   * Build a struct descriptor with no user-declared nested children. Used for
   * synthesized messages (wrappers, map entries) where nesting comes only from
   * field processing, not from {@link LogicalType#getNamedTypes}.
   */
  private static DescriptorProto fromStructType(
      Schema schema, String rowName, Set<String> dependencies,
      FromLogicalContext<GenericDescriptor> ctx) {
    return fromStructType(schema, rowName, dependencies, List.of(), ctx);
  }

  /**
   * Build a struct descriptor and embed the supplied user-declared nested
   * children (each as a {@code (simpleName, schema, fqn)} triple). The nested
   * children are emitted via {@code addNestedType}/{@code addEnumType} so they
   * appear as proto's native nested messages/enums inside this message.
   *
   * <p>Recursive: if a child's schema has its own nested children (deeper
   * nesting), they're embedded in turn.
   */
  private static DescriptorProto fromStructType(
      Schema schema, String rowName, Set<String> dependencies,
      List<NestedChild> userNested,
      FromLogicalContext<GenericDescriptor> ctx) {
    final DescriptorProto.Builder builder = DescriptorProto.newBuilder();
    builder.setName(rowName);
    // Add schema-level doc/tags/params via MessageOptions (skipped in V1).
    if (!ctx.isV1()) {
      addMessageMeta(builder, schema);
    }
    // Both lists collect synthesized definitions added by fromField as it
    // processes child fields. They're added to the builder at the end so
    // they appear nested within this message.
    final List<DescriptorProto> nestedRows = new ArrayList<>();
    final List<EnumDescriptorProto> nestedEnums = new ArrayList<>();
    final List<Field> fields = schema.getFields();
    int fieldNumber = 1;

    for (int i = 0; i < fields.size(); i++) {
      final Field field = fields.get(i);
      // Check if this is a UNION type — convert to oneof
      if (field.getSchema().getType() == Schema.Type.UNION) {
        if (ctx.isV1()) {
          throw new ValidationException("UNION not supported in V1 emission mode");
        }
        int oneofIndex = builder.getOneofDeclCount();
        builder.addOneofDecl(
            OneofDescriptorProto.newBuilder().setName(field.getName()).build());
        Schema unionSchema = field.getSchema();
        for (UnionBranch branch : unionSchema.getBranches()) {
          final FieldDescriptorProto.Builder fieldProtoBuilder;
          // Proto3 oneof can't contain repeated/map fields. Composite branches
          // (ARRAY/MAP/MULTISET) must be wrapped in a single message field
          // regardless of nullability — fromField only wraps when nullable, so
          // we force the wrap here for the non-nullable composite case.
          if (isCompositeRepeated(branch.getSchema())) {
            fieldProtoBuilder = wrapRepeatedType(
                branch.getSchema(),
                branch.getName(),
                branch.getDoc(),
                fieldNumber++,
                nestedRows,
                dependencies,
                ctx);
          } else {
            fieldProtoBuilder = fromField(
                branch.getSchema(),
                branch.getName(),
                branch.getDoc(),
                fieldNumber++,
                nestedRows,
                nestedEnums,
                dependencies,
                ctx);
          }
          if (!branch.getParams().isEmpty()) {
            Map<String, String> stringParams = new LinkedHashMap<>();
            branch.getParams().forEach((k, v) -> stringParams.put(k, String.valueOf(v)));
            addMetaParams(fieldProtoBuilder, stringParams);
          }
          fieldProtoBuilder.setOneofIndex(oneofIndex);
          // Clear optional label for oneof fields
          fieldProtoBuilder.clearProto3Optional();
          builder.addField(fieldProtoBuilder.build());
        }
      } else {
        final FieldDescriptorProto.Builder fieldProtoBuilder =
            fromField(
                field.getSchema(),
                field.getName(),
                field.getDoc(),
                fieldNumber++,
                nestedRows,
                nestedEnums,
                dependencies,
                ctx);
        if (!ctx.isV1()) {
          addFieldTagsAndParams(fieldProtoBuilder, field);
          applyDefaultIfPresent(fieldProtoBuilder, field);
        }
        builder.addField(fieldProtoBuilder.build());
      }
    }
    builder.addAllNestedType(nestedRows);
    builder.addAllEnumType(nestedEnums);
    // User-declared nested types (Outer.Inner under Outer) emitted last so
    // they don't interleave with synthesized wrapper messages. The
    // logical.named marker lets the reader distinguish user-marked named
    // types from writer-synthesized wrappers, but it's NOT needed when the
    // type is part of a cycle — the reader's cycle-detection auto-promotes
    // those regardless of the marker. Skipping the marker for cyclic types
    // means anonymous self-recursive nested types (e.g., a linked-list
    // pattern) round-trip byte-equal: source had no marker; output has none.
    Map<String, Schema> ltNamedTypes = ctx.getLogicalType().getNamedTypes();
    for (NestedChild child : userNested) {
      boolean cyclic = LogicalType.isCyclic(child.fqn, ltNamedTypes);
      if (child.schema.getType() == Schema.Type.STRUCT) {
        DescriptorProto built = fromStructType(
            child.schema, child.simpleName, dependencies, child.children, ctx);
        builder.addNestedType(cyclic ? built : withNamedTypeMarker(built));
      } else if (child.schema.getType() == Schema.Type.ENUM) {
        EnumDescriptorProto built = buildEnumDescriptor(
            child.schema, child.simpleName, ctx);
        // Enums can't participate in cycles; always mark.
        builder.addEnumType(withNamedTypeMarker(built));
      } else {
        throw new ValidationException(
            "Unsupported nested type kind for proto: "
                + child.schema.getType() + " (" + child.fqn + ")");
      }
    }
    return builder.build();
  }

  /**
   * Bundle of the data needed to embed one user-declared nested type:
   * its simple name (used as proto identifier), the schema body, the original
   * fully-qualified name (for diagnostics), and its own grandchildren so the
   * recursion can emit them too.
   */
  private static final class NestedChild {
    final String simpleName;
    final String fqn;
    final Schema schema;
    final List<NestedChild> children;

    NestedChild(String simpleName, String fqn, Schema schema,
                List<NestedChild> children) {
      this.simpleName = simpleName;
      this.fqn = fqn;
      this.schema = schema;
      this.children = children;
    }
  }

  /**
   * Tack the {@link CommonConstants#LOGICAL_NAMED_PROP} marker onto a nested
   * message's MessageOptions/Meta params. Preserves any pre-existing meta
   * (doc, tags, params); creates a Meta extension if none was set.
   */
  private static DescriptorProto withNamedTypeMarker(DescriptorProto built) {
    Meta existing = built.getOptions().getExtension(MetaProto.messageMeta);
    Meta updated = existing.toBuilder()
        .putParams(CommonConstants.LOGICAL_NAMED_PROP, "true")
        .build();
    MessageOptions newOptions = built.getOptions().toBuilder()
        .setExtension(MetaProto.messageMeta, updated)
        .build();
    return built.toBuilder().setOptions(newOptions).build();
  }

  /**
   * EnumDescriptorProto variant of {@link #withNamedTypeMarker(DescriptorProto)}.
   */
  private static EnumDescriptorProto withNamedTypeMarker(EnumDescriptorProto built) {
    Meta existing = built.getOptions().getExtension(MetaProto.enumMeta);
    Meta updated = existing.toBuilder()
        .putParams(CommonConstants.LOGICAL_NAMED_PROP, "true")
        .build();
    EnumOptions newOptions = built.getOptions().toBuilder()
        .setExtension(MetaProto.enumMeta, updated)
        .build();
    return built.toBuilder().setOptions(newOptions).build();
  }

  /**
   * Recursively assemble the {@link NestedChild} list for {@code parentFqn}'s
   * direct children, with each child carrying its own descendants. Returns an
   * empty list for top-level types (parent == null) and types with no nested
   * children. Order follows the LogicalType's namedTypes iteration order.
   */
  private static List<NestedChild> nestedChildrenOf(
      String parentFqn, Map<String, Schema> namedTypes,
      Map<String, List<String>> nestingTree) {
    List<String> childFqns = nestingTree.get(parentFqn);
    if (childFqns == null || childFqns.isEmpty()) {
      return List.of();
    }
    List<NestedChild> out = new ArrayList<>(childFqns.size());
    for (String childFqn : childFqns) {
      out.add(new NestedChild(
          simpleName(childFqn), childFqn, namedTypes.get(childFqn),
          nestedChildrenOf(childFqn, namedTypes, nestingTree)));
    }
    return out;
  }

  private static FieldDescriptorProto.Builder fromField(
      Schema schema,
      String fieldName,
      String comment,
      int fieldIndex,
      List<DescriptorProto> nestedRows,
      List<EnumDescriptorProto> nestedEnums,
      Set<String> dependencies,
      FromLogicalContext<GenericDescriptor> ctx) {
    final FieldDescriptorProto.Builder builder = FieldDescriptorProto.newBuilder();
    builder.setName(fieldName);
    builder.setNumber(fieldIndex);
    if (schema.isNullable()) {
      builder.setProto3Optional(true);
    }

    addComment(builder, comment);

    if (!schema.isNullable() && PROTO_MESSAGE_TYPES.contains(schema.getType())) {
      addMetaParam(builder, CommonConstants.FLINK_NOT_NULL, "true");
    }

    switch (schema.getType()) {
      case BOOLEAN:
        builder.setType(Type.TYPE_BOOL);
        return builder;
      case TINYINT:
        builder.setType(Type.TYPE_INT32);
        addMetaParam(builder, CommonConstants.CONNECT_TYPE_PROP, CommonConstants.CONNECT_TYPE_INT8);
        return builder;
      case SMALLINT:
        builder.setType(Type.TYPE_INT32);
        addMetaParam(
            builder, CommonConstants.CONNECT_TYPE_PROP, CommonConstants.CONNECT_TYPE_INT16);
        return builder;
      case INT:
        builder.setType(Type.TYPE_INT32);
        return builder;
      case BIGINT:
        builder.setType(Type.TYPE_INT64);
        return builder;
      case FLOAT:
        builder.setType(Type.TYPE_FLOAT);
        return builder;
      case DOUBLE:
        builder.setType(Type.TYPE_DOUBLE);
        return builder;
      case CHAR: {
        int length = schema.getLength();
        return createLengthLimitedType(builder, Type.TYPE_STRING, length, length);
      }
      case VARCHAR:
        return createLengthLimitedType(builder, Type.TYPE_STRING, -1, schema.getLength());
      case BINARY:
        return createLengthLimitedType(
            builder, Type.TYPE_BYTES, schema.getLength(), schema.getLength());
      case VARBINARY:
        return createLengthLimitedType(builder, Type.TYPE_BYTES, -1, schema.getLength());
      case TIMESTAMP:
        return createTimestampFieldDescriptor(
            schema.getPrecision(), true, dependencies, builder);
      case TIMESTAMP_LTZ:
        return createTimestampFieldDescriptor(
            schema.getPrecision(), false, dependencies, builder);
      case DATE:
        builder.setType(Type.TYPE_MESSAGE);
        builder.setTypeName(makeItTopLevelScoped(CommonConstants.PROTOBUF_DATE_TYPE));
        dependencies.add(CommonConstants.PROTOBUF_DATE_LOCATION);
        return builder;
      case TIME:
        return createTimeFieldDescriptor(schema.getPrecision(), dependencies, builder);
      case DECIMAL:
        return createDecimalField(schema, dependencies, builder);
      case VARIANT:
        if (ctx.isV1()) {
          throw new ValidationException("VARIANT not supported in V1 emission mode");
        }
        builder.setType(Type.TYPE_MESSAGE);
        builder.setTypeName(makeItTopLevelScoped(CommonConstants.PROTOBUF_VARIANT_TYPE));
        dependencies.add(CommonConstants.PROTOBUF_VARIANT_LOCATION);
        return builder;
      case NAMED_TYPE_REF: {
        String name = schema.getQualifiedName();
        // Externals live in namedTypes too (lazy-promoted) but must be
        // emitted as imports — so guard local branch with !isExternal.
        if (ctx.hasNamedType(name)
            && !ctx.getLogicalType().isExternal(name)) {
          Schema typeDef = ctx.getNamedType(name);
          builder.setType(typeDef.getType() == Schema.Type.ENUM
              ? Type.TYPE_ENUM : Type.TYPE_MESSAGE);
          builder.setTypeName(name);
          return builder;
        }
        GenericDescriptor external = ctx.getConverted(name);
        if (external != null) {
          // External type — add import and reference; dispatch on descriptor kind
          builder.setType(external instanceof EnumDescriptor
              ? Type.TYPE_ENUM : Type.TYPE_MESSAGE);
          builder.setTypeName(makeItTopLevelScoped(name));
          // Use the de-duped import name when multiple FQNs share a source
          // file; falls back to the FQN for the legacy 1-type-per-file case.
          String importName = ctx.externalImportNameFor(name);
          dependencies.add(importName != null ? importName : name);
          return builder;
        }
        throw new ValidationException("Unknown named type reference: " + name);
      }
      case STRUCT:
        return createMessageField(
            schema, fieldName, nestedRows, dependencies, builder, ctx);
      case ENUM: {
        // Anonymous ENUM: synthesize `<UpperCamelField>Enum`, add the enum
        // descriptor to the parent message's nested enums, emit a typeName
        // reference. proto3 allows `repeated <enum>` natively, so this works
        // both as a regular field and as an ARRAY element with no wrapping.
        String enumName = toMessageName(fieldName, CommonConstants.FLINK_ENUM_SUFFIX, ctx);
        nestedEnums.add(buildEnumDescriptor(schema, enumName, ctx));
        builder.setType(Type.TYPE_ENUM);
        builder.setTypeName(enumName);
        return builder;
      }
      case MAP: {
        if (schema.isNullable()) {
          return wrapRepeatedType(
              schema, fieldName, comment, fieldIndex, nestedRows, dependencies, ctx);
        } else {
          return createNotNullMapLikeField(
              fieldName, comment, fieldIndex, nestedRows, dependencies,
              schema.getKeyType(), schema.getValueType(), ctx);
        }
      }
      case ARRAY:
        if (schema.isNullable()) {
          return wrapRepeatedType(
              schema, fieldName, comment, fieldIndex, nestedRows, dependencies, ctx);
        } else {
          return createRepeatedNotNull(
              fieldName, comment, fieldIndex, nestedRows, nestedEnums,
              dependencies, schema.getElementType(), ctx);
        }
      case MULTISET:
        return createMultisetType(
            schema, fieldName, comment, fieldIndex, nestedRows, dependencies, ctx);
      case UNION:
        if (ctx.isV1()) {
          throw new ValidationException("UNION not supported in V1 emission mode");
        }
        // UNION has no value-typed representation in proto — it must become
        // a oneof, and a oneof can only live as a regular field of a struct.
        // Wrap so the UNION can sit in any position fromField is called from
        // (oneof branch in another UNION; element of a repeated).
        return wrapOneofType(
            schema, fieldName, comment, fieldIndex, nestedRows, dependencies, ctx);
      default:
        throw new ValidationException(
            "Unsupported to derive Protobuf Schema for type " + schema);
    }
  }

  private static FieldDescriptorProto.Builder createMultisetType(
      Schema schema,
      String fieldName,
      String fieldComment,
      int fieldIndex,
      List<DescriptorProto> nestedRows,
      Set<String> dependencies,
      FromLogicalContext<GenericDescriptor> ctx) {
    final FieldDescriptorProto.Builder builder;
    if (schema.isNullable()) {
      builder = wrapRepeatedType(
          schema, fieldName, fieldComment, fieldIndex, nestedRows, dependencies, ctx);
    } else {
      builder = createNotNullMapLikeField(
          fieldName, fieldComment, fieldIndex, nestedRows, dependencies,
          schema.getElementType(),
          Schema.create(Schema.Type.INT).setNullable(false), ctx);
    }
    addMetaParam(builder, CommonConstants.FLINK_TYPE_PROP, CommonConstants.FLINK_TYPE_MULTISET);
    return builder;
  }

  private static FieldDescriptorProto.Builder createDecimalField(
      Schema schema,
      Set<String> dependencies,
      FieldDescriptorProto.Builder builder) {
    builder.setType(Type.TYPE_MESSAGE);
    final Map<String, String> params = new LinkedHashMap<>();
    params.put(CommonConstants.PROTOBUF_PRECISION_PROP, String.valueOf(schema.getPrecision()));
    params.put(CommonConstants.PROTOBUF_SCALE_PROP, String.valueOf(schema.getScale()));
    addMetaParams(builder, params);
    builder.setTypeName(makeItTopLevelScoped(CommonConstants.PROTOBUF_DECIMAL_TYPE));
    dependencies.add(CommonConstants.PROTOBUF_DECIMAL_LOCATION);
    return builder;
  }

  private static FieldDescriptorProto.Builder createMessageField(
      Schema schema,
      String fieldName,
      List<DescriptorProto> nestedRows,
      Set<String> dependencies,
      FieldDescriptorProto.Builder builder,
      FromLogicalContext<GenericDescriptor> ctx) {
    return createMessageFieldWithName(
        schema, toMessageName(fieldName, CommonConstants.FLINK_ROW_SUFFIX, ctx),
        nestedRows, dependencies, builder, ctx);
  }

  private static FieldDescriptorProto.Builder createMessageFieldWithName(
      Schema schema,
      String typeName,
      List<DescriptorProto> nestedRows,
      Set<String> dependencies,
      FieldDescriptorProto.Builder builder,
      FromLogicalContext<GenericDescriptor> ctx) {
    final DescriptorProto nestedRowDescriptor =
        fromStructType(schema, typeName, dependencies, ctx);
    nestedRows.add(nestedRowDescriptor);
    builder.setType(Type.TYPE_MESSAGE);
    builder.setTypeName(typeName);
    return builder;
  }

  private static FieldDescriptorProto.Builder createLengthLimitedType(
      FieldDescriptorProto.Builder builder, Type type, int minLength, int maxLength) {
    final Map<String, String> params = new LinkedHashMap<>();
    if (minLength == maxLength && minLength > 0) {
      params.put(CommonConstants.FLINK_MIN_LENGTH, String.valueOf(minLength));
      params.put(CommonConstants.FLINK_MAX_LENGTH, String.valueOf(maxLength));
    } else if (maxLength != Integer.MAX_VALUE) {
      params.put(CommonConstants.FLINK_MAX_LENGTH, String.valueOf(maxLength));
    }
    if (!params.isEmpty()) {
      addMetaParams(builder, params);
    }
    builder.setType(type);
    return builder;
  }

  private static FieldDescriptorProto.Builder createTimeFieldDescriptor(
      int precision, Set<String> dependencies, FieldDescriptorProto.Builder builder) {
    if (precision < 0 || precision > 9) {
      throw new ValidationException(
          "TIME precision must be in [0, 9], got " + precision);
    }
    builder.setType(Type.TYPE_MESSAGE);
    builder.setTypeName(makeItTopLevelScoped(CommonConstants.PROTOBUF_TIME_TYPE));
    if (precision != 9) {
      addMetaParam(builder, CommonConstants.FLINK_PRECISION_PROP, String.valueOf(precision));
    }
    dependencies.add(CommonConstants.PROTOBUF_TIME_LOCATION);
    return builder;
  }

  private static FieldDescriptorProto.Builder createTimestampFieldDescriptor(
      int precision,
      boolean isLocalTimestamp,
      Set<String> dependencies,
      FieldDescriptorProto.Builder builder) {
    if (precision < 0 || precision > 9) {
      throw new ValidationException(
          (isLocalTimestamp ? "TIMESTAMP" : "TIMESTAMP_LTZ")
              + " precision must be in [0, 9], got " + precision);
    }
    builder.setType(Type.TYPE_MESSAGE);
    builder.setTypeName(makeItTopLevelScoped(CommonConstants.PROTOBUF_TIMESTAMP_TYPE));
    final Map<String, String> params = new LinkedHashMap<>();
    if (precision != 9) {
      params.put(CommonConstants.FLINK_PRECISION_PROP, String.valueOf(precision));
    }
    if (isLocalTimestamp) {
      params.put(CommonConstants.FLINK_TYPE_PROP, CommonConstants.FLINK_TYPE_TIMESTAMP);
    }
    addMetaParams(builder, params);
    dependencies.add(CommonConstants.PROTOBUF_TIMESTAMP_LOCATION);
    return builder;
  }

  private static String makeItTopLevelScoped(String type) {
    return "." + type;
  }

  /**
   * If the field has a default value, encode it and store under Meta.params
   * with key "connect.default".
   */
  private static void applyDefaultIfPresent(
      FieldDescriptorProto.Builder fieldBuilder, Field field) {
    // Skip when value is null — Meta.params is Map<String,String> with no
    // unambiguous null sentinel; emitting "" would conflict with valid empty
    // string defaults. Round-trip silently loses the explicit-null distinction.
    if (field.hasDefaultValue() && field.getDefaultValue() != null) {
      String encoded = ProtoDefaultValueConverter.toProtoValue(
          field.getSchema(), field.getDefaultValue());
      addMetaParam(fieldBuilder, CommonConstants.LOGICAL_DEFAULT_PROP, encoded);
    }
  }

  /**
   * Recursively registers every message and enum (top-level + nested) in the
   * given external file. The first FQN seen for {@code sourceRefName}
   * triggers a renamed FileDescriptor clone (renamed to {@code sourceRefName}
   * itself, so the emitted {@code import "..."} matches the SR ref name);
   * subsequent FQNs from the same source share that rename so the proto
   * compiler doesn't see duplicate symbols across multiple imported clones.
   */
  private static void registerExternalFile(
      FileDescriptor file, String sourceRefName,
      FromLogicalContext<GenericDescriptor> ctx) {
    for (Descriptor msg : file.getMessageTypes()) {
      registerExternalMessage(msg, file, sourceRefName, ctx);
    }
    for (EnumDescriptor enm : file.getEnumTypes()) {
      registerExternalType(enm, enm.getFullName(), file, sourceRefName, ctx);
    }
  }

  private static void registerExternalMessage(
      Descriptor msg, FileDescriptor file, String sourceRefName,
      FromLogicalContext<GenericDescriptor> ctx) {
    registerExternalType(msg, msg.getFullName(), file, sourceRefName, ctx);
    for (Descriptor nested : msg.getNestedTypes()) {
      registerExternalMessage(nested, file, sourceRefName, ctx);
    }
    for (EnumDescriptor enm : msg.getEnumTypes()) {
      registerExternalType(enm, enm.getFullName(), file, sourceRefName, ctx);
    }
  }

  private static void registerExternalType(
      GenericDescriptor descriptor, String fullName,
      FileDescriptor file, String sourceRefName,
      FromLogicalContext<GenericDescriptor> ctx) {
    ctx.putConverted(fullName, descriptor);
    // If this source ref already produced a rename, reuse it. The rename
    // target is the SchemaReference.name itself — that's what the SR client
    // uses to resolve imports against the references list, so the emitted
    // `import` statement MUST match it. (Renaming to the type FQN would
    // produce an import string that the SR side can't resolve when the
    // ref name is e.g. a filename like `peers.proto`.)
    String existingRename = ctx.externalFileAlreadyRenamedTo(sourceRefName);
    if (existingRename != null) {
      ctx.registerExternalImport(fullName, existingRename);
      return;
    }
    try {
      FileDescriptorProto renamedProto = file.toProto()
          .toBuilder().setName(sourceRefName).build();
      FileDescriptor renamedFd = FileDescriptor.buildFrom(
          renamedProto, file.getDependencies().toArray(new FileDescriptor[0]));
      ctx.addExternalDependency(renamedFd);
      ctx.markExternalFileRenamed(sourceRefName, sourceRefName);
      ctx.registerExternalImport(fullName, sourceRefName);
    } catch (DescriptorValidationException e) {
      throw new ValidationException(
          "Failed to build external type descriptor for " + fullName, e);
    }
  }

  /**
   * True for LT types that are emitted as proto {@code repeated}/{@code map}
   * fields. Such types can't appear directly inside a {@code oneof} and must
   * be wrapped in a struct.
   */
  private static boolean isCompositeRepeated(Schema schema) {
    Schema.Type t = schema.getType();
    return t == Schema.Type.ARRAY || t == Schema.Type.MAP
        || t == Schema.Type.MULTISET;
  }

  private static FieldDescriptorProto.Builder wrapRepeatedType(
      Schema repeatedType,
      String fieldName,
      String comment,
      int fieldIndex,
      List<DescriptorProto> nestedRows,
      Set<String> dependencies,
      FromLogicalContext<GenericDescriptor> ctx) {
    final FieldDescriptorProto.Builder builder = FieldDescriptorProto.newBuilder();
    final String typeName = toMessageName(
        fieldName, CommonConstants.FLINK_REPEATED_WRAPPER_SUFFIX, ctx);

    Schema wrapperStruct = Schema.createStruct(Collections.singletonList(
        new Field(CommonConstants.FLINK_WRAPPER_FIELD_NAME,
            repeatedType.setNullable(false), 0,
            null, false, null, null, null)))
        .setNullable(false);

    nestedRows.add(fromStructType(wrapperStruct, typeName, dependencies, ctx));
    addMetaParam(builder, CommonConstants.FLINK_WRAPPER, "true");
    addComment(builder, comment);
    builder.setType(Type.TYPE_MESSAGE);
    builder.setTypeName(typeName);
    builder.setNumber(fieldIndex);
    builder.setName(fieldName);
    return builder;
  }

  /**
   * Wraps a UNION in a synthesized {@code <UpperCamelField>OneofWrapper { value: UNION }}
   * struct. Proto3 forbids {@code oneof} directly inside another {@code oneof}, and
   * a {@code oneof} has no value-typed representation that could appear as a bare
   * {@code repeated} element. Wrapping rehosts the UNION as a regular field of a
   * struct, where {@code fromStructType} re-emits it as a legal {@code oneof}.
   *
   * <p>Reached from two paths:
   * <ul>
   *   <li>UNION as a oneof branch (UNION-in-UNION) — via {@code fromStructType}'s
   *       branch loop calling {@code fromField}.</li>
   *   <li>UNION as a repeated element (ARRAY-of-UNION) — via
   *       {@code createRepeatedNotNull} calling {@code fromField}.</li>
   * </ul>
   */
  private static FieldDescriptorProto.Builder wrapOneofType(
      Schema unionType,
      String fieldName,
      String comment,
      int fieldIndex,
      List<DescriptorProto> nestedRows,
      Set<String> dependencies,
      FromLogicalContext<GenericDescriptor> ctx) {
    final FieldDescriptorProto.Builder builder = FieldDescriptorProto.newBuilder();
    final String typeName = toMessageName(
        fieldName, CommonConstants.FLINK_ONEOF_WRAPPER_SUFFIX, ctx);

    Schema wrapperStruct = Schema.createStruct(Collections.singletonList(
        new Field(CommonConstants.FLINK_WRAPPER_FIELD_NAME,
            unionType, 0, null, false, null, null, null)))
        .setNullable(false);

    nestedRows.add(fromStructType(wrapperStruct, typeName, dependencies, ctx));
    addMetaParam(builder, CommonConstants.FLINK_WRAPPER, "true");
    addComment(builder, comment);
    builder.setType(Type.TYPE_MESSAGE);
    builder.setTypeName(typeName);
    builder.setNumber(fieldIndex);
    builder.setName(fieldName);
    return builder;
  }

  private static FieldDescriptorProto.Builder createRepeatedNotNull(
      String fieldName,
      String fieldComment,
      int fieldIndex,
      List<DescriptorProto> nestedRows,
      List<EnumDescriptorProto> nestedEnums,
      Set<String> dependencies,
      Schema elementType,
      FromLogicalContext<GenericDescriptor> ctx) {
    final FieldDescriptorProto.Builder builder;
    boolean isCollection = isCompositeRepeated(elementType);
    // UNION has no value-typed representation in proto — a oneof needs a
    // struct context, so a UNION can't sit as a bare `repeated` element.
    // Wrap at the repeated-element position so the surrounding-slot rule
    // (_ElementWrapper) is preserved. ENUM is handled directly by fromField
    // (which synthesizes a nested enum and emits a typeName), so it doesn't
    // need wrapping — proto allows `repeated <enum>` natively.
    boolean needsUnionWrap = elementType.getType() == Schema.Type.UNION;
    if (isCollection || elementType.isNullable() || needsUnionWrap) {
      final String typeName = toMessageName(
          fieldName, CommonConstants.FLINK_ELEMENT_WRAPPER_SUFFIX, ctx);
      Schema wrapperStruct = Schema.createStruct(Collections.singletonList(
          new Field(CommonConstants.FLINK_WRAPPER_FIELD_NAME,
              elementType, 0, null, false, null, null, null)))
          .setNullable(false);
      nestedRows.add(fromStructType(wrapperStruct, typeName, dependencies, ctx));
      builder = FieldDescriptorProto.newBuilder();
      addMetaParam(builder, CommonConstants.FLINK_WRAPPER, "true");
      addComment(builder, fieldComment);
      builder.setType(Type.TYPE_MESSAGE);
      builder.setTypeName(typeName);
      builder.setNumber(fieldIndex);
      builder.setName(fieldName);
    } else {
      builder = fromField(
          elementType, fieldName, fieldComment, fieldIndex,
          nestedRows, nestedEnums, dependencies, ctx);
    }
    builder.setLabel(Label.LABEL_REPEATED);
    builder.clearProto3Optional();
    return builder;
  }

  private static void addComment(FieldDescriptorProto.Builder builder, String fieldComment) {
    if (fieldComment != null) {
      builder.mergeOptions(createMetaForComment(fieldComment));
    }
  }

  private static void addMetaParam(
      FieldDescriptorProto.Builder builder, String paramKey, String paramValue) {
    addMetaParams(builder, Collections.singletonMap(paramKey, paramValue));
  }

  private static void addMetaParams(
      FieldDescriptorProto.Builder builder, Map<String, String> params) {
    if (params.isEmpty()) {
      return;
    }
    final Map<String, String> extendedParams = new LinkedHashMap<>();
    extendedParams.put(
        CommonConstants.FLINK_PROPERTY_VERSION,
        CommonConstants.FLINK_PROPERTY_CURRENT_VERSION);
    extendedParams.putAll(params);
    builder.mergeOptions(
        FieldOptions.newBuilder()
            .setExtension(
                MetaProto.fieldMeta,
                Meta.newBuilder().putAllParams(extendedParams).build())
            .build());
  }

  private static FieldDescriptorProto.Builder createNotNullMapLikeField(
      String fieldName,
      String comment,
      int fieldIndex,
      List<DescriptorProto> nestedRows,
      Set<String> dependencies,
      Schema keyType,
      Schema valueType,
      FromLogicalContext<GenericDescriptor> ctx) {
    final FieldDescriptorProto.Builder builder = FieldDescriptorProto.newBuilder();
    // Match protoc's UpperCamelCase convention for map-entry messages
    // (e.g., "foo_bar" → "FooBarEntry"). Same naming rule as the other
    // synthesized message types.
    final String typeName = toMessageName(fieldName, CommonConstants.MAP_ENTRY_SUFFIX, ctx);

    Schema mapEntryStruct = Schema.createStruct(Arrays.asList(
        new Field(CommonConstants.KEY_FIELD, keyType, 0,
            null, false, null, null, null),
        new Field(CommonConstants.VALUE_FIELD, valueType, 1,
            null, false, null, null, null)))
        .setNullable(false);

    final DescriptorProto mapDescriptor =
        fromStructType(mapEntryStruct, typeName, dependencies, ctx);
    addComment(builder, comment);
    nestedRows.add(mapDescriptor);
    builder.setType(Type.TYPE_MESSAGE);
    builder.setTypeName(typeName);
    builder.setNumber(fieldIndex);
    builder.setLabel(Label.LABEL_REPEATED);
    builder.setName(fieldName);
    builder.clearProto3Optional();
    return builder;
  }

  private static FieldOptions createMetaForComment(String comment) {
    return FieldOptions.newBuilder()
        .setExtension(MetaProto.fieldMeta, Meta.newBuilder().setDoc(comment).build())
        .build();
  }

  private static void addMessageMeta(DescriptorProto.Builder builder, Schema schema) {
    Meta.Builder metaBuilder = buildSchemaMeta(schema);
    if (metaBuilder != null) {
      builder.setOptions(MessageOptions.newBuilder()
          .setExtension(MetaProto.messageMeta, metaBuilder.build())
          .build());
    }
  }

  private static void addEnumMeta(EnumDescriptorProto.Builder builder, Schema schema) {
    Meta.Builder metaBuilder = buildSchemaMeta(schema);
    if (metaBuilder != null) {
      builder.setOptions(EnumOptions.newBuilder()
          .setExtension(MetaProto.enumMeta, metaBuilder.build())
          .build());
    }
  }

  private static void addFieldTagsAndParams(
      FieldDescriptorProto.Builder builder, Field field) {
    if (field.getTags().isEmpty() && field.getParams().isEmpty()
        && field.getRules().isEmpty()) {
      return;
    }
    Meta.Builder metaBuilder = Meta.newBuilder();
    if (!field.getTags().isEmpty()) {
      metaBuilder.addAllTags(field.getTags());
    }
    if (!field.getParams().isEmpty()) {
      Map<String, String> stringParams = new LinkedHashMap<>();
      field.getParams().forEach((k, v) -> stringParams.put(k, String.valueOf(v)));
      metaBuilder.putAllParams(stringParams);
    }
    addRulesToMeta(metaBuilder, field.getRules());
    builder.mergeOptions(
        FieldOptions.newBuilder()
            .setExtension(MetaProto.fieldMeta, metaBuilder.build())
            .build());
  }

  /**
   * Convert LT {@link io.confluent.kafka.schemaregistry.type.logical.Rule}
   * instances into proto {@link MetaProto.Rule} entries on the given
   * {@link Meta.Builder}. Optional fields ({@code name}, {@code doc}) are
   * only set when non-null so the wire form stays compact.
   */
  private static void addRulesToMeta(
      Meta.Builder metaBuilder,
      java.util.List<io.confluent.kafka.schemaregistry.type.logical.Rule> rules) {
    for (io.confluent.kafka.schemaregistry.type.logical.Rule r : rules) {
      MetaProto.Rule.Builder rb = MetaProto.Rule.newBuilder()
          .setExpr(r.getExpr())
          .setSql(r.getSql());
      if (r.getName() != null) {
        rb.setName(r.getName());
      }
      if (r.getDoc() != null) {
        rb.setDoc(r.getDoc());
      }
      metaBuilder.addRules(rb.build());
    }
  }

  private static Meta.Builder buildEnumValueMeta(EnumValue ev) {
    boolean hasMeta = false;
    Meta.Builder metaBuilder = Meta.newBuilder();
    if (ev.getDoc() != null) {
      metaBuilder.setDoc(ev.getDoc());
      hasMeta = true;
    }
    if (!ev.getParams().isEmpty()) {
      Map<String, String> stringParams = new LinkedHashMap<>();
      ev.getParams().forEach((k, v) -> stringParams.put(k, String.valueOf(v)));
      metaBuilder.putAllParams(stringParams);
      hasMeta = true;
    }
    return hasMeta ? metaBuilder : null;
  }

  private static Meta.Builder buildSchemaMeta(Schema schema) {
    boolean hasMeta = false;
    Meta.Builder metaBuilder = Meta.newBuilder();
    if (schema.getDoc() != null) {
      metaBuilder.setDoc(schema.getDoc());
      hasMeta = true;
    }
    if (!schema.getTags().isEmpty()) {
      metaBuilder.addAllTags(schema.getTags());
      hasMeta = true;
    }
    if (!schema.getParams().isEmpty()) {
      Map<String, String> stringParams = new LinkedHashMap<>();
      schema.getParams().forEach((k, v) -> stringParams.put(k, String.valueOf(v)));
      metaBuilder.putAllParams(stringParams);
      hasMeta = true;
    }
    if (!schema.getRules().isEmpty()) {
      addRulesToMeta(metaBuilder, schema.getRules());
      hasMeta = true;
    }
    return hasMeta ? metaBuilder : null;
  }

}
