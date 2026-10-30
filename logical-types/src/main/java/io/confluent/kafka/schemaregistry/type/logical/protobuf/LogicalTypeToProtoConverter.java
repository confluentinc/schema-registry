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
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaReference;
import com.squareup.wire.schema.internal.parser.MessageElement;
import com.squareup.wire.schema.internal.parser.ProtoFileElement;
import com.squareup.wire.schema.internal.parser.TypeElement;
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
import com.google.protobuf.DescriptorProtos.FileOptions;
import com.google.protobuf.DescriptorProtos.MessageOptions;
import com.google.protobuf.DescriptorProtos.OneofDescriptorProto;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.DescriptorValidationException;
import com.google.protobuf.Descriptors.EnumDescriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
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
import java.util.LinkedHashSet;
import java.util.Collections;
import java.util.EnumMap;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Converts logical type {@link Schema} to Protobuf {@link Descriptor}.
 */
public class LogicalTypeToProtoConverter {

  /** Lowest field number Protobuf permits. */
  private static final int MIN_FIELD_NUMBER = 1;
  /** Highest field number Protobuf permits (2^29 - 1). */
  private static final int MAX_FIELD_NUMBER = 536870911;
  /** First number in the range Protobuf reserves for its own implementation. */
  private static final int IMPL_RESERVED_FIRST_FIELD_NUMBER = 19000;
  /** Last number in that range, inclusive. */
  private static final int IMPL_RESERVED_LAST_FIELD_NUMBER = 19999;

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
   * The params that pin the name of the message or enum the root struct's regular field
   * {@code fieldName} generates, as {@code fromLogicalType} names it now: put on the field before
   * renaming it, they keep the name, which the Protobuf compatibility checks compare. Empty for a
   * field that generates none.
   */
  public static Map<String, Object> typeNameParams(
      LogicalType logicalType, String rowName, String fieldName) {
    String name = ProtoToLogicalTypeConverter.pinTypeName(rootMessage(logicalType, rowName),
        fieldName);
    return name != null && !isNamedTypeRef(logicalType, fieldName)
        ? Collections.singletonMap(ProtoTypeNames.NAME, name) : Collections.emptyMap();
  }

  /**
   * The params that pin the names of the messages and enums the members of the root struct's
   * UNION field {@code fieldName} generate, by branch name, as {@link #typeNameParams} for a field.
   */
  public static Map<String, Map<String, Object>> branchTypeNameParams(
      LogicalType logicalType, String rowName, String fieldName) {
    Map<String, Map<String, Object>> params = new LinkedHashMap<>();
    ProtoToLogicalTypeConverter.pinBranchTypeNames(rootMessage(logicalType, rowName), fieldName)
        .forEach((branch, name) -> params.put(branch,
            Collections.singletonMap(ProtoTypeNames.NAME, name)));
    return params;
  }

  private static Descriptor rootMessage(LogicalType logicalType, String rowName) {
    // The writer emits the root message first.
    return fromLogicalType(logicalType, rowName).toDescriptor().getFile().getMessageTypes().get(0);
  }

  private static boolean isNamedTypeRef(LogicalType logicalType, String fieldName) {
    Schema root = logicalType.getRootSchema();
    return root.getType() == Schema.Type.STRUCT && root.getField(fieldName) != null
        && root.getField(fieldName).getSchema().getType() == Schema.Type.NAMED_TYPE_REF;
  }

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

    // A named root (bare STRUCT/ENUM carrying LogicalType.name) emits as the file's root message
    // under its own simple name, taking precedence over the caller's rowName. The namespace lives
    // on the file's package declaration (logicalType.getNamespace()), so only the simple name is
    // used here — mirroring the NAMED_TYPE_REF-root resolution below.
    if (logicalType.getName() != null
        && (schema.getType() == Schema.Type.STRUCT || schema.getType() == Schema.Type.ENUM)) {
      rowName = logicalType.getName();
    }

    // A root over several messages is written as each of them, in order, with no root param: a
    // file of several messages naming no root is read as all of them.
    List<String> messageOrder = null;
    if (ProtoToLogicalTypeConverter.isMultiMessageRoot(schema)) {
      messageOrder = multiMessageOrder(schema, logicalType);
      schema = Schema.createNamedTypeRef(messageOrder.get(0)).setNullable(false);
    }

    // External-only public import: the registered root is a NAMED_TYPE_REF to
    // a type defined in an external (referenced) schema, with no local types.
    // Proto's `import public` is the natural representation — produces an
    // otherwise-empty proto file that publicly imports the referenced type,
    // allowing downstream consumers to reach it via this schema's name.
    // A reader keeps an external root's body in namedTypes too, so external counts as absent.
    if (schema.getType() == Schema.Type.NAMED_TYPE_REF) {
      String fullName = schema.getQualifiedName();
      String importName =
          !logicalType.getNamedTypes().containsKey(fullName) || logicalType.isExternal(fullName)
              ? declaringImport(logicalType, fullName) : null;
      if (importName != null) {
        // The file only imports, so it has nowhere to declare a local type.
        if (!logicalType.getLocalNamedTypes().isEmpty()) {
          throw new ValidationException("Root " + fullName + " is a referenced type, imported "
              + "publicly, so the schema cannot declare its own types too: "
              + logicalType.getLocalNamedTypes().keySet());
        }
        // A schema's root is found through one public import, so not through a re-export.
        if (declaration(logicalType.getResolvedReferences().get(importName), logicalType,
            fullName) != Declaration.OWN) {
          throw new ValidationException("Root " + fullName + " would be imported from "
              + importName + ", which only re-exports it, so the schema could not resolve it; "
              + "reference the file that declares it");
        }
        return buildPublicImportFile(logicalType, fullName, importName, version);
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

    // If root is a wrappable leaf type, publicly import the well-known proto
    // file defining its message, as for an external root. No local message
    // definition, no rowName usage, no namedTypes/references processing.
    if (ROOT_WRAPPER_DESCRIPTORS.containsKey(schema.getType())) {
      validateWrappableRoot(logicalType, schema);
      Descriptor wrapper = ROOT_WRAPPER_DESCRIPTORS.get(schema.getType());
      return applyEditionMetadata(publicImportFile(wrapper.getFile(), wrapper.getFullName(),
          logicalType.getNamespace(), Collections.emptyList()), version);
    }

    // A proto file declares a single `package`; every LOCAL named type lives in
    // it. (Externals live in imported files with their own packages — they're
    // emitted as `import` statements via the resolvedReferences pre-walk
    // below.) Validate against locals only.
    Map<String, Schema> localNamedTypes = logicalType.getLocalNamedTypes();
    if (messageOrder != null) {
      Map<String, Schema> ordered = new LinkedHashMap<>();
      for (String message : messageOrder) {
        ordered.put(message, localNamedTypes.get(message));
      }
      ordered.putAll(localNamedTypes);
      localNamedTypes = ordered;
    }
    validateNamespacesMatchFilePackage(
        localNamedTypes, logicalType.getNamespace());

    // Reject cross-namespace simple-name collisions before building. Proto
    // type names at file level must be unique simple identifiers.
    validateNoSimpleNameCollisions(
        localNamedTypes, rootResolvedFromFqn, rowName);

    FromLogicalContext<GenericDescriptor> ctx =
        new FromLogicalContext<>(logicalType, version);
    ctx.setTypeNames(new ProtoTypeNames());

    // Pre-populate cache with all named types (messages and enums, including
    // nested) from each external schema. Each registered type gets a renamed
    // FileDescriptor clone so it can be imported by its qualified name. The
    // source-ref name (the resolvedReferences key) is the dedup identifier:
    // two types from the same source file would produce duplicate-symbol
    // errors if each got its own renamed clone, so subsequent FQNs from a
    // single source share the first FQN's rename.
    // A file reached both as itself and through files re-exporting it is registered once, so its
    // types are declared by one import: a direct reference first, then a file over a re-export.
    Set<String> direct = new HashSet<>();
    logicalType.getReferences().forEach(reference -> direct.add(reference.getName()));
    Map<String, String> sourceByFile = new LinkedHashMap<>();
    Map<String, FileDescriptor> fileBySource = new HashMap<>();
    for (Map.Entry<String, String> entry :
            logicalType.getResolvedReferences().entrySet()) {
      ProtobufSchema parsed = new ProtobufSchema(entry.getValue(),
          logicalType.getReferences(), logicalType.getResolvedReferences(),
          null, null, null, null);
      FileDescriptor file = fileOf(parsed);
      fileBySource.put(entry.getKey(), file);
      // A re-export's types are declared by the imported file, named by its reference
      String declaring = isReExport(parsed) ? file.getName() : entry.getKey();
      sourceByFile.merge(declaring, entry.getKey(), (chosen, candidate) ->
          sourceRank(candidate, direct, logicalType) < sourceRank(chosen, direct, logicalType)
              ? candidate : chosen);
    }
    for (String source : sourceByFile.values()) {
      registerExternalFile(fileBySource.get(source), source, ctx);
    }

    ProtobufSchema result = new ProtobufSchema(
        buildFile(schema, rowName, logicalType.getNamespace(),
            localNamedTypes, rootResolvedFromFqn, messageOrder == null, logicalType, ctx),
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
   * The file a referenced schema's first type is declared in, found by that type's exact name:
   * a lookup by name also finds an imported message by its short name, such as one sharing the
   * name of an enum, which is all a file of only enums declares.
   */
  private static FileDescriptor fileOf(ProtobufSchema parsed) {
    String name = parsed.name();
    Descriptor message = parsed.toDescriptor();
    if (message != null && message.getFullName().equals(name)) {
      return message.getFile();
    }
    EnumDescriptor enumType = parsed.toDynamicSchema().getEnumDescriptor(name);
    if (enumType != null && enumType.getFullName().equals(name)) {
      return enumType.getFile();
    }
    throw new ValidationException("Referenced Protobuf schema declares no type " + name);
  }

  private static boolean isReExport(ProtobufSchema parsed) {
    return parsed.rawSchema().getTypes().isEmpty();
  }

  /** A direct reference before a resolved one, and a file declaring types before a re-export. */
  private static int sourceRank(String source, Set<String> direct, LogicalType logicalType) {
    boolean reExport = isReExport(new ProtobufSchema(
        logicalType.getResolvedReferences().get(source)));
    return (direct.contains(source) ? 0 : 2) + (reExport ? 1 : 0);
  }

  /**
   * The reference a root is imported from: the one direct reference that declares it, or failing
   * any, the one resolved file that does. A file declaring it itself is taken over one that only
   * re-exports it; several of either are ambiguous, since each could be what was imported.
   */
  private static String declaringImport(LogicalType logicalType, String fullName) {
    Set<String> direct = new LinkedHashSet<>();
    logicalType.getReferences().forEach(reference -> direct.add(reference.getName()));
    String found = onlyDeclaring(direct, logicalType, fullName);
    if (found == null) {
      Set<String> others = new LinkedHashSet<>(logicalType.getResolvedReferences().keySet());
      others.removeAll(direct);
      found = onlyDeclaring(others, logicalType, fullName);
    }
    return found;
  }

  private static String onlyDeclaring(Set<String> names, LogicalType logicalType,
      String fullName) {
    Map<Declaration, List<String>> byKind = new EnumMap<>(Declaration.class);
    for (String name : names) {
      String text = logicalType.getResolvedReferences().get(name);
      if (text != null) {
        byKind.computeIfAbsent(declaration(text, logicalType, fullName),
            kind -> new ArrayList<>()).add(name);
      }
    }
    List<String> declaring = byKind.containsKey(Declaration.OWN)
        ? byKind.get(Declaration.OWN)
        : byKind.getOrDefault(Declaration.RE_EXPORT, Collections.emptyList());
    if (declaring.size() > 1) {
      throw new ValidationException("Root " + fullName + " is declared by more than one "
          + "referenced file, " + declaring + ", so which one it is imported from is ambiguous");
    }
    return declaring.isEmpty() ? null : declaring.get(0);
  }

  /** How a referenced file declares a message, if it does. */
  private enum Declaration {
    // Not merely by importing a file that declares it
    NONE,
    OWN,
    // As the root of a file of only public imports
    RE_EXPORT
  }

  private static Declaration declaration(String text, LogicalType logicalType, String fullName) {
    try {
      // A file's own text tells its own messages; only a re-export needs its dependencies.
      ProtoFileElement file = new ProtobufSchema(text).rawSchema();
      String packageName = file.getPackageName();
      String prefix = packageName == null || packageName.isEmpty() ? "" : packageName + ".";
      for (TypeElement type : file.getTypes()) {
        if (declaresMessage(type, prefix, fullName)) {
          return Declaration.OWN;
        }
      }
      if (!file.getTypes().isEmpty() || file.getPublicImports().isEmpty()) {
        return Declaration.NONE;
      }
      ProtobufSchema schema = new ProtobufSchema(text, logicalType.getReferences(),
          logicalType.getResolvedReferences(), null, null, null, null);
      return fullName.equals(schema.name()) ? Declaration.RE_EXPORT : Declaration.NONE;
    } catch (RuntimeException e) {
      // a file that does not parse declares nothing to import
      return Declaration.NONE;
    }
  }

  private static boolean declaresMessage(TypeElement type, String prefix, String fullName) {
    String name = prefix + type.getName();
    return type instanceof MessageElement && name.equals(fullName)
        || type.getNestedTypes().stream()
            .anyMatch(nested -> declaresMessage(nested, name + ".", fullName));
  }

  /**
   * Build a proto file that publicly imports a single externally-referenced
   * type via {@code import public}. Result has zero local messages/enums;
   * consumers resolve the root through {@code schema.toDescriptor()} which
   * uses {@link ProtobufSchema#name()}'s public-import fall-through.
   *
   * <p>Matches the behavior expected when a script's registered root IS the
   * external type (e.g. {@code TYPE com.Foo;} where {@code com.Foo} is not
   * locally declared), with no local body to define.
   */
  private static ProtobufSchema buildPublicImportFile(
      LogicalType logicalType, String externalTypeFullName, String importName,
      LogicalTypeVersion version) {
    String externalProtoText = logicalType.getResolvedReferences().get(importName);
    ProtobufSchema externalSchema = new ProtobufSchema(
        externalProtoText,
        logicalType.getReferences(),
        logicalType.getResolvedReferences(),
        null, null, null, null);
    FileDescriptor externalFile = externalSchema.toDescriptor().getFile();

    String externalFileName = importName;
    try {
      FileDescriptor renamedExternal = FileDescriptor.buildFrom(
          externalFile.toProto().toBuilder().setName(externalFileName).build(),
          externalFile.getDependencies().toArray(new FileDescriptor[0]));
      return applyEditionMetadata(publicImportFile(renamedExternal, externalTypeFullName,
          logicalType.getNamespace(), logicalType.getReferences()), version);
    } catch (DescriptorValidationException e) {
      throw new ValidationException(
          "Failed to build public-import proto for '"
              + externalTypeFullName + "'", e);
    }
  }

  /**
   * An otherwise-empty file publicly importing the root's file. The root is recorded in the
   * file_meta params unless that file declares at most one message and
   * {@link ProtobufSchema#name()} picks it anyway: a wrapper of several messages naming no root is
   * read as all of them.
   */
  private static ProtobufSchema publicImportFile(FileDescriptor imported, String rootFullName,
      String packageName, List<SchemaReference> references) {
    FileDescriptorProto.Builder fileBuilder = FileDescriptorProto.newBuilder()
        .setSyntax("proto3")
        .addDependency(imported.getName())
        .addPublicDependency(0);
    if (packageName != null && !packageName.isEmpty()) {
      fileBuilder.setPackage(packageName);
    }
    try {
      ProtobufSchema schema = new ProtobufSchema(FileDescriptor.buildFrom(
          fileBuilder.build(), new FileDescriptor[] {imported}), references);
      if (schema.name().equals(rootFullName) && imported.getMessageTypes().size() < 2) {
        return schema;
      }
      fileBuilder.addDependency(MetaProto.getDescriptor().getName())
          .setOptions(FileOptions.newBuilder().setExtension(MetaProto.fileMeta,
              Meta.newBuilder().putParams(ProtobufSchema.ROOT_KEY, rootFullName).build()));
      schema = new ProtobufSchema(FileDescriptor.buildFrom(fileBuilder.build(),
          new FileDescriptor[] {imported, MetaProto.getDescriptor()}), references);
      // Fails for a root that isn't a message, such as an enum beside messages
      schema.name();
      return schema;
    } catch (DescriptorValidationException | IllegalArgumentException e) {
      throw new ValidationException(
          "Failed to build public-import proto for '" + rootFullName + "'", e);
    }
  }

  private static String simpleName(String fqn) {
    int dot = fqn.lastIndexOf('.');
    return dot < 0 ? fqn : fqn.substring(dot + 1);
  }

  /**
   * The messages a multi-message root lists, in order: every top-level struct the schema declares
   * and nothing else, as the file it writes is read back.
   */
  public static List<String> multiMessageOrder(Schema root, LogicalType logicalType) {
    Map<String, Schema> local = logicalType.getLocalNamedTypes();
    Set<String> topLevel = new LinkedHashSet<>();
    local.forEach((name, body) -> {
      if (body.getType() == Schema.Type.STRUCT
          && LogicalType.parentOf(name, local.keySet()) == null) {
        topLevel.add(name);
      }
    });
    List<String> order = new ArrayList<>();
    for (Field field : root.getFields()) {
      Schema member = field.getSchema();
      if (member.getType() != Schema.Type.NAMED_TYPE_REF
          || !topLevel.contains(member.getQualifiedName())
          || order.contains(member.getQualifiedName())) {
        throw new ValidationException("A root over several messages may list only the schema's "
            + "own top-level messages, each once, not field '" + field.getName() + "'");
      }
      order.add(member.getQualifiedName());
    }
    if (!order.containsAll(topLevel) || order.isEmpty()) {
      throw new ValidationException("A root over several messages must list every top-level "
          + "message the schema declares: " + topLevel);
    }
    return order;
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
    String derived = derivedMessageName(fieldName, suffix, ctx.isV1());
    return ((ProtoTypeNames) ctx.getTypeNames()).name(fieldName, suffix, derived);
  }

  // The name toMessageName gives a type when no param pins one or takes it.
  static String derivedMessageName(String fieldName, String suffix, boolean v1) {
    if (v1) {
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
                + "into separate schemas referenced as externals.");
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
      Map<String, Schema> namedTypes, String rootResolvedFromFqn, boolean writeRoot,
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
      // A root sharing its file with other messages is named, as a file of several messages
      // naming no root is read as all of them.
      if (writeRoot && fileBuilder.getMessageTypeCount() > 1) {
        String root = packageName == null || packageName.isEmpty()
            ? rowName : packageName + "." + rowName;
        dependencies.add(MetaProto.getDescriptor().getName());
        fileBuilder.setOptions(FileOptions.newBuilder().setExtension(MetaProto.fileMeta,
            Meta.newBuilder().putParams(ProtobufSchema.ROOT_KEY, root).build()));
      }

      fileBuilder.addAllDependency(dependencies);
      if (packageName != null && !packageName.isEmpty()) {
        fileBuilder.setPackage(packageName);
      }
      final FileDescriptorProto fileDescriptorProto = fileBuilder.build();
      checkDistinctNames(fileDescriptorProto);
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

  /**
   * Refuses a name defined twice in one scope, e.g. the messages of union branch {@code f0.b0} and
   * field {@code f0_b0}, which are both F0B0Row; the descriptor build would only fail opaquely.
   */
  private static void checkDistinctNames(FileDescriptorProto file) {
    final Map<String, String> kinds = new HashMap<>();
    for (DescriptorProto message : file.getMessageTypeList()) {
      claimName(kinds, message.getName(), "message", "the schema", List.of());
    }
    for (EnumDescriptorProto enumType : file.getEnumTypeList()) {
      claimName(kinds, enumType.getName(), "enum", "the schema", List.of());
    }
    for (DescriptorProto message : file.getMessageTypeList()) {
      checkDistinctNames(message, message.getName());
    }
  }

  private static void checkDistinctNames(DescriptorProto message, String scope) {
    final Map<String, String> kinds = new HashMap<>();
    final List<FieldDescriptorProto> fields = message.getFieldList();
    for (FieldDescriptorProto field : fields) {
      claimName(kinds, field.getName(), "field", scope, List.of());
    }
    for (OneofDescriptorProto oneof : message.getOneofDeclList()) {
      claimName(kinds, oneof.getName(), "oneof", scope, List.of());
    }
    for (DescriptorProto nested : message.getNestedTypeList()) {
      claimName(kinds, nested.getName(), "message", scope, fields);
    }
    for (EnumDescriptorProto nested : message.getEnumTypeList()) {
      claimName(kinds, nested.getName(), "enum", scope, fields);
    }
    for (DescriptorProto nested : message.getNestedTypeList()) {
      checkDistinctNames(nested, scope + "." + nested.getName());
    }
  }

  private static void claimName(Map<String, String> kinds, String name, String kind,
      String scope, List<FieldDescriptorProto> fields) {
    final String prior = kinds.putIfAbsent(name, kind);
    if (prior == null) {
      return;
    }
    final List<String> users = fields.stream()
        .filter(f -> f.getTypeName().equals(name) || f.getTypeName().endsWith("." + name))
        .map(FieldDescriptorProto::getName)
        .collect(Collectors.toList());
    throw new ValidationException("Protobuf name '" + name + "' is defined twice in " + scope
        + (prior.equals(kind) ? "" : ", as a " + prior + " and a " + kind)
        + (users.isEmpty() ? "" : " (used by fields " + String.join(", ", users) + ")")
        + "; rename one of the fields it is generated from");
  }

  // A oneof member's field, whose type is named by the member's pin.
  private static FieldDescriptorProto.Builder fromBranch(
      Field union, UnionBranch branch, int branchNumber,
      List<DescriptorProto> nestedRows,
      List<EnumDescriptorProto> nestedEnums,
      Set<String> dependencies,
      FromLogicalContext<GenericDescriptor> ctx) {
    // Nested messages are always qualified, so a member's rename never renames its type.
    final String qualifiedName = union.getName() + "_" + branch.getName();
    ProtoTypeNames names = (ProtoTypeNames) ctx.getTypeNames();
    names.enterBranch(union.getName(), branch.getName());
    try {
      // Proto3 oneof can't contain repeated/map fields. Composite branches
      // (ARRAY/MAP/MULTISET) must be wrapped in a single message field
      // regardless of nullability — fromField only wraps when nullable, so
      // we force the wrap here for the non-nullable composite case.
      if (isCompositeRepeated(branch.getSchema())) {
        return wrapRepeatedType(
            branch.getSchema(),
            qualifiedName,
            branch.getDoc(),
            branchNumber,
            nestedRows,
            dependencies,
            ctx);
      }
      return fromField(
          branch.getSchema(),
          qualifiedName,
          branch.getDoc(),
          branchNumber,
          nestedRows,
          nestedEnums,
          dependencies,
          ctx);
    } finally {
      names.exitBranch();
    }
  }

  // Members share the message's field namespace: a branch name already taken is qualified by the
  // oneof, and kept in logical.branch.
  private static String memberName(
      Field union, UnionBranch branch, Set<String> takenNames, String rowName) {
    if (takenNames.add(branch.getName())) {
      return branch.getName();
    }
    final String qualifiedName = union.getName() + "_" + branch.getName();
    if (!takenNames.add(qualifiedName)) {
      throw new ValidationException("Union branch '" + branch.getName() + "' of '"
          + union.getName() + "' in " + rowName + " cannot be named '" + qualifiedName
          + "', which is already taken");
    }
    return qualifiedName;
  }

  private static EnumDescriptorProto buildEnumDescriptor(
      Schema enumSchema, String enumName, FromLogicalContext<?> ctx) {
    EnumDescriptorProto.Builder enumBuilder = EnumDescriptorProto.newBuilder()
        .setName(enumName);
    List<EnumValue> values = enumSchema.getEnumValues();
    // Pass 1: reserve author-declared numbers so unnumbered values can be assigned around them.
    // Unlike a field number a repeat is legal — it declares an alias — so duplicates are counted,
    // not rejected, and derive allow_alias, which protoc refuses when nothing actually aliases.
    Set<Integer> reservedNumbers = new LinkedHashSet<>();
    boolean hasAlias = false;
    for (EnumValue ev : values) {
      Integer declared = ev.getEnumNumber();
      if (declared != null && !reservedNumbers.add(declared)) {
        hasAlias = true;
      }
    }
    if (hasAlias) {
      enumBuilder.setOptions(EnumOptions.newBuilder().setAllowAlias(true));
    }
    // Pass 2: a declared number is used as-is, an unnumbered value takes the next unreserved one.
    // Cursor starts at 0 because enum numbering is 0-based, unlike fields.
    int[] numberCursor = {0};
    for (EnumValue ev : values) {
      Integer declared = ev.getEnumNumber();
      EnumValueDescriptorProto.Builder evBuilder =
          EnumValueDescriptorProto.newBuilder()
              .setName(ev.getSymbol())
              .setNumber(declared != null
                  ? declared : nextAvailableNumber(numberCursor, reservedNumbers));
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
    // Proto3 requires the first value to be 0 and nothing downstream enforces it — neither
    // EnumDescriptorProto.build nor buildFrom checks. Two ways to land here: the first value
    // declares a non-zero number, or it is unnumbered and a later value claimed 0.
    if (enumBuilder.getValueCount() > 0 && enumBuilder.getValue(0).getNumber() != 0) {
      throw new ValidationException("Proto3 requires the first value of enum '" + enumName
          + "' to have number 0, but '" + enumBuilder.getValue(0).getName() + "' has "
          + enumBuilder.getValue(0).getNumber());
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
    ProtoTypeNames names = (ProtoTypeNames) ctx.getTypeNames();
    names.enter(schema, userNested.stream().map(c -> c.simpleName).collect(Collectors.toList()));
    try {
      return buildStructType(schema, rowName, dependencies, userNested, ctx);
    } finally {
      names.exit();
    }
  }

  // Fields pinned to one name with one body, as a shared hand-written message, define it once.
  private static DescriptorProto buildStructType(
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
    // Pass 1: reserve every author-declared field number so unnumbered fields can be assigned
    // around them, and reject a schema that declares the same number twice — an invalid proto that
    // would otherwise surface as an opaque DescriptorValidationException from FileDescriptor build.
    final Set<Integer> reservedNumbers = new LinkedHashSet<>();
    // Field and oneof names, which a oneof member must not take; members are added as named, and
    // no generated type takes their names either.
    final Set<String> takenNames = new HashSet<>();
    for (Field field : fields) {
      takenNames.add(field.getName());
    }
    final Map<List<String>, String> memberNames = new HashMap<>();
    for (Field field : fields) {
      if (field.getSchema().getType() == Schema.Type.UNION) {
        for (UnionBranch branch : field.getSchema().getBranches()) {
          reserveFieldNumber(branch.getFieldNumber(), reservedNumbers, rowName);
          memberNames.put(Arrays.asList(field.getName(), branch.getName()),
              memberName(field, branch, takenNames, rowName));
        }
      } else {
        reserveFieldNumber(field.getFieldNumber(), reservedNumbers, rowName);
      }
    }
    // Pass 2 (below) assigns numbers: an author-declared number is used as-is; an unnumbered field
    // gets the next number not already reserved, so a positional fallback can never collide with an
    // explicit number. The cursor is a 1-element array so nextFieldNumber can advance it.
    final int[] numberCursor = {1};
    ((ProtoTypeNames) ctx.getTypeNames()).reserve(memberNames.values());

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
          // A oneof member is a Protobuf field: use its recorded number when present, else the next
          // free number — same rule as a regular field.
          final Integer branchDeclared = branch.getFieldNumber();
          final int branchNumber = branchDeclared != null
              ? branchDeclared : nextFieldNumber(numberCursor, reservedNumbers);
          validateFieldNumber(branchNumber, branch.getName(), rowName);
          final String memberName =
              memberNames.get(Arrays.asList(field.getName(), branch.getName()));
          final FieldDescriptorProto.Builder fieldProtoBuilder = fromBranch(
              field, branch, branchNumber, nestedRows, nestedEnums, dependencies, ctx);
          fieldProtoBuilder.setName(memberName);
          // Strip the format-native number (emitted via the native slot) from the branch meta.
          Map<String, Object> branchParams = Schema.stripFormatNativeParams(branch.getParams());
          Map<String, String> stringParams = new LinkedHashMap<>();
          branchParams.forEach((k, v) -> stringParams.put(k, String.valueOf(v)));
          if (!memberName.equals(branch.getName())) {
            stringParams.put(CommonConstants.LOGICAL_BRANCH_PROP, branch.getName());
          }
          addMetaParams(fieldProtoBuilder, stringParams);
          fieldProtoBuilder.setOneofIndex(oneofIndex);
          // Clear optional label for oneof fields
          fieldProtoBuilder.clearProto3Optional();
          builder.addField(fieldProtoBuilder.build());
        }
      } else {
        // Emit the author-declared field number when present (the round-trip fidelity fix),
        // otherwise assign the next number not reserved by an explicit one.
        final Integer declaredNumber = field.getFieldNumber();
        final int effectiveNumber = declaredNumber != null
            ? declaredNumber : nextFieldNumber(numberCursor, reservedNumbers);
        validateFieldNumber(effectiveNumber, field.getName(), rowName);
        final FieldDescriptorProto.Builder fieldProtoBuilder =
            fromField(
                field.getSchema(),
                field.getName(),
                field.getDoc(),
                effectiveNumber,
                nestedRows,
                nestedEnums,
                dependencies,
                ctx);
        if (!ctx.isV1()) {
          addFieldTagsAndParams(fieldProtoBuilder, field);
          applyDefaultIfPresent(fieldProtoBuilder, field,
              ctx.getLogicalType().getNamedTypes());
        }
        builder.addField(fieldProtoBuilder.build());
      }
    }
    builder.addAllNestedType(new LinkedHashSet<>(nestedRows));
    builder.addAllEnumType(new LinkedHashSet<>(nestedEnums));
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

  private static FieldDescriptorProto.Builder fromNamedTypeRef(
      Schema schema, FieldDescriptorProto.Builder builder, Set<String> dependencies,
      FromLogicalContext<GenericDescriptor> ctx) {
    String name = schema.getQualifiedName();
    // Externals live in namedTypes too (lazy-promoted) but must be
    // emitted as imports — so guard local branch with !isExternal.
    if (ctx.hasNamedType(name)
        && !ctx.getLogicalType().isExternal(name)) {
      Schema typeDef = ctx.getNamedType(name);
      builder.setType(typeDef.getType() == Schema.Type.ENUM
          ? Type.TYPE_ENUM : Type.TYPE_MESSAGE);
      // Absolute, or a type generated in this message under the same name would shadow it.
      builder.setTypeName(makeItTopLevelScoped(name));
    } else {
      GenericDescriptor external = ctx.getConverted(name);
      if (external == null) {
        throw new ValidationException("Unknown named type reference: " + name);
      }
      // External type — add import and reference; dispatch on descriptor kind
      builder.setType(external instanceof EnumDescriptor
          ? Type.TYPE_ENUM : Type.TYPE_MESSAGE);
      builder.setTypeName(makeItTopLevelScoped(name));
      // Use the de-duped import name when multiple FQNs share a source
      // file; falls back to the FQN for the legacy 1-type-per-file case.
      String importName = ctx.externalImportNameFor(name);
      dependencies.add(importName != null ? importName : name);
    }
    // A message field is nullable unless marked, as an inline struct is, whatever its label.
    if (!schema.isNullable() && builder.getType() == Type.TYPE_MESSAGE) {
      addMetaParam(builder, CommonConstants.FLINK_NOT_NULL, "true");
    }
    return builder;
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
        // Emitted in both V1 and V2: Flink has a native VariantType (LogicalTypeRoot.VARIANT),
        // so VARIANT is not an LT-only construct.
        builder.setType(Type.TYPE_MESSAGE);
        builder.setTypeName(makeItTopLevelScoped(CommonConstants.PROTOBUF_VARIANT_TYPE));
        dependencies.add(CommonConstants.PROTOBUF_VARIANT_LOCATION);
        return builder;
      case NAMED_TYPE_REF:
        return fromNamedTypeRef(schema, builder, dependencies, ctx);
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
      FieldDescriptorProto.Builder fieldBuilder, Field field, Map<String, Schema> namedTypes) {
    // Skip when value is null — Meta.params is Map<String,String> with no
    // unambiguous null sentinel; emitting "" would conflict with valid empty
    // string defaults. Round-trip silently loses the explicit-null distinction.
    if (field.hasDefaultValue() && field.getDefaultValue() != null
        && !field.hasDerivedDefault()) {
      String encoded = ProtoDefaultValueConverter.toProtoValue(
          field.getSchema(), field.getDefaultValue(), namedTypes);
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

    // The wrapper's payload is NOT NULL only while the wrapper is converted: the type is the
    // caller's, whose logical type would otherwise come back with the collection NOT NULL.
    boolean nullable = repeatedType.isNullable();
    repeatedType.setNullable(false);
    try {
      Schema wrapperStruct = Schema.createStruct(Collections.singletonList(
          new Field(CommonConstants.FLINK_WRAPPER_FIELD_NAME, repeatedType, 0,
              null, false, null, null, null)))
          .setNullable(false);
      nestedRows.add(fromStructType(wrapperStruct, typeName, dependencies, ctx));
    } finally {
      repeatedType.setNullable(nullable);
    }
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
      refuseMapEntryName(elementType, builder.getTypeName(), fieldName, ctx);
    }
    builder.setLabel(Label.LABEL_REPEATED);
    builder.clearProto3Optional();
    return builder;
  }

  // The reader takes repeated messages named ...Entry with only key and value fields for a map's
  // entries, so an array's elements cannot be such a message, pinned or declared.
  private static void refuseMapEntryName(Schema element, String typeName, String fieldName,
      FromLogicalContext<GenericDescriptor> ctx) {
    String name = typeName.substring(typeName.lastIndexOf('.') + 1);
    if (!name.endsWith(CommonConstants.MAP_ENTRY_SUFFIX)) {
      return;
    }
    if (element.getType() == Schema.Type.NAMED_TYPE_REF) {
      // Resolved as fromNamedTypeRef does: a local type, else an external one
      String ref = element.getQualifiedName();
      boolean local = ctx.hasNamedType(ref) && !ctx.getLogicalType().isExternal(ref);
      GenericDescriptor external = local ? null : ctx.getConverted(ref);
      if (local ? isEntryShaped(ctx.getNamedType(ref))
          : external instanceof Descriptor && isEntryShaped((Descriptor) external)) {
        throw new ValidationException("Array field " + fieldName + " cannot hold " + ref
            + ": a message named ..." + CommonConstants.MAP_ENTRY_SUFFIX
            + " with only key and value fields would read back as a map");
      }
    } else if (isEntryShaped(element)) {
      throw new ValidationException("Param " + ProtoTypeNames.NAME + " of array field "
          + fieldName + " cannot end with " + CommonConstants.MAP_ENTRY_SUFFIX + ", got '" + name
          + "': its key and value elements would read back as a map");
    }
  }

  private static boolean isEntryShaped(Schema struct) {
    return struct.getType() == Schema.Type.STRUCT && struct.getFields().size() == 2
        && struct.getField(CommonConstants.KEY_FIELD) != null
        && struct.getField(CommonConstants.VALUE_FIELD) != null;
  }

  // As the reader counts them: a oneof is one part, named key or value as a field would be.
  private static boolean isEntryShaped(Descriptor message) {
    Set<String> parts = new HashSet<>();
    for (FieldDescriptor f : message.getFields()) {
      parts.add(f.getRealContainingOneof() != null
          ? f.getRealContainingOneof().getName() : f.getName());
    }
    return parts.size() == 2 && parts.contains(CommonConstants.KEY_FIELD)
        && parts.contains(CommonConstants.VALUE_FIELD);
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
      // Merge onto any options already set (allow_alias), rather than replacing them.
      builder.setOptions(builder.getOptions().toBuilder()
          .setExtension(MetaProto.enumMeta, metaBuilder.build())
          .build());
    }
  }

  /**
   * Return the next number not already reserved by an author-declared number, advancing
   * {@code cursor} past it. Skipping reserved numbers guarantees a positional fallback never
   * collides with an explicit number, and the monotonic cursor keeps successive fallbacks distinct.
   * Shared by fields and enum values; the caller seeds the cursor with the format's first legal
   * number (1 for fields, 0 for enum values).
   */
  private static int nextAvailableNumber(int[] cursor, Set<Integer> reserved) {
    while (reserved.contains(cursor[0])) {
      cursor[0]++;
    }
    return cursor[0]++;
  }

  /**
   * Positional field number: {@link #nextAvailableNumber} plus a jump over the range Protobuf
   * reserves for itself, so a message long enough to reach 19,000 keeps allocating at 20,000
   * instead of tripping {@link #validateFieldNumber}. Enum allocation calls the generic helper
   * directly — those numbers are legal there.
   */
  private static int nextFieldNumber(int[] cursor, Set<Integer> reserved) {
    int number = nextAvailableNumber(cursor, reserved);
    if (number >= IMPL_RESERVED_FIRST_FIELD_NUMBER && number <= IMPL_RESERVED_LAST_FIELD_NUMBER) {
      cursor[0] = IMPL_RESERVED_LAST_FIELD_NUMBER + 1;
      number = nextAvailableNumber(cursor, reserved);
    }
    return number;
  }

  /**
   * Reject a field number protoc would refuse: outside 1..536,870,911, or inside the
   * 19,000..19,999 range Protobuf reserves for itself. FileDescriptor.buildFrom checks only that a
   * number is positive, so otherwise a bad {@link Schema#PROTOBUF_FIELD_NUMBER} would be stored
   * and fail only under protoc. Applied to the emitted number, so it covers the positional
   * fallback too. Enum numbers are exempt — plain int32, and may be negative.
   */
  private static void validateFieldNumber(int number, String fieldName, String rowName) {
    if (number < MIN_FIELD_NUMBER || number > MAX_FIELD_NUMBER) {
      throw new ValidationException("Field '" + fieldName + "' in struct '" + rowName
          + "' has Protobuf field number " + number + ", outside the legal range "
          + MIN_FIELD_NUMBER + ".." + MAX_FIELD_NUMBER);
    }
    if (number >= IMPL_RESERVED_FIRST_FIELD_NUMBER && number <= IMPL_RESERVED_LAST_FIELD_NUMBER) {
      throw new ValidationException("Field '" + fieldName + "' in struct '" + rowName
          + "' has Protobuf field number " + number + ", which falls in the range "
          + IMPL_RESERVED_FIRST_FIELD_NUMBER + ".." + IMPL_RESERVED_LAST_FIELD_NUMBER
          + " that Protobuf reserves for its own implementation");
    }
  }

  /**
   * Add an author-declared field number to the reserved set, rejecting a duplicate — an invalid
   * proto that would otherwise surface as an opaque DescriptorValidationException. A {@code null}
   * number (unnumbered field or branch) is ignored.
   */
  private static void reserveFieldNumber(Integer declared, Set<Integer> reserved, String rowName) {
    if (declared != null && !reserved.add(declared)) {
      throw new ValidationException("Duplicate " + Schema.PROTOBUF_FIELD_NUMBER + " "
          + declared + " in struct '" + rowName + "'");
    }
  }

  private static void addFieldTagsAndParams(
      FieldDescriptorProto.Builder builder, Field field) {
    // Format-native params are carried through native slots (the field number via
    // FieldDescriptorProto.number), never through the field meta params, so strip them here:
    // protobuf.* because it is emitted natively, avro.* because it is foreign to proto.
    Map<String, Object> userParams = Schema.stripFormatNativeParams(field.getParams());
    if (field.getTags().isEmpty() && userParams.isEmpty()
        && field.getRules().isEmpty()) {
      return;
    }
    Meta.Builder metaBuilder = Meta.newBuilder();
    if (!field.getTags().isEmpty()) {
      metaBuilder.addAllTags(field.getTags());
    }
    if (!userParams.isEmpty()) {
      Map<String, String> stringParams = new LinkedHashMap<>();
      userParams.forEach((k, v) -> stringParams.put(k, String.valueOf(v)));
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
    // Format-native params ride their own native slots (the enum number via
    // EnumValueDescriptorProto.number), never the generic meta params, so strip them here.
    Map<String, Object> userParams = Schema.stripFormatNativeParams(ev.getParams());
    if (!userParams.isEmpty()) {
      Map<String, String> stringParams = new LinkedHashMap<>();
      userParams.forEach((k, v) -> stringParams.put(k, String.valueOf(v)));
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
    // Strip first: a format-native param alone must not manufacture a Meta block.
    Map<String, Object> userParams = Schema.stripFormatNativeParams(schema.getParams());
    if (!userParams.isEmpty()) {
      Map<String, String> stringParams = new LinkedHashMap<>();
      userParams.forEach((k, v) -> stringParams.put(k, String.valueOf(v)));
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
