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

import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.DescriptorProtos.Edition;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import io.confluent.kafka.schemaregistry.type.logical.Schema;
import io.confluent.kafka.schemaregistry.type.logical.Schema.EnumValue;
import io.confluent.kafka.schemaregistry.type.logical.Schema.Field;
import io.confluent.kafka.schemaregistry.type.logical.Schema.UnionBranch;
import io.confluent.kafka.schemaregistry.type.logical.LogicalType;
import io.confluent.kafka.schemaregistry.type.logical.ValidationException;
import io.confluent.kafka.schemaregistry.type.logical.common.ToLogicalContext;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.EnumValueDescriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Descriptors.FieldDescriptor.Type;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.Descriptors.OneofDescriptor;
import io.confluent.protobuf.MetaProto;
import io.confluent.protobuf.MetaProto.Meta;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;

/**
 * Converts Protobuf {@link Descriptor} to logical type {@link Schema}.
 *
 * <p><b>Default-value path emission.</b> ARRAY contributes no index — descending into its element
 * type uses the parent path directly. {@code flink.wrapped} wrapper structs
 * ({@code _RepeatedWrapper}, {@code _ElementWrapper}, {@code _OneofWrapper}) are also transparent:
 * see {@code convertWrapperPayload}, which walks the wrapper's "value" payload (regular field or
 * oneof) directly with the parent {@code indexPath}. All other container conventions follow
 * {@link io.confluent.kafka.schemaregistry.type.logical.LogicalType#getDefaultValues()}.
 */
public class ProtoToLogicalTypeConverter {

  private static final int MAX_LENGTH = Integer.MAX_VALUE;
  private static final int DEFAULT_DECIMAL_PRECISION = 10;
  private static final int DEFAULT_DECIMAL_SCALE = 0;

  public static Schema toRootSchema(final ProtobufSchema schema) {
    return toLogicalType(schema).getRootSchema();
  }

  public static LogicalType toLogicalType(final ProtobufSchema schema) {
    // Re-export-only file: the local proto has no types of its own, just
    // `import public "..."`. Detect this BEFORE toDescriptor(), because
    // toDescriptor() resolves through the public import to the imported file
    // (which DOES have types) — making the no-local-types case invisible
    // downstream. The recovered LT root is a NAMED_TYPE_REF to the first
    // publicly-imported type; references / resolvedReferences pass through.
    if (schema.rawSchema() != null
        && schema.rawSchema().getTypes().isEmpty()
        && !schema.rawSchema().getPublicImports().isEmpty()) {
      // schema.name() already does the public-import fall-through (with
      // transitive resolution + cycle protection), so reader and
      // ProtobufSchema agree on which type counts as the root.
      return new LogicalType(
          emptyToNull(schema.rawSchema().getPackageName()),
          Schema.createNamedTypeRef(schema.name()).setNullable(false),
          Collections.emptyMap(),
          schema.references(),
          schema.resolvedReferences(),
          Collections.emptyMap());
    }
    final Descriptor rootDescriptor = schema.toDescriptor();
    if (rootDescriptor == null) {
      throw new ValidationException("ProtobufSchema has no root descriptor");
    }
    // If the root descriptor is itself a well-known wrapper (Bool/Int32/Int64/
    // Float/Double/String/Bytes/Variant), unwrap to the bare LT leaf type.
    // Mirrors ProtobufData's wrapper.for.raw.primitives behavior. Wrapped
    // roots are inherently nullable (the message itself can be unset).
    final FileDescriptor file = rootDescriptor.getFile();
    Optional<Schema> unwrapped = toUnwrappedSchema(rootDescriptor);
    if (unwrapped.isPresent()) {
      return new LogicalType(
          emptyToNull(file.getPackage()),
          unwrapped.get(),
          Collections.emptyMap(),
          schema.references(),
          schema.resolvedReferences(),
          Collections.emptyMap());
    }
    final ToLogicalContext<String> ctx =
        new ToLogicalContext<>(schema);
    // Extend reference names with all nested message/enum full names from
    // each resolved external schema, so nested external types are recognized.
    // Enum-only files (no top-level messages) are skipped — the top-level
    // enum's name is already in the set from the constructor.
    for (Map.Entry<String, String> entry :
        schema.resolvedReferences().entrySet()) {
      ProtobufSchema parsedRef = new ProtobufSchema(entry.getValue(),
          schema.references(), schema.resolvedReferences(),
          null, null, null, null);
      Descriptor refDescriptor = parsedRef.toDescriptor();
      if (refDescriptor != null) {
        collectExternalTypeNames(refDescriptor.getFile(), ctx);
      }
    }
    final List<Descriptor> messageTypes = file.getMessageTypes();
    if (messageTypes.isEmpty()) {
      throw new ValidationException(
          "Protobuf file has no top-level messages");
    }
    // Convention: the descriptor stored in {@code schema} is the root (callers
    // that pass {@code new ProtobufSchema(specificMessage)} can pick any top-
    // level message). All other top-level messages and all top-level enums are
    // local named types (corresponding to STRUCT/ENUM declarations). Anonymous
    // structs synthesized for inline anonymous types remain nested inside
    // their parent message.
    // Pre-register every named-type peer (full name) so field-side conversion
    // can recognize them via ctx.hasNamedType when their typeName is
    // referenced. Each entry gets a placeholder; the body is filled in below.
    //
    // The ROOT message is also pre-registered. This lets recursive root
    // messages (e.g., `message Tree { Tree left = 1; Tree right = 2; }`)
    // resolve their self-references via the placeholder lookup. After all
    // bodies are built we either keep the root as a NAMED_TYPE_REF (recursive
    // case) or unwrap it back to a STRUCT (non-recursive case — preserves
    // existing behavior for callers that probe getRootSchema().getField).
    Descriptor rootMessage = rootDescriptor;
    for (Descriptor msg : messageTypes) {
      ctx.putNamedType(msg.getFullName(),
          Schema.createStruct(new ArrayList<>()));
    }
    final String rootFqn = rootMessage.getFullName();
    for (com.google.protobuf.Descriptors.EnumDescriptor enm : file.getEnumTypes()) {
      ctx.putNamedType(enm.getFullName(),
          Schema.createEnum(new ArrayList<>()));
    }
    // Lift NESTED types (proto's `message Outer { message Inner ... }`) into
    // localNamedTypes too — keyed by their full dotted name. The field handler
    // checks hasNamedType, so once they're registered as placeholders,
    // field references to nested types emit NAMED_TYPE_REF instead of inlining.
    for (Descriptor topLevel : messageTypes) {
      preRegisterNestedTypes(topLevel, ctx);
    }
    // Build the root body and replace the placeholder.
    Schema rootBody = toLogicalTypeNested(
        false, rootMessage, ctx, Collections.emptyList());
    ctx.putNamedType(rootFqn, rootBody);
    // Build peer bodies (replacing placeholders). Iterate every top-level
    // message and skip whichever one is the root (which may or may not be at
    // file index 0, depending on which descriptor the caller passed).
    for (Descriptor peer : messageTypes) {
      if (peer == rootMessage) {
        continue;
      }
      ctx.putNamedType(peer.getFullName(),
          toLogicalTypeNested(false, peer, ctx,
              Collections.singletonList(peer.getIndex())));
    }
    for (com.google.protobuf.Descriptors.EnumDescriptor enm : file.getEnumTypes()) {
      ctx.putNamedType(enm.getFullName(), convertEnumDescriptor(enm));
    }
    // Build nested bodies (replacing placeholders) for every top-level message.
    for (Descriptor topLevel : messageTypes) {
      buildNestedBodies(topLevel, ctx);
    }
    // Decide the rootSchema shape:
    //   - Recursive (self or mutual cycle): keep as NAMED_TYPE_REF, body in
    //     namedTypes — necessary for cyclic field references to resolve.
    //   - Has nested types in namedTypes (auto-promoted via cycle detection
    //     or user-marked): keep as NAMED_TYPE_REF — the nested types use
    //     parentOf() to find their parent, which requires the root to be in
    //     namedTypes.
    //   - Otherwise: unwrap to STRUCT and remove root from namedTypes —
    //     preserves the historical "namedTypes = peers; root is direct"
    //     convention for the simple case.
    final Schema rootSchema;
    if (LogicalType.isCyclic(rootFqn, ctx.getNamedTypes())
        || hasNestedNamedTypes(rootFqn, ctx.getNamedTypes())) {
      rootSchema = Schema.createNamedTypeRef(rootFqn).setNullable(false);
    } else {
      rootSchema = rootBody;
      ((java.util.Map<String, Schema>) ctx.getNamedTypes()).remove(rootFqn);
    }
    return new LogicalType(
        emptyToNull(file.getPackage()),
        rootSchema,
        ctx.getNamedTypes(),
        ctx.getExternalTypes(),
        schema.references(),
        schema.resolvedReferences(),
        ctx.getDefaultValues());
  }

  /**
   * True iff any key in {@code namedTypes} is a dotted-prefix descendant of
   * {@code rootFqn} — i.e., a nested named type whose parent-of resolution
   * needs {@code rootFqn} to remain present in {@code namedTypes}. If we
   * unwrapped the root in this case, {@code parentOf} would return null for
   * the nested type and the writer's namespace validator would reject it.
   */
  private static boolean hasNestedNamedTypes(
      String rootFqn, java.util.Map<String, Schema> namedTypes) {
    String prefix = rootFqn + ".";
    for (String name : namedTypes.keySet()) {
      if (name.startsWith(prefix)) {
        return true;
      }
    }
    return false;
  }

  private static String emptyToNull(String s) {
    return s == null || s.isEmpty() ? null : s;
  }

  private static Schema convertEnumDescriptor(
      com.google.protobuf.Descriptors.EnumDescriptor enm) {
    List<EnumValue> enumValues = new ArrayList<>();
    for (EnumValueDescriptor evd : enm.getValues()) {
      String evDoc = null;
      Map<String, Object> evParams = null;
      if (evd.getOptions().hasExtension(MetaProto.enumValueMeta)) {
        Meta evMeta = evd.getOptions().getExtension(MetaProto.enumValueMeta);
        String doc = evMeta.getDoc();
        evDoc = (doc != null && !doc.trim().isEmpty()) ? doc : null;
        Map<String, String> params = evMeta.getParamsMap();
        if (!params.isEmpty()) {
          evParams = new LinkedHashMap<>(params);
        }
      }
      enumValues.add(new EnumValue(evd.getName(), evDoc, evParams));
    }
    Schema enumSchema = Schema.createEnum(enumValues).setNullable(false);
    if (enm.getOptions().hasExtension(MetaProto.enumMeta)) {
      Meta meta = enm.getOptions().getExtension(MetaProto.enumMeta);
      readMeta(meta, enumSchema);
    }
    return enumSchema;
  }

  /**
   * Recursively walks an external file's messages and enums (top-level + nested)
   * and registers each full name as an external reference.
   */
  private static void collectExternalTypeNames(
      FileDescriptor file, ToLogicalContext<String> ctx) {
    for (Descriptor msg : file.getMessageTypes()) {
      collectExternalMessageNames(msg, ctx);
    }
    for (com.google.protobuf.Descriptors.EnumDescriptor enm : file.getEnumTypes()) {
      ctx.addExternalType(enm.getFullName());
    }
  }

  /**
   * Walk {@code msg}'s nested messages and enums (recursively) and pre-register
   * each USER-DECLARED type as a placeholder in
   * {@link ToLogicalContext#putNamedType}. The
   * {@link CommonConstants#LOGICAL_NAMED_PROP logical.named} marker (set by
   * the writer's user-declared nested emission) distinguishes user-declared
   * types from writer-synthesized wrappers. Skips synthesized map-entry
   * messages and any nested message/enum without the marker — those stay
   * inlined.
   */
  private static void preRegisterNestedTypes(
      Descriptor msg, ToLogicalContext<String> ctx) {
    for (Descriptor nested : msg.getNestedTypes()) {
      if (nested.getOptions().getMapEntry()) {
        continue;
      }
      // Pre-register if the user marked the type as named, OR if it's part
      // of a cycle (self-reference or mutual recursion via other reachable
      // message types). Anonymous types only get registered for the cycle
      // case — they MUST become named in the LT to avoid infinite-inline,
      // but non-recursive anonymous types continue to inline as STRUCT.
      if (isUserDeclaredNamedType(nested) || isCyclicMessage(nested)) {
        ctx.putNamedType(nested.getFullName(),
            Schema.createStruct(new ArrayList<>()));
      }
      // Recurse regardless of the parent's marker — a non-named wrapper might
      // still contain a user-declared nested type underneath (rare, but the
      // marker is per-type, not per-subtree).
      preRegisterNestedTypes(nested, ctx);
    }
    for (com.google.protobuf.Descriptors.EnumDescriptor enm : msg.getEnumTypes()) {
      if (isUserDeclaredNamedType(enm)) {
        ctx.putNamedType(enm.getFullName(),
            Schema.createEnum(new ArrayList<>()));
      }
    }
  }

  /**
   * True if {@code start}'s message-field graph reaches back to {@code start}
   * itself — i.e., the type is part of a cycle (self-reference or mutual
   * recursion via other reachable message types).
   *
   * <p>Used by {@link #preRegisterNestedTypes} to decide whether an unmarked
   * nested type needs to be promoted to named in the LT. Anonymous types
   * that aren't part of any cycle continue to inline normally; cyclic ones
   * MUST be promoted, otherwise the body walk infinite-loops.
   */
  private static boolean isCyclicMessage(Descriptor start) {
    String target = start.getFullName();
    java.util.Set<String> seen = new java.util.HashSet<>();
    java.util.Deque<Descriptor> stack = new java.util.ArrayDeque<>();
    // Seed with start's direct children so we don't immediately match start
    // itself (which would be true for any node, vacuously).
    for (com.google.protobuf.Descriptors.FieldDescriptor f : start.getFields()) {
      if (f.getType()
          == com.google.protobuf.Descriptors.FieldDescriptor.Type.MESSAGE) {
        stack.push(f.getMessageType());
      }
    }
    while (!stack.isEmpty()) {
      Descriptor cur = stack.pop();
      if (cur.getFullName().equals(target)) {
        return true;
      }
      if (!seen.add(cur.getFullName())) {
        continue;
      }
      for (com.google.protobuf.Descriptors.FieldDescriptor f : cur.getFields()) {
        if (f.getType()
            == com.google.protobuf.Descriptors.FieldDescriptor.Type.MESSAGE) {
          stack.push(f.getMessageType());
        }
      }
    }
    return false;
  }

  /**
   * Walk {@code msg}'s nested messages and enums (recursively) and replace
   * each placeholder with its converted body. Skip map-entry messages and any
   * type that wasn't pre-registered (i.e., synthesized wrappers). The
   * logical.named marker is auto-stripped from the recovered Schema by
   * {@code isUserParam}, which excludes every {@code logical.*} key — no
   * explicit cleanup needed here.
   */
  private static void buildNestedBodies(
      Descriptor msg, ToLogicalContext<String> ctx) {
    for (Descriptor nested : msg.getNestedTypes()) {
      if (nested.getOptions().getMapEntry()) {
        continue;
      }
      // Build the body for any nested type that was pre-registered (either
      // user-marked or auto-promoted because it's cyclic). Mirror the
      // pre-registration condition in preRegisterNestedTypes.
      if (isUserDeclaredNamedType(nested) || isCyclicMessage(nested)) {
        // Empty index path: nested types' default-value paths aren't tracked
        // at this layer (they come up only inside the parent's field walk).
        Schema body = toLogicalTypeNested(
            false, nested, ctx, Collections.emptyList());
        ctx.putNamedType(nested.getFullName(), body);
      }
      buildNestedBodies(nested, ctx);
    }
    for (com.google.protobuf.Descriptors.EnumDescriptor enm : msg.getEnumTypes()) {
      if (isUserDeclaredNamedType(enm)) {
        ctx.putNamedType(enm.getFullName(), convertEnumDescriptor(enm));
      }
    }
  }

  /**
   * Does this nested message carry the
   * {@link CommonConstants#LOGICAL_NAMED_PROP logical.named} marker in its
   * Meta params? If so, the writer flagged it as a user-declared named type
   * and the reader should lift it into {@code localNamedTypes}.
   */
  private static boolean isUserDeclaredNamedType(Descriptor msg) {
    if (!msg.getOptions().hasExtension(MetaProto.messageMeta)) {
      return false;
    }
    Meta meta = msg.getOptions().getExtension(MetaProto.messageMeta);
    return "true".equals(meta.getParamsOrDefault(
        CommonConstants.LOGICAL_NAMED_PROP, null));
  }

  /**
   * EnumDescriptor variant of {@link #isUserDeclaredNamedType(Descriptor)}.
   */
  private static boolean isUserDeclaredNamedType(
      com.google.protobuf.Descriptors.EnumDescriptor enm) {
    if (!enm.getOptions().hasExtension(MetaProto.enumMeta)) {
      return false;
    }
    Meta meta = enm.getOptions().getExtension(MetaProto.enumMeta);
    return "true".equals(meta.getParamsOrDefault(
        CommonConstants.LOGICAL_NAMED_PROP, null));
  }

  private static void collectExternalMessageNames(
      Descriptor msg, ToLogicalContext<String> ctx) {
    ctx.addExternalType(msg.getFullName());
    for (Descriptor nested : msg.getNestedTypes()) {
      collectExternalMessageNames(nested, ctx);
    }
    for (com.google.protobuf.Descriptors.EnumDescriptor enm : msg.getEnumTypes()) {
      ctx.addExternalType(enm.getFullName());
    }
  }

  private static Schema toLogicalTypeNested(
      final boolean isNullable,
      final Descriptor schema,
      final ToLogicalContext<String> ctx,
      final List<Integer> indexPath) {
    // Walk regular fields first, then oneofs, so each field's indexPath matches its
    // position in the resulting struct. Downstream consumers rely on the
    // regulars-then-oneofs layout, so we align path numbering to it rather than
    // reordering the fields.
    final List<Field> fields = new ArrayList<>();
    int index = 0;
    for (FieldDescriptor fieldDescriptor : schema.getFields()) {
      if (fieldDescriptor.getRealContainingOneof() != null) {
        continue;
      }
      fields.add(toField(ctx, fieldDescriptor, appendToList(indexPath, index++)));
    }
    for (OneofDescriptor oneOfDescriptor : schema.getRealOneofs()) {
      Schema unionSchema = toLogicalTypeOneof(
          oneOfDescriptor, ctx, appendToList(indexPath, index));
      fields.add(new Field(oneOfDescriptor.getName(), unionSchema, index++,
          null, false, null, null, null));
    }
    Schema structSchema = Schema.createStruct(fields).setNullable(isNullable);
    // Read message-level doc/tags/params from MessageOptions
    if (schema.getOptions().hasExtension(MetaProto.messageMeta)) {
      Meta meta = schema.getOptions().getExtension(MetaProto.messageMeta);
      readMeta(meta, structSchema);
    }
    return structSchema;
  }

  private static List<Integer> appendToList(final List<Integer> list, final int value) {
    final List<Integer> newList = new ArrayList<>(list);
    newList.add(value);
    return newList;
  }

  private static Schema toLogicalTypeOneof(
      final OneofDescriptor oneOfDescriptor,
      final ToLogicalContext<String> ctx,
      final List<Integer> indexPath) {
    List<FieldDescriptor> fieldDescriptors = oneOfDescriptor.getFields();
    final List<UnionBranch> branches = new ArrayList<>();
    for (int i = 0; i < fieldDescriptors.size(); i++) {
      final FieldDescriptor fieldDescriptor = fieldDescriptors.get(i);
      final String description = getDescription(fieldDescriptor);
      ctx.pushFieldPath(fieldDescriptor.getName());
      Schema fieldSchema = fieldToLogicalType(
          fieldDescriptor, ctx, appendToList(indexPath, i));
      // Force nullable since only one branch can be set
      fieldSchema = fieldSchema.setNullable(true);
      ctx.popFieldPath();
      Map<String, Object> branchParams = getBranchParams(fieldDescriptor);
      branches.add(new UnionBranch(
          fieldDescriptor.getName(), fieldSchema, description, branchParams));
    }
    return Schema.createUnion(branches).setNullable(true);
  }

  private static Field toField(
      final ToLogicalContext<String> ctx,
      final FieldDescriptor field,
      final List<Integer> indexPath) {
    final String description = getDescription(field);
    ctx.pushFieldPath(field.getName());
    Schema fieldSchema = fieldToLogicalType(field, ctx, indexPath);
    ctx.popFieldPath();
    // connect.default Meta param overrides any proto2 native default.
    Object defaultValue = null;
    boolean hasDefault = false;
    if (field.getOptions().hasExtension(MetaProto.fieldMeta)) {
      Meta meta = field.getOptions().getExtension(MetaProto.fieldMeta);
      String defaultStr = meta.getParamsMap().get(CommonConstants.LOGICAL_DEFAULT_PROP);
      if (defaultStr != null) {
        defaultValue = ProtoDefaultValueConverter.toJavaData(fieldSchema, defaultStr);
        hasDefault = true;
      }
    }
    if (!hasDefault && field.hasDefaultValue()) {
      defaultValue = field.getDefaultValue();
      hasDefault = true;
      // Mirror Flink: register the raw proto2 native default in the
      // path-keyed map. The connect.default override path above is
      // LT-specific and intentionally not mirrored — Flink's converter
      // doesn't read it.
      ctx.putDefaultValue(indexPath, defaultValue);
    } else if (!hasDefault && !isInsideMapEntry(field)) {
      // proto3 implicit scalar default: 0 / 0L / 0.0f / 0.0 / false / "" /
      // empty bytes / first-declared enum value. Recorded in the path-keyed
      // map ONLY (not on the Field's defaultValue) — the implicit default is
      // a wire-level proto-spec rule, not an explicit user declaration, so
      // DDL roundtrip stays clean.
      //
      // The {@code isInsideMapEntry} guard is LT-specific: our walker recurses
      // into synthetic MapEntry structs (via toLogicalTypeNested from
      // toMapSchema), unlike Flink's converter which reads key/value types
      // directly. Skip entry-struct sub-fields so their implicit defaults
      // don't surface as spurious entries below the map's own indexPath.
      final Object implicitDefault = synthesizeProto3ImplicitScalarDefault(field);
      if (implicitDefault != null) {
        ctx.putDefaultValue(indexPath, implicitDefault);
      }
    }
    List<String> fieldTags = getFieldTags(field);
    Map<String, Object> fieldParams = getFieldParams(field);
    List<io.confluent.kafka.schemaregistry.type.logical.Rule> fieldRules =
        getFieldRules(field);
    return new Field(field.getName(), fieldSchema, field.getIndex(),
        defaultValue, hasDefault, description, fieldTags, fieldParams, fieldRules);
  }

  /**
   * Extract CHECK rules from a field's {@code Meta.rules} list. Returns null
   * when the field has no rules so the {@link Field} constructor's
   * default-empty applies.
   */
  private static List<io.confluent.kafka.schemaregistry.type.logical.Rule> getFieldRules(
      FieldDescriptor field) {
    if (!field.getOptions().hasExtension(MetaProto.fieldMeta)) {
      return null;
    }
    return convertProtoRules(
        field.getOptions().getExtension(MetaProto.fieldMeta).getRulesList());
  }

  /**
   * Convert a proto {@code repeated Rule} into the LT {@link
   * io.confluent.kafka.schemaregistry.type.logical.Rule} list. Returns null
   * for empty input.
   */
  private static List<io.confluent.kafka.schemaregistry.type.logical.Rule> convertProtoRules(
      List<MetaProto.Rule> protoRules) {
    if (protoRules.isEmpty()) {
      return null;
    }
    List<io.confluent.kafka.schemaregistry.type.logical.Rule> out =
        new ArrayList<>(protoRules.size());
    for (MetaProto.Rule pr : protoRules) {
      // Skip rules with empty `expr` or `sql`. proto3 string fields default
      // to "" when unset on the wire, so without this guard a stray
      // `MetaProto.Rule` with only `name`/`doc` set would round-trip into
      // a Rule that the DDL emitter would render as `CHECK ()` —
      // unparseable. Matches the Avro/JSON readers' policy of skipping
      // null/empty rules.
      if (pr.getExpr().isEmpty() || pr.getSql().isEmpty()) {
        continue;
      }
      String name = pr.getName().isEmpty() ? null : pr.getName();
      String doc = pr.getDoc().isEmpty() ? null : pr.getDoc();
      out.add(new io.confluent.kafka.schemaregistry.type.logical.Rule(
          name, doc, pr.getExpr(), pr.getSql()));
    }
    return out;
  }

  private static List<String> getFieldTags(FieldDescriptor field) {
    if (field.getOptions().hasExtension(MetaProto.fieldMeta)) {
      List<String> tags = field.getOptions().getExtension(MetaProto.fieldMeta).getTagsList();
      return tags.isEmpty() ? null : new ArrayList<>(tags);
    }
    return null;
  }

  private static Map<String, Object> getFieldParams(FieldDescriptor field) {
    if (field.getOptions().hasExtension(MetaProto.fieldMeta)) {
      Map<String, String> params =
          field.getOptions().getExtension(MetaProto.fieldMeta).getParamsMap();
      if (!params.isEmpty()) {
        Map<String, Object> filtered = new LinkedHashMap<>();
        for (Map.Entry<String, String> entry : params.entrySet()) {
          if (isUserParam(entry.getKey())) {
            filtered.put(entry.getKey(), entry.getValue());
          }
        }
        return filtered.isEmpty() ? null : filtered;
      }
    }
    return null;
  }

  /**
   * True if a Meta.params key represents user-supplied metadata (vs. an
   * internal type-encoding marker). Internal namespaces are flink.*, connect.*,
   * and logical.* — all reserved for the converter; user `WITH params` from
   * SQL must not collide with these prefixes. Also reserved are unprefixed
   * keys that the proto encoding writes for specific types: {@code precision}
   * and {@code scale} for {@code .confluent.type.Decimal} fields.
   */
  private static boolean isUserParam(String key) {
    if (key.startsWith("flink.")
        || key.startsWith("connect.")
        || key.startsWith(CommonConstants.LOGICAL_PREFIX)) {
      return false;
    }
    return !CommonConstants.PROTOBUF_PRECISION_PROP.equals(key)
        && !CommonConstants.PROTOBUF_SCALE_PROP.equals(key);
  }

  private static String getDescription(FieldDescriptor field) {
    if (field.getOptions().hasExtension(MetaProto.fieldMeta)) {
      final Meta meta = field.getOptions().getExtension(MetaProto.fieldMeta);
      final String doc = meta.getDoc();
      return (doc == null || doc.trim().isEmpty()) ? null : doc;
    }
    return null;
  }

  private static Map<String, Object> getBranchParams(FieldDescriptor field) {
    if (field.getOptions().hasExtension(MetaProto.fieldMeta)) {
      final Meta meta = field.getOptions().getExtension(MetaProto.fieldMeta);
      Map<String, String> params = meta.getParamsMap();
      if (!params.isEmpty()) {
        // Filter out internal flink.*, connect.*, and logical.* params.
        Map<String, Object> result = new LinkedHashMap<>();
        for (Map.Entry<String, String> entry : params.entrySet()) {
          if (isUserParam(entry.getKey())) {
            result.put(entry.getKey(), entry.getValue());
          }
        }
        return result.isEmpty() ? null : result;
      }
    }
    return null;
  }

  private static void readMeta(Meta meta, Schema schema) {
    String doc = meta.getDoc();
    if (doc != null && !doc.trim().isEmpty()) {
      schema.setDoc(doc);
    }
    if (!meta.getTagsList().isEmpty()) {
      schema.setTags(meta.getTagsList());
    }
    Map<String, String> params = meta.getParamsMap();
    if (!params.isEmpty()) {
      Map<String, Object> filtered = new LinkedHashMap<>();
      for (Map.Entry<String, String> entry : params.entrySet()) {
        if (isUserParam(entry.getKey())) {
          filtered.put(entry.getKey(), entry.getValue());
        }
      }
      if (!filtered.isEmpty()) {
        schema.setParams(filtered);
      }
    }
    List<io.confluent.kafka.schemaregistry.type.logical.Rule> rules =
        convertProtoRules(meta.getRulesList());
    if (rules != null) {
      schema.setRules(rules);
    }
  }

  private static Schema fieldToLogicalType(
      final FieldDescriptor schema,
      final ToLogicalContext<String> ctx,
      final List<Integer> indexPath) {
    boolean isNullableType =
        getMeta(schema)
            .flatMap(getParam(CommonConstants.FLINK_NOT_NULL))
            .map(s -> !Boolean.parseBoolean(s))
            .orElseGet(() ->
                schema.getType().equals(Type.MESSAGE) && !schema.isRepeated());
    if (schema.isRepeated()) {
      return convertRepeated(schema, ctx, isNullableType, indexPath);
    } else {
      return convertNonRepeated(schema, ctx, isNullableType, indexPath);
    }
  }

  private static Schema convertNonRepeated(
      final FieldDescriptor schema,
      final ToLogicalContext<String> ctx,
      final boolean isNullableType,
      final List<Integer> indexPath) {
    final boolean isNullable = hasOptionalKeyword(schema) || isNullableType;

    switch (schema.getType()) {
      case INT32:
      case SINT32:
      case SFIXED32: {
        if (schema.getOptions().hasExtension(MetaProto.fieldMeta)) {
          Meta fieldMeta = schema.getOptions().getExtension(MetaProto.fieldMeta);
          Map<String, String> params = fieldMeta.getParamsMap();
          String connectType = params.get(CommonConstants.CONNECT_TYPE_PROP);
          if (CommonConstants.CONNECT_TYPE_INT8.equals(connectType)) {
            return Schema.create(Schema.Type.TINYINT).setNullable(isNullable);
          } else if (CommonConstants.CONNECT_TYPE_INT16.equals(connectType)) {
            return Schema.create(Schema.Type.SMALLINT).setNullable(isNullable);
          }
        }
        return Schema.create(Schema.Type.INT).setNullable(isNullable);
      }
      case UINT32:
      case FIXED32:
      case INT64:
      case UINT64:
      case SINT64:
      case FIXED64:
      case SFIXED64:
        return Schema.create(Schema.Type.BIGINT).setNullable(isNullable);
      case FLOAT:
        return Schema.create(Schema.Type.FLOAT).setNullable(isNullable);
      case DOUBLE:
        return Schema.create(Schema.Type.DOUBLE).setNullable(isNullable);
      case BOOL:
        return Schema.create(Schema.Type.BOOLEAN).setNullable(isNullable);
      case ENUM: {
        String enumFullName = schema.getEnumType().getFullName();
        // External enum: lazy-promote the body into namedTypes so the LT is
        // self-contained for downstream consumers (Flink shim, DDL emitter).
        // The pre-walk already added enumFullName to externalTypes so the
        // local-vs-external distinction is preserved on the LT.
        if (ctx.isExternalType(enumFullName)) {
          if (!ctx.hasNamedType(enumFullName)) {
            ctx.putNamedType(enumFullName, convertEnumDescriptor(schema.getEnumType()));
          }
          return Schema.createNamedTypeRef(enumFullName).setNullable(isNullable);
        }
        // Local named-type enum (file-level peer registered up-front)
        if (ctx.hasNamedType(enumFullName)) {
          return Schema.createNamedTypeRef(enumFullName).setNullable(isNullable);
        }
        List<EnumValue> enumValues = new ArrayList<>();
        for (EnumValueDescriptor evd : schema.getEnumType().getValues()) {
          String evDoc = null;
          Map<String, Object> evParams = null;
          if (evd.getOptions().hasExtension(MetaProto.enumValueMeta)) {
            Meta evMeta = evd.getOptions().getExtension(MetaProto.enumValueMeta);
            String doc = evMeta.getDoc();
            evDoc = (doc != null && !doc.trim().isEmpty()) ? doc : null;
            Map<String, String> params = evMeta.getParamsMap();
            if (!params.isEmpty()) {
              evParams = new LinkedHashMap<>(params);
            }
          }
          enumValues.add(new EnumValue(evd.getName(), evDoc, evParams));
        }
        Schema enumSchema = Schema.createEnum(enumValues).setNullable(isNullable);
        // Read enum-level doc/tags/params from EnumOptions
        if (schema.getEnumType().getOptions().hasExtension(MetaProto.enumMeta)) {
          Meta meta = schema.getEnumType().getOptions().getExtension(MetaProto.enumMeta);
          readMeta(meta, enumSchema);
        }
        return enumSchema;
      }
      case STRING:
        return createStringType(isNullable, schema);
      case BYTES:
        return createBytesType(isNullable, schema);
      case MESSAGE: {
        String fullName = schema.getMessageType().getFullName();
        switch (fullName) {
          case CommonConstants.PROTOBUF_DECIMAL_TYPE:
            return createDecimalType(isNullable, schema);
          case CommonConstants.PROTOBUF_DATE_TYPE:
            return Schema.create(Schema.Type.DATE).setNullable(isNullable);
          case CommonConstants.PROTOBUF_TIME_TYPE:
            return createTimeType(isNullable, schema);
          case CommonConstants.PROTOBUF_TIMESTAMP_TYPE:
            return createTimestampType(isNullable, schema);
          case CommonConstants.PROTOBUF_VARIANT_TYPE:
            return Schema.create(Schema.Type.VARIANT).setNullable(isNullable);
          default:
            // Well-known proto wrappers (BoolValue/Int32Value/Int64Value/etc.)
            // unwrap to nullable primitives. Check before external-type lookup so
            // imports of google/protobuf/wrappers.proto don't cause wrappers to be
            // emitted as unresolved NAMED_TYPE_REFs.
            Optional<Schema> wrapperUnwrap = toUnwrappedSchema(schema.getMessageType());
            if (wrapperUnwrap.isPresent()) {
              return wrapperUnwrap.get();
            }
            // External MESSAGE: lazy-promote the body into namedTypes so the
            // LT is self-contained. The pre-walk already added fullName to
            // externalTypes so the local-vs-external distinction is preserved.
            // Placeholder-then-body pattern guards against recursive bodies.
            if (ctx.isExternalType(fullName)) {
              if (!ctx.hasNamedType(fullName)) {
                ctx.putNamedType(fullName, Schema.createStruct(new ArrayList<>()));
                ctx.putNamedType(fullName, toLogicalTypeNested(
                    false, schema.getMessageType(), ctx, Collections.emptyList()));
              }
              return Schema.createNamedTypeRef(fullName).setNullable(isNullable);
            }
            // Local named-type message (file-level peer registered up-front)
            if (ctx.hasNamedType(fullName)) {
              return Schema.createNamedTypeRef(fullName).setNullable(isNullable);
            }
            if (!ctx.addSeenSchema(fullName)) {
              throw new ValidationException(
                  ctx.getCyclicSchemaErrorMessage());
            }
            final Schema recordSchema =
                toUnwrappedOrRecordSchema(
                    isNullable, schema, ctx, indexPath);
            ctx.removeSeenSchema(fullName);
            return recordSchema;
        }
      }
      default:
        throw new ValidationException("Unknown Protobuf schema type " + schema.getType());
    }
  }

  // copied from Descriptors.java since it is not public
  private static boolean hasOptionalKeyword(FieldDescriptor fieldDescriptor) {
    return fieldDescriptor.toProto().getProto3Optional()
        || (getEdition(fieldDescriptor.getFile()) == Edition.EDITION_PROTO2
        && fieldDescriptor.isOptional()
        && fieldDescriptor.getContainingOneof() == null);
  }

  // copied from Descriptors.java since it is not public
  private static DescriptorProtos.Edition getEdition(FileDescriptor file) {
    switch (file.toProto().getSyntax()) {
      case "editions":
        return file.toProto().getEdition();
      case "proto3":
        return Edition.EDITION_PROTO3;
      default:
        return Edition.EDITION_PROTO2;
    }
  }

  private static Schema createStringType(boolean isNullable, FieldDescriptor schema) {
    final Optional<Meta> meta = getMeta(schema);
    if (meta.isPresent()) {
      final Meta fieldMeta = meta.get();
      final int minLength = Integer.parseInt(
          fieldMeta.getParamsOrDefault(CommonConstants.FLINK_MIN_LENGTH, "-1"));
      final int maxLength = Optional.ofNullable(
              fieldMeta.getParamsOrDefault(CommonConstants.FLINK_MAX_LENGTH, null))
          .map(Integer::valueOf)
          .orElse(MAX_LENGTH);
      if (minLength > 0 && minLength == maxLength) {
        return Schema.createChar(maxLength).setNullable(isNullable);
      } else if (maxLength < MAX_LENGTH) {
        return Schema.createVarchar(maxLength).setNullable(isNullable);
      }
    }
    return Schema.createString().setNullable(isNullable);
  }

  private static Schema createBytesType(boolean isNullable, FieldDescriptor schema) {
    final Optional<Meta> meta = getMeta(schema);
    if (meta.isPresent()) {
      final Meta fieldMeta = meta.get();
      final int minLength = Integer.parseInt(
          fieldMeta.getParamsOrDefault(CommonConstants.FLINK_MIN_LENGTH, "-1"));
      final int maxLength = Optional.ofNullable(
              fieldMeta.getParamsOrDefault(CommonConstants.FLINK_MAX_LENGTH, null))
          .map(Integer::valueOf)
          .orElse(MAX_LENGTH);
      if (minLength > 0 && minLength == maxLength) {
        return Schema.createBinary(maxLength).setNullable(isNullable);
      } else if (maxLength < MAX_LENGTH) {
        return Schema.createVarbinary(maxLength).setNullable(isNullable);
      }
    }
    return Schema.createBytes().setNullable(isNullable);
  }

  private static Schema createTimeType(boolean isNullable, FieldDescriptor schema) {
    // TimeOfDay's nanos field gives the wire type natural precision 9.
    final int defaultPrecision = 9;
    final int precision = getMeta(schema)
        .map(m -> Integer.parseInt(
            m.getParamsOrDefault(CommonConstants.FLINK_PRECISION_PROP,
                String.valueOf(defaultPrecision))))
        .orElse(defaultPrecision);
    return Schema.createTime(precision).setNullable(isNullable);
  }

  private static Schema createDecimalType(boolean isNullable, FieldDescriptor schema) {
    int precision = DEFAULT_DECIMAL_PRECISION;
    int scale = DEFAULT_DECIMAL_SCALE;
    final Optional<Meta> meta = getMeta(schema);
    if (meta.isPresent()) {
      Meta fieldMeta = meta.get();
      Map<String, String> params = fieldMeta.getParamsMap();
      String precisionStr = params.get(CommonConstants.PROTOBUF_PRECISION_PROP);
      if (precisionStr != null) {
        try {
          precision = Integer.parseInt(precisionStr);
        } catch (NumberFormatException e) {
          // ignore
        }
      }
      String scaleStr = params.get(CommonConstants.PROTOBUF_SCALE_PROP);
      if (scaleStr != null) {
        try {
          scale = Integer.parseInt(scaleStr);
        } catch (NumberFormatException e) {
          // ignore
        }
      }
    }
    return Schema.createDecimal(precision, scale).setNullable(isNullable);
  }

  private static Schema createTimestampType(boolean isNullable, FieldDescriptor schema) {
    final int defaultPrecision = 9;
    final Optional<Meta> meta = getMeta(schema);
    if (meta.isPresent()) {
      final int precision = meta
          .map(m -> Integer.parseInt(
              m.getParamsOrDefault(CommonConstants.FLINK_PRECISION_PROP,
                  String.valueOf(defaultPrecision))))
          .orElse(defaultPrecision);
      if (CommonConstants.FLINK_TYPE_TIMESTAMP.equals(
          meta.get().getParamsOrDefault(CommonConstants.FLINK_TYPE_PROP, null))) {
        return Schema.createTimestamp(precision).setNullable(isNullable);
      } else {
        return Schema.createTimestampLtz(precision).setNullable(isNullable);
      }
    } else {
      return Schema.createTimestampLtz(defaultPrecision).setNullable(isNullable);
    }
  }

  private static Optional<Meta> getMeta(final FieldDescriptor schema) {
    if (schema.getOptions().hasExtension(MetaProto.fieldMeta)) {
      return Optional.of(schema.getOptions().getExtension(MetaProto.fieldMeta));
    }
    return Optional.empty();
  }

  private static Schema convertRepeated(
      final FieldDescriptor schema,
      final ToLogicalContext<String> ctx,
      final boolean isNullableType,
      final List<Integer> indexPath) {
    if (isMapDescriptor(schema)) {
      return toMapSchema(schema, isNullableType, ctx, indexPath);
    } else {
      final boolean isArrayElementWrapped =
          getMeta(schema)
              .flatMap(getParam(CommonConstants.FLINK_WRAPPER))
              .map(Boolean::parseBoolean)
              .orElse(false);
      final Schema arraySchema;
      if (isArrayElementWrapped) {
        // The wrapper is a single-payload struct: either a regular field named
        // "value" (RepeatedWrapper), or a oneof named "value" (OneofWrapper for
        // wrapped UNIONs). Walk the payload directly with the parent indexPath
        // — skipping the wrapper struct walk avoids appending a wrapper-layer
        // index that the unwrapped schema doesn't carry, keeping default-value
        // paths aligned with the returned schema's structure.
        Schema elementSchema = convertWrapperPayload(
            schema.getMessageType(), ctx, indexPath);
        arraySchema = Schema.createArray(elementSchema).setNullable(isNullableType);
      } else {
        arraySchema = Schema.createArray(convertNonRepeated(
                schema, ctx, false, indexPath))
            .setNullable(isNullableType);
      }
      // Proto spec: an absent repeated field is an empty list. Record that as
      // the implicit default so downstream consumers (e.g. Tableflow
      // schema-evolution compat checks) can treat "adding a new repeated
      // field" as safe.
      ctx.putDefaultValue(indexPath, Collections.emptyList());
      return arraySchema;
    }
  }

  private static Function<Meta, Optional<String>> getParam(final String paramKey) {
    return m -> Optional.ofNullable(m.getParamsOrDefault(paramKey, null));
  }

  private static Schema toUnwrappedOrRecordSchema(
      final boolean isNullable,
      final FieldDescriptor descriptor,
      final ToLogicalContext<String> ctx,
      final List<Integer> indexPath) {
    final boolean isRepeatedWrapped =
        getMeta(descriptor)
            .flatMap(getParam(CommonConstants.FLINK_WRAPPER))
            .map(Boolean::parseBoolean)
            .orElse(false);
    if (isRepeatedWrapped) {
      // The wrapper struct has a single payload named "value" — either a
      // regular field (RepeatedWrapper), or a oneof (OneofWrapper for wrapped
      // UNIONs). Walk the payload directly with the parent indexPath; skipping
      // the wrapper struct walk avoids appending a wrapper-layer index that
      // the unwrapped schema doesn't carry, keeping default-value paths
      // aligned with the returned schema's structure. The wrapped value is
      // always nullable (the wrapper field's presence carries the marker).
      Schema valueSchema = convertWrapperPayload(
          descriptor.getMessageType(), ctx, indexPath);
      return valueSchema.setNullable(true);
    }

    return toUnwrappedSchema(descriptor.getMessageType())
        .orElseGet(() -> toLogicalTypeNested(
            isNullable, descriptor.getMessageType(), ctx, indexPath));
  }

  /**
   * Convert a "flink.wrapped" wrapper struct's single payload (named "value") to its
   * inner schema, walking with {@code indexPath} unmodified. Handles both shapes:
   * a single regular field, or a single oneof (for wrapped UNIONs).
   */
  private static Schema convertWrapperPayload(
      final Descriptor wrapper,
      final ToLogicalContext<String> ctx,
      final List<Integer> indexPath) {
    FieldDescriptor valueField = wrapper.findFieldByName(
        CommonConstants.FLINK_WRAPPER_FIELD_NAME);
    if (valueField != null && valueField.getRealContainingOneof() == null) {
      return fieldToLogicalType(valueField, ctx, indexPath);
    }
    for (OneofDescriptor oneof : wrapper.getRealOneofs()) {
      if (CommonConstants.FLINK_WRAPPER_FIELD_NAME.equals(oneof.getName())) {
        return toLogicalTypeOneof(oneof, ctx, indexPath);
      }
    }
    throw new ValidationException(
        "flink.wrapped wrapper missing 'value' payload: " + wrapper.getFullName());
  }

  private static Optional<Schema> toUnwrappedSchema(final Descriptor descriptor) {
    String fullName = descriptor.getFullName();
    switch (fullName) {
      case CommonConstants.PROTOBUF_DOUBLE_WRAPPER_TYPE:
        return Optional.of(Schema.create(Schema.Type.DOUBLE).setNullable(true));
      case CommonConstants.PROTOBUF_FLOAT_WRAPPER_TYPE:
        return Optional.of(Schema.create(Schema.Type.FLOAT).setNullable(true));
      case CommonConstants.PROTOBUF_INT64_WRAPPER_TYPE:
      case CommonConstants.PROTOBUF_UINT64_WRAPPER_TYPE:
      case CommonConstants.PROTOBUF_UINT32_WRAPPER_TYPE:
        return Optional.of(Schema.create(Schema.Type.BIGINT).setNullable(true));
      case CommonConstants.PROTOBUF_INT32_WRAPPER_TYPE:
        return Optional.of(Schema.create(Schema.Type.INT).setNullable(true));
      case CommonConstants.PROTOBUF_BOOL_WRAPPER_TYPE:
        return Optional.of(Schema.create(Schema.Type.BOOLEAN).setNullable(true));
      case CommonConstants.PROTOBUF_STRING_WRAPPER_TYPE:
        return Optional.of(Schema.createString().setNullable(true));
      case CommonConstants.PROTOBUF_BYTES_WRAPPER_TYPE:
        return Optional.of(Schema.createBytes().setNullable(true));
      case CommonConstants.PROTOBUF_VARIANT_TYPE:
        return Optional.of(Schema.create(Schema.Type.VARIANT).setNullable(true));
      default:
        return Optional.empty();
    }
  }

  private static Schema toMapSchema(
      final FieldDescriptor descriptor,
      final boolean isNullableType,
      final ToLogicalContext<String> ctx,
      final List<Integer> indexPath) {
    // Convert the wrapper struct to a LT struct first. This handles oneof-as-key
    // (e.g. MULTISET<UNION<...>>) uniformly with all other element types — the
    // existing struct conversion turns oneofs into UNIONs. Then look up the
    // "key" and "value" entries by name (positions are unreliable when a oneof
    // is present because oneof branches flatten into the field list).
    final Schema entryStruct = toLogicalTypeNested(
        false, descriptor.getMessageType(), ctx, indexPath);
    final Schema keyType = entryStruct.getField(CommonConstants.KEY_FIELD).getSchema();
    final Schema valueType = entryStruct.getField(CommonConstants.VALUE_FIELD).getSchema();

    final boolean isMultiset =
        getMeta(descriptor)
            .map(m -> Objects.equals(
                CommonConstants.FLINK_TYPE_MULTISET,
                m.getParamsOrDefault(CommonConstants.FLINK_TYPE_PROP, null)))
            .orElse(false);

    // Proto spec: an absent map field (including the multiset encoding, which
    // is built on top of a proto map) is an empty map. Record that as the
    // implicit default so downstream consumers (e.g. Tableflow schema-evolution
    // compat checks) can treat "adding a new map field" as safe.
    ctx.putDefaultValue(indexPath, Collections.emptyMap());

    if (isMultiset) {
      if (valueType.getType() != Schema.Type.INT) {
        throw new ValidationException(
            "Unexpected value type for a MULTISET type: " + valueType);
      }
      return Schema.createMultiset(keyType).setNullable(isNullableType);
    } else {
      return Schema.createMap(keyType, valueType).setNullable(isNullableType);
    }
  }

  /**
   * The {@link FieldDescriptor#hasPresence()} gate is what makes this correct:
   * fields with presence semantics (proto2 singular, proto3 {@code optional},
   * oneof members, MESSAGE/GROUP) map "absent" to {@code null} not to an
   * implicit scalar zero. Repeated and map fields are skipped — their
   * synthesis belongs to the repeated/map code path in
   * {@link #convertRepeated}/{@link #toMapSchema}.
   *
   * <p>Note: despite the misleading name,
   * {@link FieldDescriptor#getDefaultValue()} returns the type-correct zero
   * for implicit-presence scalars even when
   * {@link FieldDescriptor#hasDefaultValue()} is {@code false}. Only
   * MESSAGE/GROUP throw {@link UnsupportedOperationException} from it, and
   * the {@code hasPresence()} guard already excludes those — the explicit
   * throw below is a defensive boundary check.
   *
   * @return the synthesized default, or {@code null} if no default should be
   *     recorded
   */
  private static Object synthesizeProto3ImplicitScalarDefault(final FieldDescriptor field) {
    if (field.isRepeated() || field.hasPresence()) {
      return null;
    }
    // MESSAGE/GROUP always have presence and should have been filtered by the
    // guard above; if that ever changes, fail loudly rather than silently
    // swallowing the default because FieldDescriptor#getDefaultValue() throws
    // UnsupportedOperationException for them.
    switch (field.getType()) {
      case MESSAGE:
      case GROUP:
        throw new IllegalStateException(
            "MESSAGE/GROUP fields should have been filtered by the hasPresence() guard; "
                + "got " + field.getType() + " for field " + field.getFullName());
      default:
        return field.getDefaultValue();
    }
  }

  /**
   * LT-specific guard for {@link #synthesizeProto3ImplicitScalarDefault}: skip
   * sub-fields of synthetic MapEntry structs. The LT walker recurses into the
   * entry struct (via {@link #toLogicalTypeNested} from {@link #toMapSchema}),
   * unlike Flink's converter which reads key/value types directly. Without
   * this guard, the entry's {@code key}/{@code value} fields would get
   * implicit defaults that surface as spurious entries below the map's own
   * indexPath. Both native map entries (parent option {@code map_entry}) and
   * Flink-MULTISET-style user-defined entries (matched structurally by
   * {@link #isMapEntryShape}) are covered.
   */
  private static boolean isInsideMapEntry(final FieldDescriptor field) {
    Descriptor parent = field.getContainingType();
    if (parent == null) {
      return false;
    }
    return parent.getOptions().getMapEntry() || isMapEntryShape(parent);
  }

  /**
   * Structural map-entry detection: the descriptor's name ends with
   * {@code Entry} and it has exactly two logical entities, named {@code key}
   * (regular field or oneof) and {@code value} (regular field). A oneof
   * counts as one entity (a UNION) so a {@code MULTISET&lt;UNION&lt;...&gt;&gt;}
   * entry — where {@code key} is a oneof — still matches.
   *
   * <p>Two callers depend on this gate:
   * <ul>
   *   <li>{@link #isMapDescriptor}, which is the routing gate for
   *       {@link #toMapSchema}.</li>
   *   <li>{@link #synthesizeProto3ImplicitScalarDefault}, which uses it to
   *       exclude entry-struct sub-fields from implicit-default synthesis —
   *       every descriptor walked as a map entry must also be excluded
   *       there. Sharing this helper keeps the two checks in lock-step.</li>
   * </ul>
   */
  private static boolean isMapEntryShape(final Descriptor descriptor) {
    if (!descriptor.getName().endsWith(CommonConstants.MAP_ENTRY_SUFFIX)) {
      return false;
    }
    int regularFieldCount = 0;
    boolean hasKeyRegular = false;
    boolean hasValueRegular = false;
    for (FieldDescriptor f : descriptor.getFields()) {
      if (f.getRealContainingOneof() != null) {
        continue;
      }
      regularFieldCount++;
      if (CommonConstants.KEY_FIELD.equals(f.getName())) {
        hasKeyRegular = true;
      } else if (CommonConstants.VALUE_FIELD.equals(f.getName())) {
        hasValueRegular = true;
      }
    }
    List<com.google.protobuf.Descriptors.OneofDescriptor> oneofs = descriptor.getRealOneofs();
    if (regularFieldCount + oneofs.size() != 2) {
      return false;
    }
    boolean hasKeyOneof = oneofs.stream()
        .anyMatch(o -> CommonConstants.KEY_FIELD.equals(o.getName()));
    return (hasKeyRegular || hasKeyOneof) && hasValueRegular;
  }

  private static boolean isMapDescriptor(final FieldDescriptor fieldDescriptor) {
    if (fieldDescriptor.getType() != Type.MESSAGE) {
      return false;
    }
    return isMapEntryShape(fieldDescriptor.getMessageType());
  }
}
