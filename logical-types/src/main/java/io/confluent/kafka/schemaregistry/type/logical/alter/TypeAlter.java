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

package io.confluent.kafka.schemaregistry.type.logical.alter;

import com.google.protobuf.DescriptorProtos.DescriptorProto;
import com.google.protobuf.DescriptorProtos.EnumDescriptorProto;
import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import io.confluent.kafka.schemaregistry.CompatibilityLevel;
import io.confluent.kafka.schemaregistry.CompatibilityPolicy;
import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.SimpleParsedSchemaHolder;
import io.confluent.kafka.schemaregistry.client.SchemaMetadata;
import io.confluent.kafka.schemaregistry.client.rest.entities.Config;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaReference;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.RegisterSchemaRequest;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import io.confluent.kafka.schemaregistry.type.logical.LogicalType;
import io.confluent.kafka.schemaregistry.type.logical.Rule;
import io.confluent.kafka.schemaregistry.type.logical.Schema;
import io.confluent.kafka.schemaregistry.type.logical.Schema.EnumValue;
import io.confluent.kafka.schemaregistry.type.logical.Schema.Field;
import io.confluent.kafka.schemaregistry.type.logical.Schema.UnionBranch;
import io.confluent.kafka.schemaregistry.type.logical.SchemaType;
import io.confluent.kafka.schemaregistry.type.logical.protobuf.ProtoToLogicalTypeConverter;
import io.confluent.kafka.schemaregistry.type.logical.provenance.ProvenanceReport;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Applies the actions of an ALTER TYPE to the most recent version of a type, soft-deleted or not,
 * as a logical type, and gives the result for the caller to register as a new version.
 *
 * <p>An edit must not change what the existing data means: a renamed Avro field keeps its old
 * name as an alias, and every Protobuf field keeps its number, so a drop does not renumber the
 * fields after it.
 */
public final class TypeAlter {

  /** The WITH property that names a type's format. */
  public static final String FORMAT = "format";
  /** The WITH property that sets the compatibility policy of a type's subject. */
  public static final String COMPATIBILITY_POLICY = "compatibility.policy";
  private static final Set<String> POLICIES = Set.of("LOGICAL", "STRICT", "LENIENT");
  // Protobuf keeps these field numbers for its own use, and refuses a schema that has one.
  private static final int RESERVED_FIRST = 19_000;
  private static final int RESERVED_LAST = 19_999;
  private static final String KEY_SUFFIX = "-key";
  private static final String VALUE_SUFFIX = "-value";

  /** What a tool knows of the type it alters that this package doesn't. */
  public static final class Options {

    private boolean columns;
    private Function<String, String> noReferences;
    private Consumer<LogicalType> check = lt -> { };

    /**
     * Whether the type is a topic's own, whose attributes are columns: their names compare
     * whatever their case, and its key and value types share their constraints' names.
     */
    public Options columns(boolean columns) {
      this.columns = columns;
      return this;
    }

    /**
     * Asked with the name of each type the altered type would refer to: the refusal if it may
     * not refer to that type, or null if it may. Not set, it may refer to any.
     */
    public Options noReferences(Function<String, String> noReferences) {
      this.noReferences = noReferences;
      return this;
    }

    /** The tool's own last check of the edited type, which refuses by throwing. */
    public Options check(Consumer<LogicalType> check) {
      this.check = Objects.requireNonNull(check, "check");
      return this;
    }
  }

  /** The new version an ALTER TYPE makes, for the caller to register. */
  public static final class Result {

    private final SchemaType format;
    private final SchemaMetadata base;
    private final int version;
    private final LogicalType edited;
    private final String policy;

    Result(SchemaType format, SchemaMetadata base, int version, LogicalType edited,
        String policy) {
      this.format = format;
      this.base = base;
      this.version = version;
      this.edited = edited;
      this.policy = policy;
    }

    public SchemaType format() {
      return format;
    }

    /** The version edited: the most recent one, soft-deleted or not. */
    public SchemaMetadata base() {
      return base;
    }

    /**
     * The number the new version takes; another by then means the type changed meanwhile. The
     * request from {@link #request()} carries it, so that the registry refuses it then.
     */
    public int version() {
      return version;
    }

    /** The new version, or null if the statement changes only the subject's settings. */
    public LogicalType edited() {
      return edited;
    }

    /** The compatibility policy a SET WITH gives the subject, or null to leave it. */
    public String policy() {
      return policy;
    }

    /**
     * The request that registers the new version, as logical types DDL, with its version, so
     * that the registry refuses it if another version was registered meanwhile; null if the
     * statement makes no new version.
     */
    public RegisterSchemaRequest request() {
      if (edited == null) {
        return null;
      }
      RegisterSchemaRequest request = Formats.request(format, edited);
      request.setVersion(version);
      return request;
    }
  }

  private final TypeLookup lookup;
  private final List<AlterAction> actions;
  private final String subject;

  private SchemaType format;
  private LogicalType original;
  private Schema root;
  private final Map<String, Schema> namedTypes = new LinkedHashMap<>();
  private final Set<String> externalTypes = new LinkedHashSet<>();
  private final Map<String, String> externalImports = new LinkedHashMap<>();
  private final List<SchemaReference> references = new ArrayList<>();
  private final Map<String, String> resolved = new LinkedHashMap<>();
  // The Protobuf field numbers of every version.
  private final TreeSet<Integer> usedNumbers = new TreeSet<>();
  // The root message of the version being edited, and of every version, for the numbers
  // nested messages have used.
  private Message current;
  // The attributes this statement drops, by their names before and in it, which it may not give
  // another attribute.
  private final Set<String> dropped = new HashSet<>();
  // The names of the previous version of attributes this statement renames, by their new names.
  private final Map<String, String> previousNames = new HashMap<>();
  // The attributes this statement adds, by their names in it.
  private final Set<String> added = new HashSet<>();
  private final List<Message> versions = new ArrayList<>();
  // The compatibility policy a SET WITH gives the subject, or null to leave it.
  private String policy;
  // The references moved from a soft-deleted version to the latest live one of their types.
  private final List<ColumnIds.Move> moved = new ArrayList<>();

  private final Consumer<LogicalType> check;
  // Whether this is a topic's own type, whose attributes are columns of case-insensitive names.
  private final boolean columns;
  // The refusal of a type the altered one would refer to, if it may refer to none; or null.
  private final Function<String, String> noReferences;

  private TypeAlter(TypeLookup lookup, String subject, List<AlterAction> actions,
      Options options) {
    this.lookup = lookup;
    this.actions = actions;
    this.subject = subject;
    this.check = options.check;
    this.columns = options.columns;
    this.noReferences = options.noReferences;
  }

  // For a type created again over its dropped versions: no statement alters it.
  private TypeAlter(TypeLookup lookup, String subject) {
    this(lookup, subject, null, new Options());
  }

  /**
   * The new version the actions make of a type, checked as far as it can be without the
   * registry: the caller sets the policy, if the result has one, and registers the edited type
   * as the result's version.
   */
  public static Result alter(TypeLookup lookup, String subject, List<AlterAction> actions,
      Options options) {
    return new TypeAlter(lookup, subject, actions, options).run();
  }

  /**
   * The version a new version of a subject gets: one more than any, soft-deleted or not, as
   * the registry numbers them. Read before the version a statement edits, it makes the
   * registration fail if another statement registered one meanwhile.
   */
  public static int nextVersion(TypeLookup lookup, String subject) {
    return lookup.versions(subject, true).stream().mapToInt(Integer::intValue).max()
        .orElse(0) + 1;
  }

  /** Removes a property by its name, ignoring case, returning its value or the fallback. */
  public static String removeIgnoreCase(Map<String, String> props, String key,
      String otherwise) {
    for (String k : new ArrayList<>(props.keySet())) {
      if (k.equalsIgnoreCase(key)) {
        return props.remove(k);
      }
    }
    return otherwise;
  }

  /**
   * Takes the compatibility policy out of a WITH, upper-cased, or returns null if it has none.
   */
  public static String removePolicy(Map<String, String> with) {
    String policy = removeIgnoreCase(with, COMPATIBILITY_POLICY, null);
    if (policy == null) {
      return null;
    }
    String upper = policy.toUpperCase(Locale.ROOT);
    if (!POLICIES.contains(upper)) {
      throw new TypeRefusedException("Unknown " + COMPATIBILITY_POLICY + " '" + policy
          + "': it is LOGICAL, STRICT or LENIENT");
    }
    return upper;
  }

  /**
   * A Protobuf type created again over its dropped versions, numbered as an ALTER of the last of
   * them would number it: an attribute keeps the number of its namesake, at every level, and any
   * other is numbered above every number used, so none reads another's data.
   */
  public static LogicalType numberedAfterDrop(TypeLookup lookup, String subject, SchemaMetadata md,
                      LogicalType lt) {
    return new TypeAlter(lookup, subject).numberAfterDrop(md, lt);
  }

  private LogicalType numberAfterDrop(SchemaMetadata md, LogicalType lt) {
    LogicalType last = lookup.logicalType(subject, md);
    final Schema old = targetOf(last);
    format = SchemaType.PROTOBUF;
    original = lt;
    root = lt.getRootSchema();
    namedTypes.putAll(lt.getNamedTypes());
    if (target().getType() != Schema.Type.STRUCT || old.getType() != Schema.Type.STRUCT) {
      return lt;
    }
    recordUsedNumbers();
    Messages at = new Messages(Message.of(old, last.getNamedTypes()), versions);
    setStruct(renumbered(target(), old, at, null).getFields());
    return new LogicalType(lt.getName(), lt.getNamespace(), root, namedTypes,
        lt.getExternalTypes(), lt.getExternalImports(), lt.getReferences(),
        lt.getResolvedReferences(), lt.getDefaultValues());
  }

  /**
   * An Avro type created again over its dropped versions, with a null default for each nullable
   * field it adds to the last of them, as an ALTER of it gives one: Avro reads their data by it.
   */
  public static LogicalType defaultedAfterDrop(TypeLookup lookup, String subject, SchemaMetadata md,
                       LogicalType lt) {
    return new TypeAlter(lookup, subject).defaultAfterDrop(md, lt);
  }

  private LogicalType defaultAfterDrop(SchemaMetadata md, LogicalType lt) {
    Schema old = targetOf(lookup.logicalType(subject, md));
    root = lt.getRootSchema();
    namedTypes.putAll(lt.getNamedTypes());
    setTarget(withDefaults(old, target()));
    return new LogicalType(lt.getName(), lt.getNamespace(), root, namedTypes,
        lt.getExternalTypes(), lt.getExternalImports(), lt.getReferences(),
        lt.getResolvedReferences(), lt.getDefaultValues());
  }

  /**
   * Refuses an Avro or JSON type created again over its dropped versions whose attributes Schema
   * Registry's provenance pairs with the last of them otherwise than an ALTER of it would, as if
   * it kept every attribute of the same name and added the others.
   */
  public static void refuseOtherColumnsAfterDrop(TypeLookup lookup, String subject,
      SchemaType format, SchemaMetadata md, LogicalType lt) {
    TypeAlter editor = new TypeAlter(lookup, subject);
    editor.format = format;
    editor.refuseOtherColumns(md, lt, md.getVersion() + 1);
  }

  /**
   * Drops an alias that is now another attribute's name, unless it is the attribute's own in the
   * version edited: it would make both attributes the one it names, as in a swap through a third
   * name, or swap them back and forth.
   */
  private void dropStaleAliases() {
    Schema target = target();
    if (format != SchemaType.AVRO || target.getType() != Schema.Type.STRUCT) {
      return;
    }
    Set<String> names = target.getFields().stream().map(Field::getName)
        .collect(Collectors.toSet());
    List<Field> fields = new ArrayList<>();
    boolean dropped = false;
    for (Field f : target.getFields()) {
      List<String> kept = f.getAliases().stream()
          .filter(a -> !a.equals(f.getName())
            && (!names.contains(a) || a.equals(previousName(f.getName()))))
          .collect(Collectors.toList());
      if (kept.size() == f.getAliases().size()) {
        fields.add(f);
        continue;
      }
      Map<String, Object> params = new LinkedHashMap<>(f.getParams());
      if (kept.isEmpty()) {
        params.remove(Schema.AVRO_ALIASES);
      } else {
        params.put(Schema.AVRO_ALIASES, String.join(",", kept));
      }
      fields.add(copy(f, f.getName(), f.getSchema(), params));
      dropped = true;
    }
    if (dropped) {
      setStruct(fields);
    }
  }

  private LogicalType edited() {
    return new LogicalType(original.getName(), original.getNamespace(), root, namedTypes,
        externalTypes, externalImports, references, resolved, Map.of());
  }

  private Result run() {
    // Read first, so that a version registered after the one edited is caught.
    int version = nextVersion(lookup, subject);
    SchemaMetadata md = lookup.latest(subject);
    if (md == null) {
      throw new TypeRefusedException("Unknown type " + subject);
    }
    // A new version continues the most recent one, soft-deleted or not, as provenance pairs it.
    boolean softDeleted = md.getVersion() < version - 1;
    if (softDeleted) {
      md = mostRecent(version - 1);
    }
    format = Formats.ofSchemaType(md.getSchemaType());
    original = lookup.logicalType(subject, md);
    root = original.getRootSchema();
    namedTypes.putAll(original.getNamedTypes());
    externalTypes.addAll(original.getExternalTypes());
    externalImports.putAll(original.getExternalImports());
    references.addAll(original.getReferences());
    resolved.putAll(original.getResolvedReferences());
    if (format == SchemaType.PROTOBUF) {
      recordProtobufNumbers();
      current = Message.of(target(), namedTypes);
      recordUsedNumbers();
    }

    boolean changesSchema = false;
    for (AlterAction action : actions) {
      changesSchema |= apply(action);
    }
    // A struct of no attributes is no schema in any format; judged on what the actions leave.
    if (target().getType() == Schema.Type.STRUCT && target().getFields().isEmpty()) {
      throw new TypeRefusedException("Type " + subject + " would have no attributes; drop the "
          + (columns ? "topic" : "type") + " instead");
    }

    LogicalType edited = null;
    if (changesSchema) {
      if (format == SchemaType.PROTOBUF) {
        refuseUncarried(md);
      }
      dropStaleAliases();
      recompileRules();
      if (format == SchemaType.JSON) {
        restoreDirectRefs();
      }
      dropUnusedReferences();
      moveDeletedReferences();
      refuseOlderReferences(lookup, subject, original.getReferences(), references, policy,
          "Type " + subject + " cannot be changed so", "drop");
      edited = edited();
      refuseOtherColumns(md, edited, version);
      if (softDeleted) {
        refuseIncompatible(lookup, subject, format, md, edited, policy, refused(),
            "version " + md.getVersion() + " of type " + subject);
      }
      check.accept(edited);
    }
    return new Result(format, md, version, edited, policy);
  }

  // A new version is written from the logical type, which carries neither what only a proto2
  // file has, such as required fields and defaults, nor the numbers and names a message or
  // enum reserves. The first would be lost, and the second free for a new field to take.
  private void refuseUncarried(SchemaMetadata md) {
    ParsedSchema schema = lookup.parse(md);
    if (!(schema instanceof ProtobufSchema)) {
      return;
    }
    FileDescriptorProto file = ((ProtobufSchema) schema).toDescriptor().getFile().toProto();
    String lost = !"proto3".equals(file.getSyntax())
        ? "it is not a proto3 file, and would be registered as one, without its required "
          + "fields and defaults"
        : reserves(file.getMessageTypeList(), file.getEnumTypeList())
          ? "it reserves field numbers or names, which would be dropped, and a new field "
            + "could then take one"
        : null;
    if (lost != null) {
      throw new TypeRefusedException("Type " + subject + " cannot be altered: " + lost
          + ". Register its new version as a Protobuf schema");
    }
  }

  private static boolean reserves(List<DescriptorProto> messages,
      List<EnumDescriptorProto> enums) {
    for (EnumDescriptorProto e : enums) {
      if (e.getReservedRangeCount() > 0 || e.getReservedNameCount() > 0) {
        return true;
      }
    }
    for (DescriptorProto m : messages) {
      if (m.getReservedRangeCount() > 0 || m.getReservedNameCount() > 0
          || reserves(m.getNestedTypeList(), m.getEnumTypeList())) {
        return true;
      }
    }
    return false;
  }

  /**
   * Refuses a new version referring to an older version of a type than {@code was}, those of the
   * version it follows, did: one only a soft-deleted version had. The way out names what to
   * {@code drop}. Under LOGICAL, which the statement's {@code policy} or the subject's may set,
   * the registry checks soft-deleted versions itself.
   */
  public static void refuseOlderReferences(TypeLookup lookup, String subject,
      List<SchemaReference> was, List<SchemaReference> now, String policy, String refused,
      String drop) {
    List<ColumnIds.Move> back = ColumnIds.moves(was, now).stream()
        .filter(m -> m.to() < m.from()).collect(Collectors.toList());
    if (back.isEmpty() || CompatibilityPolicy.forName(policy != null ? policy
        : lookup.config(subject).getCompatibilityPolicy()) == CompatibilityPolicy.LOGICAL) {
      return;
    }
    throw new TypeRefusedException(refused + ": " + back.stream()
        .map(m -> "its reference to type " + m.subject() + " would move back from version "
            + m.from() + " to version " + m.to() + ", the latest live one, because version "
            + m.from() + " is soft-deleted")
        .collect(Collectors.joining(", and ")) + "; a version cannot refer to an earlier "
        + "version of a type than the one it follows" + wayOut(back, drop));
  }

  /**
   * Refuses a new version where the subject's level and policy would refuse it after a dropped
   * version, {@code md}: the registry checks dropped versions only under LOGICAL. A refusal
   * starts with {@code refused} and names the version as {@code which}.
   */
  public static void refuseIncompatible(TypeLookup lookup, String subject, SchemaType format,
      SchemaMetadata md, LogicalType lt, String policy, String refused, String which) {
    Config config = lookup.config(subject);
    CompatibilityLevel level = CompatibilityLevel.forName(config.getCompatibilityLevel());
    CompatibilityPolicy applied = CompatibilityPolicy.forName(policy != null ? policy
        : config.getCompatibilityPolicy());
    if (level == null || applied == CompatibilityPolicy.LOGICAL) {
      return;
    }
    // The native schema the registry checks, or null if it cannot be made as the registry
    // makes it: left to the registry, which refuses it.
    ParsedSchema schema = ColumnIds.nativeOf(subject, format, lt);
    if (schema != null && Boolean.TRUE.equals(config.isNormalize())) {
      schema = schema.normalize();
    }
    if (schema == null) {
      return;
    }
    List<String> errors = schema.isCompatible(level, applied,
        List.of(new SimpleParsedSchemaHolder(lookup.parse(md))));
    if (!errors.isEmpty()) {
      throw new TypeRefusedException(refused + ": it is incompatible with " + which + ", whose "
          + "records it reads, at " + level + " compatibility: " + String.join("; ", errors));
    }
  }

  private boolean apply(AlterAction action) {
    if (!(action instanceof AlterAction.SetWith)) {
      refuseSeveralMessages();
    }
    if (action instanceof AlterAction.AddAttribute) {
      AlterAction.AddAttribute a = (AlterAction.AddAttribute) action;
      Schema one = parse(aliases -> TypeScripts.fieldScript(a.name(), a.field(), aliases))
          .getRootSchema();
      // The text is one attribute's: none follows it after a comma, and no clause of a
      // struct after a parenthesis that closes it.
      if (one.getFields().size() != 1 || !one.getParams().isEmpty() || one.getDoc() != null
          || !one.getTags().isEmpty() || !one.getRules().isEmpty()) {
        throw new TypeRefusedException("Attribute " + a.name() + " is given more than one "
            + "attribute's type and clauses: " + a.field());
      }
      Field field = one.getFields().get(0);
      List<Field> fields = new ArrayList<>(struct().getFields());
      int same = named(fields, a.name());
      if (same >= 0) {
        throw new TypeRefusedException("Type " + subject + " already has attribute "
            + fields.get(same).getName());
      }
      refuseDroppedName(a.name());
      Formats.refuseUnwritable(format, field);
      Map<String, Object> params = new LinkedHashMap<>(field.getParams());
      if (format == SchemaType.PROTOBUF) {
        params.put(Schema.PROTOBUF_FIELD_NUMBER, String.valueOf(nextNumber(fields)));
      }
      // A nullable attribute without a default reads as null for existing data.
      boolean nullDefault = !field.hasDefaultValue() && field.getSchema().isNullable();
      Schema schema = format == SchemaType.PROTOBUF
          ? numberBranches(field.getSchema(), nextNumber(fields) + 1) : field.getSchema();
      fields.add(new Field(field.getName(), schema, fields.size(),
          nullDefault ? null : field.getDefaultValue(),
          nullDefault || field.hasDefaultValue(), field.getDoc(), field.getTags(), params,
          namedRules(field)));
      added.add(field.getName());
      setStruct(fields);
      return true;
    } else if (action instanceof AlterAction.DropAttribute) {
      AlterAction.DropAttribute a = (AlterAction.DropAttribute) action;
      List<Field> fields = new ArrayList<>(struct().getFields());
      int i = named(fields, a.name());
      if (i < 0) {
        if (a.ifExists()) {
          return false;
        }
        throw new TypeRefusedException("Type " + subject + " has no attribute " + a.name());
      }
      for (Rule r : struct().getRules()) {
        if (Constraints.columns(r.getSql()).contains(fields.get(i).getName())) {
          throw new TypeRefusedException("Attribute " + a.name() + " is used by constraint "
              + (r.getName() != null ? r.getName() : "CHECK (" + r.getSql() + ")")
              + "; drop the constraint first");
        }
      }
      String name = fields.remove(i).getName();
      String previous = previousName(name);
      added.remove(name);
      dropped.add(name);
      dropped.add(previous);
      // Given to another attribute earlier in the statement, as in RENAME a TO b, ADD a.
      if (find(fields, previous) >= 0) {
        refuseDroppedName(previous);
      }
      setStruct(fields);
      return true;
    } else if (action instanceof AlterAction.RenameAttribute) {
      AlterAction.RenameAttribute a = (AlterAction.RenameAttribute) action;
      if (format == SchemaType.JSON) {
        throw new TypeRefusedException("Attributes of JSON type " + subject + " cannot be renamed, "
            + "since JSON Schema has no alias that keeps the attribute's data");
      }
      List<Field> fields = new ArrayList<>(struct().getFields());
      int i = named(fields, a.from());
      if (i < 0) {
        throw new TypeRefusedException("Type " + subject + " has no attribute " + a.from());
      }
      // A column may take its own name in another case, as no other's.
      int taken = named(fields, a.to());
      if (taken >= 0 && taken != i) {
        throw new TypeRefusedException("Type " + subject + " already has attribute "
            + fields.get(taken).getName());
      }
      refuseDroppedName(a.to());
      Field f = fields.get(i);
      // A macro variable of the new name would take the attribute's place in its body.
      List<Rule> using = new ArrayList<>(f.getRules());
      using.addAll(struct().getRules());
      for (Rule r : using) {
        if (Constraints.columns(r.getSql()).contains(f.getName())
            && Constraints.variables(r.getSql()).contains(a.to())) {
          throw new TypeRefusedException("Attribute " + a.from() + " cannot be renamed to "
              + a.to() + ": constraint " + (r.getName() != null ? r.getName()
              : "CHECK (" + r.getSql() + ")") + " uses " + a.to()
              + " as an iteration variable");
        }
      }
      Map<String, Object> params = new LinkedHashMap<>(f.getParams());
      if (format == SchemaType.AVRO) {
        List<String> aliases = new ArrayList<>(f.getAliases());
        aliases.add(f.getName());
        params.put(Schema.AVRO_ALIASES, String.join(",", aliases));
      }
      // The native names of the attribute's types follow its name; pin them to the old one.
      params.putAll(Formats.typeNameParams(format, edited(), subject, f.getName()));
      final Schema type = withBranchParams(f.getSchema(),
          Formats.branchTypeNameParams(format, edited(), subject, f.getName()));
      // CHECKs refer to the attribute by name, so they follow the rename.
      List<Rule> own = new ArrayList<>();
      for (Rule r : f.getRules()) {
        own.add(renamed(r, f.getName(), a.to()));
      }
      previousNames.put(a.to(), previousName(f.getName()));
      previousNames.remove(f.getName());
      if (added.remove(f.getName())) {
        added.add(a.to());
      }
      fields.set(i, Constraints.withRules(copy(f, a.to(), type, params), own));
      List<Rule> tableRules = new ArrayList<>();
      for (Rule r : struct().getRules()) {
        tableRules.add(renamed(r, f.getName(), a.to()));
      }
      setStruct(fields);
      setStructRules(tableRules);
      return true;
    } else if (action instanceof AlterAction.AlterAttributeType) {
      AlterAction.AlterAttributeType a = (AlterAction.AlterAttributeType) action;
      List<Field> fields = new ArrayList<>(struct().getFields());
      int i = named(fields, a.name());
      if (i < 0) {
        throw new TypeRefusedException("Type " + subject + " has no attribute " + a.name());
      }
      Schema type = parse(aliases -> TypeScripts.typeScript(a.type(), aliases))
          .getRootSchema();
      Formats.refuseUnwritable(format, type);
      Field f = fields.get(i);
      Map<String, Object> params = f.getParams();
      // A type of the same kind keeps the native names pinned on it; another kind gets new.
      if (sameKind(f.getSchema(), type)) {
        type = withBranchNameParams(type, f.getSchema());
      } else {
        params = new LinkedHashMap<>(params);
        params.keySet().removeIf(Schema::isTypeNameParam);
      }
      if (format == SchemaType.AVRO) {
        type = withDefaults(f.getSchema(), type);
      }
      if (format == SchemaType.PROTOBUF) {
        Messages root = new Messages(current, versions);
        type = type.getType() == Schema.Type.UNION
            ? renumbered(type, f.getSchema(), root, new int[] {nextNumber(fields)})
            : renumbered(type, f.getSchema(), numberIn(f.getParams()) > 0
              ? root.field(numberIn(f.getParams())) : root.field(f.getName()), null);
        // A union has no number of its own, only its branches; what replaces it gets a
        // new one, rather than the lowest free one, a branch's.
        if (type.getType() != Schema.Type.UNION
            && f.getSchema().getType() == Schema.Type.UNION) {
          params = new LinkedHashMap<>(params);
          params.put(Schema.PROTOBUF_FIELD_NUMBER, String.valueOf(nextNumber(fields)));
        }
      }
      // A null default does not apply once the attribute is NOT NULL.
      if (!type.isNullable() && f.hasDefaultValue() && f.getDefaultValue() == null) {
        fields.set(i, new Field(f.getName(), type, f.getPosition(), null, false, false,
            f.getDoc(), f.getTags(), params, f.getRules()));
      } else if (format == SchemaType.AVRO && f.getDefaultValue() != null) {
        fields.set(i, new Field(f.getName(), type, f.getPosition(),
            AvroDefaults.converted(f.getName(), f.getSchema(), type, f.getDefaultValue()),
            f.hasDefaultValue(), f.hasDerivedDefault(), f.getDoc(), f.getTags(), params,
            f.getRules()));
      } else {
        fields.set(i, copy(f, f.getName(), type, params));
      }
      setStruct(fields);
      return true;
    } else if (action instanceof AlterAction.SetDataType) {
      AlterAction.SetDataType a = (AlterAction.SetDataType) action;
      Schema target = target();
      if (target.getType() == Schema.Type.STRUCT) {
        throw new TypeRefusedException("Type " + subject
            + " is a struct; use ALTER ATTRIBUTE to change its attributes");
      }
      Schema type = parse(aliases -> TypeScripts.typeScript(a.type(), aliases))
          .getRootSchema();
      Formats.refuseUnwritable(format, type);
      // An enum keeps its nullability, comment and properties, as with ADD VALUE.
      if (target.getType() == Schema.Type.ENUM && type.getType() == Schema.Type.ENUM) {
        type = copyMetadata(target, Schema.createEnum(type.getEnumValues()))
            .setParams(target.getParams());
      }
      setTarget(type);
      return true;
    } else if (action instanceof AlterAction.AddValue) {
      AlterAction.AddValue a = (AlterAction.AddValue) action;
      Schema target = target();
      if (target.getType() != Schema.Type.ENUM) {
        throw new TypeRefusedException("Type " + subject + " is not an enum");
      }
      List<EnumValue> values = new ArrayList<>(target.getEnumValues());
      if (values.stream().anyMatch(v -> v.getSymbol().equals(a.symbol()))) {
        throw new TypeRefusedException("Enum " + subject + " already has value " + a.symbol());
      }
      int at = values.size();
      String anchor = a.before() != null ? a.before() : a.after();
      if (anchor != null) {
        at = -1;
        for (int i = 0; i < values.size(); i++) {
          if (values.get(i).getSymbol().equals(anchor)) {
            at = a.before() != null ? i : i + 1;
          }
        }
        if (at < 0) {
          throw new TypeRefusedException("Enum " + subject + " has no value " + anchor);
        }
      }
      values.add(at, new EnumValue(a.symbol()));
      setTarget(copyMetadata(target, Schema.createEnum(values)).setParams(target.getParams()));
      return true;
    } else if (action instanceof AlterAction.AddConstraint) {
      AlterAction.Check check = ((AlterAction.AddConstraint) action).check();
      Schema struct = struct();
      String name = constraintName(check, Constraints.base(subject), struct);
      List<Rule> rules = new ArrayList<>(struct.getRules());
      // Compiled again with the rest once all the actions are applied.
      rules.add(new Rule(name, check.message(), "true", check.sql()));
      setStructRules(rules);
      return true;
    } else if (action instanceof AlterAction.DropConstraint) {
      AlterAction.DropConstraint a = (AlterAction.DropConstraint) action;
      return dropConstraint(a.name(), a.ifExists());
    } else if (action instanceof AlterAction.AddAttributeCheck) {
      AlterAction.AddAttributeCheck a = (AlterAction.AddAttributeCheck) action;
      Field f = field(a.name());
      String name = constraintName(a.check(), Constraints.base(subject) + "_" + f.getName(),
          struct());
      List<Rule> rules = new ArrayList<>(f.getRules());
      rules.add(new Rule(name, a.check().message(), "true", a.check().sql()));
      setField(Constraints.withRules(f, rules));
      return true;
    } else if (action instanceof AlterAction.SetAttributeComment) {
      AlterAction.SetAttributeComment a = (AlterAction.SetAttributeComment) action;
      Field f = field(a.name());
      setField(new Field(f.getName(), f.getSchema(), f.getPosition(), f.getDefaultValue(),
          f.hasDefaultValue(), f.hasDerivedDefault(), a.comment(), f.getTags(), f.getParams(),
          f.getRules()));
      return true;
    } else if (action instanceof AlterAction.SetAttributeTags) {
      AlterAction.SetAttributeTags a = (AlterAction.SetAttributeTags) action;
      Field f = field(a.name());
      setField(new Field(f.getName(), f.getSchema(), f.getPosition(), f.getDefaultValue(),
          f.hasDefaultValue(), f.hasDerivedDefault(), f.getDoc(),
          a.tags().isEmpty() ? null : a.tags(), f.getParams(), f.getRules()));
      return true;
    } else if (action instanceof AlterAction.SetAttributeWith) {
      AlterAction.SetAttributeWith a = (AlterAction.SetAttributeWith) action;
      Field f = field(a.name());
      Map<String, Object> params = new LinkedHashMap<>(f.getParams());
      // An avro. or protobuf. property, such as a field number, an alias or a pinned type name,
      // is how the format reads the attribute's existing data, which this package keeps.
      for (Map.Entry<String, String> e : a.with().entrySet()) {
        Object was = params.get(e.getKey());
        if (Schema.isFormatNativeParam(e.getKey())
            && (was == null || !was.toString().equals(e.getValue()))) {
          throw new TypeRefusedException("Property " + e.getKey() + " of attribute "
              + f.getName() + " cannot be set: it is how the " + format + " format reads the "
              + "attribute's existing data");
        }
      }
      params.putAll(a.with());
      setField(copy(f, f.getName(), f.getSchema(), params));
      return true;
    } else if (action instanceof AlterAction.SetComment) {
      Schema copy = structOrEnumCopy();
      setTarget(copy.setDoc(((AlterAction.SetComment) action).comment()));
      return true;
    } else if (action instanceof AlterAction.SetTags) {
      List<String> tags = ((AlterAction.SetTags) action).tags();
      // The logical types DDL has no TAGS for an enum.
      if (!tags.isEmpty() && target().getType() == Schema.Type.ENUM) {
        throw new TypeRefusedException("Type " + subject + " is an enum, which cannot have tags");
      }
      Schema copy = structOrEnumCopy();
      setTarget(copy.setTags(tags.isEmpty() ? List.of() : tags));
      return true;
    } else {
      Map<String, String> with = new LinkedHashMap<>(((AlterAction.SetWith) action).with());
      String newFormat = removeIgnoreCase(with, FORMAT, null);
      if (newFormat != null && Formats.of(newFormat) != format) {
        throw new TypeRefusedException("The format of type " + subject + " cannot change");
      }
      String stated = removePolicy(with);
      if (stated != null) {
        policy = stated;
      }
      if (with.isEmpty()) {
        return false;
      }
      refuseSeveralMessages();
      Schema target = target();
      if (target.getType() != Schema.Type.STRUCT && target.getType() != Schema.Type.ENUM) {
        throw new TypeRefusedException("WITH properties " + with.keySet()
            + " are only allowed on a struct or enum type");
      }
      Map<String, Object> params = new LinkedHashMap<>(target.getParams());
      params.putAll(with);
      Schema copy = target.getType() == Schema.Type.STRUCT
          ? Schema.createStruct(target.getFields()) : Schema.createEnum(target.getEnumValues());
      setTarget(copyMetadata(target, copy).setParams(params));
      return true;
    }
  }

  // The most recent version, soft-deleted or not.
  private SchemaMetadata mostRecent(int version) {
    SchemaMetadata md = lookup.version(subject, version);
    if (md == null) {
      throw new TypeRefusedException("Could not read type " + subject + ": it has no version "
          + version);
    }
    return md;
  }

  /**
   * Moves a reference to a version soft-deleted since, which the registry refuses in a new
   * schema, to the latest live version of its type, as naming the type in a statement would,
   * whether or not the version edited is soft-deleted.
   */
  private void moveDeletedReferences() {
    for (int i = 0; i < references.size(); i++) {
      SchemaReference ref = references.get(i);
      if (isLive(ref)) {
        continue;
      }
      SchemaMetadata live = lookup.latest(TypeReferences.referenced(subject, ref.getSubject()));
      if (live == null) {
        throw new TypeRefusedException("Type " + subject + " refers to type " + ref.getSubject()
            + ", which has no live version; create it again, or drop the attributes of it");
      }
      references.set(i, new SchemaReference(ref.getName(), ref.getSubject(),
          live.getVersion()));
      moved.add(new ColumnIds.Move(ref.getName(), ref.getSubject(), ref.getVersion(),
          live.getVersion(), ref.getName()));
    }
    if (!moved.isEmpty()) {
      resolved.clear();
      resolved.putAll(TypeReferences.resolve(lookup, subject, references));
    }
  }

  // How a refusal starts, with the references moved from soft-deleted versions.
  private String refused() {
    String refused = "Type " + subject + " cannot be "
        + (actions == null ? "created again" : "changed") + " so";
    return moved.isEmpty() ? refused : refused + ", with " + moved.stream()
        .map(TypeAlter::described).collect(Collectors.joining(" and "));
  }

  private static String described(ColumnIds.Move move) {
    return "its reference to type " + move.subject() + " moved "
        + (move.to() < move.from() ? "back " : "") + "to version " + move.to()
        + ", the latest live one, because version " + move.from() + " is soft-deleted";
  }

  private boolean isLive(SchemaReference ref) {
    return lookup.versions(TypeReferences.referenced(subject, ref.getSubject()), false)
        .contains(ref.getVersion());
  }

  /**
   * Refuses a new Avro or JSON version whose attributes Schema Registry's provenance pairs with
   * the version edited otherwise than the statement means, or not at all: one kept or renamed
   * keeps its column, and one new to the statement takes a new one.
   */
  private void refuseOtherColumns(SchemaMetadata base, LogicalType edited, int version) {
    ColumnIds.Ids ids = format == SchemaType.PROTOBUF ? null
        : ColumnIds.compute(lookup, subject, base, format, edited, version, refused());
    if (ids == null) {
      return;
    }
    ColumnIds.Version next = ids.next();
    Map<List<Integer>, ColumnIds.Move> uses = new HashMap<>();
    Map<List<Integer>, List<Integer>> continued = ColumnIds.continued(ids,
        name -> added.contains(name) ? null : previousName(name), uses);
    Map<Integer, ProvenanceReport.Member> before = ids.base().byId();
    String at = " of version " + base.getVersion() + " of type " + subject;
    Set<List<Integer>> kept = new HashSet<>();
    for (ProvenanceReport.Member m : next.members().values()) {
      List<Integer> was = continued.get(m.getPath());
      ProvenanceReport.Member own = was == null ? null : ids.base().members().get(was);
      List<Integer> parent = parent(m.getPath(), next.members());
      List<Integer> use = use(m.getPath(), uses);
      if (use != null) {
        // Below a moved reference, as its type's versions pair it.
        own = before.get(ids.followed().get(m.getPath()));
      } else if (own != null && (!own.getKind().equals(m.getKind())
          || parent != null && !kept.contains(parent))) {
        // Of another kind, or below a new place, it is new.
        own = null;
      } else if (own != null) {
        kept.add(m.getPath());
      }
      ProvenanceReport.Member pairs = before.get(m.getId());
      if (own != null ? m.getId() == own.getId() : pairs == null) {
        continue;
      }
      if (use != null) {
        // Blamed on the deepest type below the use that changed, as a nested one moved.
        String type = next.deepest(m.getPath(), use.size(), ids.changed().keySet());
        throw new TypeRefusedException(movedRefusal(ids, type == null ? uses.get(use)
            : ids.changed().get(type), type, m, pairs, own, at));
      }
      String name = next.name(m.getPath());
      String why = pairs != null ? "would read another attribute's values, those of "
          + ids.base().name(pairs.getPath()) + at
          : "would not read its own values, those of " + ids.base().name(own.getPath()) + at;
      String other = previousName(name);
      if (pairs != null && m.getPath().size() == 1 && !other.equals(name)
          && ids.base().name(pairs.getPath()).equals(name)
          && previousName(other).equals(name)) {
        // Aliases carried from an earlier swap of the two read a swap back as no rename.
        String temp = name + "_tmp";
        for (int n = 2; find(struct().getFields(), temp) >= 0 || dropped.contains(temp); n++) {
          temp = name + "_tmp" + n;
        }
        throw new TypeRefusedException("Attribute " + name + " " + why + ": the aliases of "
            + name + " and " + other + " already swap their names, so they cannot be "
            + "swapped back in one statement; swap them in three: RENAME ATTRIBUTE "
            + name + " TO " + temp + ", then RENAME ATTRIBUTE " + other + " TO " + name
            + ", then RENAME ATTRIBUTE " + temp + " TO " + other);
      }
      throw new TypeRefusedException("Attribute " + name + " " + why);
    }
  }

  /**
   * The refusal of a place below a use of {@code type}, whose reference's move pairs it otherwise
   * than the type's versions do; each attribute is named in that type too, to tell them apart.
   */
  private String movedRefusal(ColumnIds.Ids ids, ColumnIds.Move move, String type,
                  ProvenanceReport.Member m, ProvenanceReport.Member pairs,
                  ProvenanceReport.Member own, String at) {
    Set<String> names = new HashSet<>(List.of(move.name(), move.was(), move.subject()));
    Set<String> defined = TypeReferences.definedNames(lookup, subject,
        new SchemaReference(move.name(), move.subject(), move.to()));
    if (defined != null) {
      names.addAll(defined);
    }
    String typeName = type == null ? move.subject() : imported(ids.next(), type);
    // A type the move changed below its own is named without versions.
    boolean versioned = names.contains(typeName);
    if (!versioned) {
      names = Set.of(typeName);
    }
    String shown = versioned ? move.subject() : typeName;
    String as = as(ids.next().nameIn(m.getPath(), namedAs(ids.next(), names)),
        versioned ? move.to() : 0, shown);
    String name = ids.next().name(m.getPath()) + as + (as.isEmpty() ? "" : ",");
    Set<String> was = namedAs(ids.base(), names);
    int from = versioned ? move.from() : 0;
    String why;
    if (pairs == null) {
      why = "would not read its own values, those of " + ids.base().name(own.getPath()) + at
          + as(ids.base().nameIn(own.getPath(), was), from, shown);
    } else {
      why = (own != null ? "would read the values of " : "would read another attribute's "
          + "values, those of ") + ids.base().name(pairs.getPath()) + at
          + as(ids.base().nameIn(pairs.getPath(), was), from, shown)
          + (own != null ? ", rather than its own, those of " + ids.base().name(own.getPath())
          : "");
    }
    String pairing = ": Schema Registry's provenance would pair its attributes otherwise than "
        + "versions " + Math.min(move.from(), move.to()) + " to "
        + Math.max(move.from(), move.to()) + " of " + move.subject() + " do, so attribute "
        + name + " " + why;
    if (!moved.contains(move)) {
      String refer = actions == null ? " cannot be created again referring to version "
          : " cannot refer to version ";
      return "Type " + subject + refer + move.to() + " of type " + move.subject() + pairing;
    }
    return "Type " + subject + " cannot be changed so, with " + described(move) + pairing
        + wayOut(List.of(move), "drop");
  }

  // Moved for a soft-deleted version, the way out is to give up the type, or its change.
  static String wayOut(List<ColumnIds.Move> moves, String drop) {
    StringBuilder out = new StringBuilder();
    for (ColumnIds.Move move : moves) {
      out.append("; ").append(drop).append(" the attributes of type ").append(move.subject());
      if (move.to() < move.from()) {
        out.append(", or register a new version of it carrying the changes of ")
            .append(move.from() == move.to() + 1 ? "version " + move.from()
              : "versions " + (move.to() + 1) + " to " + move.from());
      }
    }
    return out.toString();
  }

  // A named type by the reference it is imported by, as a JSON type's Ref<n>, or its own name.
  private static String imported(ColumnIds.Version v, String type) {
    String uri = v.type().getExternalImports().get(type);
    return uri == null ? type : uri.contains("#") ? uri.substring(0, uri.indexOf('#')) : uri;
  }

  // The names a version's named types go by that are one of names or imported by one.
  private static Set<String> namedAs(ColumnIds.Version v, Set<String> names) {
    Set<String> named = new HashSet<>(names);
    v.type().getNamedTypes().keySet().stream().filter(t -> names.contains(imported(v, t)))
        .forEach(named::add);
    return named;
  }

  // ", a of version 1 of com.acme.pair", a place's name as in a type at a version (0: unknown).
  private static String as(String name, int version, String type) {
    return name == null ? "" : ", " + name + " of " + (version > 0 ? "version " + version
        + " of " : "type ") + type;
  }

  // The use of a moved reference's type a place is below, or null for none.
  private static List<Integer> use(List<Integer> path, Map<List<Integer>, ?> uses) {
    for (int n = path.size() - 1; n >= 0; n--) {
      if (uses.containsKey(path.subList(0, n))) {
        return path.subList(0, n);
      }
    }
    return null;
  }

  // The nearest member above a place, or null for a top-level one.
  private static List<Integer> parent(List<Integer> path,
                      Map<List<Integer>, ProvenanceReport.Member> members) {
    for (int n = path.size() - 1; n > 0; n--) {
      if (members.containsKey(path.subList(0, n))) {
        return path.subList(0, n);
      }
    }
    return null;
  }

  // An attribute's name in the version edited, before this statement renamed it.
  private String previousName(String name) {
    return previousNames.getOrDefault(name, name);
  }

  // Read as one change, a name dropped and given again would make the new attribute the old one,
  // so its data would carry over (Avro, JSON) or the version could not read it (Avro aliases).
  private void refuseDroppedName(String name) {
    if (dropped.contains(name)) {
      throw new TypeRefusedException("Attribute " + name + " is dropped by this statement, so its "
          + "name cannot be given to another attribute in it; use two statements");
    }
  }

  /**
   * A JSON type read back names a referenced type {@code Ref<n>}, imported by the reference's
   * name.
   * Naming it by that name again writes a direct $ref, as CREATE TYPE does, rather than a $defs
   * entry that is itself a $ref, which the registry's compatibility check does not follow.
   */
  private void restoreDirectRefs() {
    Map<String, String> renames = new LinkedHashMap<>();
    externalImports.forEach((name, uri) -> {
      // Not to a name the logical types DDL cannot write, which stays imported.
      if (TypeReferences.nameable(uri)
          && references.stream().anyMatch(r -> r.getName().equals(uri))) {
        renames.put(name, uri);
      }
    });
    if (renames.isEmpty()) {
      return;
    }
    root = renameRefs(root, renames);
    Map<String, Schema> renamed = new LinkedHashMap<>();
    namedTypes.forEach((name, body) ->
        renamed.put(renames.getOrDefault(name, name), renameRefs(body, renames)));
    namedTypes.clear();
    namedTypes.putAll(renamed);
    Set<String> external = new LinkedHashSet<>();
    externalTypes.forEach(name -> external.add(renames.getOrDefault(name, name)));
    externalTypes.clear();
    externalTypes.addAll(external);
    externalImports.keySet().removeAll(renames.keySet());
  }

  /** A copy of a type with its references to named types renamed; the type itself if none. */
  private static Schema renameRefs(Schema type, Map<String, String> renames) {
    Schema copy;
    switch (type.getType()) {
      case NAMED_TYPE_REF:
        String name = renames.get(type.getQualifiedName());
        if (name == null) {
          return type;
        }
        copy = Schema.createNamedTypeRef(name);
        break;
      case STRUCT:
        List<Field> fields = new ArrayList<>();
        boolean changed = false;
        for (Field f : type.getFields()) {
          Schema s = renameRefs(f.getSchema(), renames);
          changed |= s != f.getSchema();
          fields.add(s == f.getSchema() ? f : copy(f, f.getName(), s, f.getParams()));
        }
        if (!changed) {
          return type;
        }
        copy = Schema.createStruct(fields);
        break;
      case UNION:
        List<UnionBranch> branches = new ArrayList<>();
        boolean branchChanged = false;
        for (UnionBranch b : type.getBranches()) {
          Schema s = renameRefs(b.getSchema(), renames);
          branchChanged |= s != b.getSchema();
          branches.add(s == b.getSchema() ? b
              : new UnionBranch(b.getName(), s, b.getDoc(), b.getParams()));
        }
        if (!branchChanged) {
          return type;
        }
        copy = Schema.createUnion(branches);
        break;
      case ARRAY:
      case MULTISET:
        Schema element = renameRefs(type.getElementType(), renames);
        if (element == type.getElementType()) {
          return type;
        }
        copy = type.getType() == Schema.Type.ARRAY
            ? Schema.createArray(element) : Schema.createMultiset(element);
        break;
      case MAP:
        Schema key = renameRefs(type.getKeyType(), renames);
        Schema value = renameRefs(type.getValueType(), renames);
        if (key == type.getKeyType() && value == type.getValueType()) {
          return type;
        }
        copy = Schema.createMap(key, value);
        break;
      default:
        return type;
    }
    return copyMetadata(type, copy).setParams(type.getParams());
  }

  /**
   * Drops the referenced types the edited type no longer uses, with their references, so that
   * the new version does not keep those types from being deleted.
   */
  private void dropUnusedReferences() {
    Set<String> reachable = new LinkedHashSet<>();
    List<Schema> work = new ArrayList<>(List.of(root));
    while (!work.isEmpty()) {
      Set<String> refs = new LinkedHashSet<>();
      LogicalType.collectNamedRefs(work.remove(work.size() - 1), refs);
      for (String name : refs) {
        if (reachable.add(name) && namedTypes.containsKey(name)) {
          work.add(namedTypes.get(name));
        }
      }
    }
    Set<String> unused = new LinkedHashSet<>(externalTypes);
    unused.removeAll(reachable);
    if (unused.isEmpty()) {
      return;
    }
    externalTypes.removeAll(unused);
    namedTypes.keySet().removeAll(unused);
    externalImports.keySet().removeAll(unused);
    // A JSON type read back names a referenced type Ref<n>, imported by the reference's name.
    Set<String> used = new LinkedHashSet<>(reachable);
    for (String name : reachable) {
      String uri = externalImports.get(name);
      if (uri != null) {
        used.add(uri.contains("#") ? uri.substring(0, uri.indexOf('#')) : uri);
      }
    }
    // A reference whose defined name cannot be told is kept.
    references.removeIf(ref -> {
      Set<String> names = TypeReferences.definedNames(lookup, subject, ref);
      return names != null && names.stream().noneMatch(used::contains);
    });
    resolved.clear();
    resolved.putAll(TypeReferences.resolve(lookup, subject, references));
  }

  // A type that may not refer to others, as a topic's own, is refused in the tool's words; any
  // other type can, in a context or not.
  private LogicalType parseWithReferences(TypeReferences.ScriptBuilder builder,
      Map<String, String> names) {
    if (noReferences != null) {
      LogicalType plain = TypeScripts.parse(builder.build(Map.of()));
      if (plain.getExternalTypes().isEmpty()) {
        return plain;
      }
      for (String type : plain.getExternalTypes()) {
        String refusal = noReferences.apply(type);
        if (refusal != null) {
          throw new TypeRefusedException(refusal);
        }
      }
    }
    return TypeReferences.parse(lookup, format, builder, subject, names);
  }

  /**
   * Parses a script, resolving the types it refers to and adding them to this type's. A type it
   * names moves to its latest version, which every attribute of that type takes, since a schema
   * refers to one version of a subject.
   */
  private LogicalType parse(TypeReferences.ScriptBuilder builder) {
    // A JSON type's reference to a type it already refers to keeps its name, which its $refs use.
    Map<String, String> names = new HashMap<>();
    if (format == SchemaType.JSON) {
      for (SchemaReference r : references) {
        names.put(TypeReferences.referenced(subject, r.getSubject()), r.getName());
      }
    }
    LogicalType lt = parseWithReferences(builder, names);
    externalTypes.addAll(lt.getExternalTypes());
    boolean moved = false;
    // A hand-written schema may name a reference otherwise than its subject, as Avro by its
    // record's full name; it moves under that name. JSON also needs the name its $ref uses.
    for (SchemaReference ref : lt.getReferences()) {
      boolean found = false;
      for (int i = 0; i < references.size(); i++) {
        SchemaReference r = references.get(i);
        // The same type however its subject is spelt: a stored reference's subject without
        // a context is in this type's context.
        if (!TypeReferences.referenced(subject, r.getSubject())
            .equals(TypeReferences.referenced(subject, ref.getSubject()))) {
          continue;
        }
        if (!r.getVersion().equals(ref.getVersion())) {
          references.set(i, new SchemaReference(r.getName(), r.getSubject(),
              ref.getVersion()));
          moved = true;
        }
        found |= format != SchemaType.JSON || r.getName().equals(ref.getName());
      }
      if (!found) {
        references.add(ref);
      }
    }
    if (moved) {
      resolved.clear();
      resolved.putAll(TypeReferences.resolve(lookup, subject, references));
    } else {
      lt.getResolvedReferences().forEach(resolved::putIfAbsent);
    }
    return lt;
  }

  private Field field(String name) {
    List<Field> fields = struct().getFields();
    int i = named(fields, name);
    if (i < 0) {
      throw new TypeRefusedException("Type " + subject + " has no attribute " + name);
    }
    return fields.get(i);
  }

  private void setField(Field field) {
    List<Field> fields = new ArrayList<>(struct().getFields());
    fields.set(find(fields, field.getName()), field);
    setStruct(fields);
  }

  private void setStructRules(List<Rule> rules) {
    Schema struct = struct();
    setTarget(copyMetadata(struct, Schema.createStruct(struct.getFields()))
        .setParams(struct.getParams()).setRules(rules));
  }

  private Schema structOrEnumCopy() {
    Schema target = target();
    if (target.getType() != Schema.Type.STRUCT && target.getType() != Schema.Type.ENUM) {
      throw new TypeRefusedException("Only a struct or enum type has a comment or tags; "
          + subject + " is neither");
    }
    Schema copy = target.getType() == Schema.Type.STRUCT
        ? Schema.createStruct(target.getFields()) : Schema.createEnum(target.getEnumValues());
    return copyMetadata(target, copy).setParams(target.getParams());
  }

  /**
   * An added attribute's constraints, named as CREATE TYPE names them; a name the type already
   * has is refused.
   */
  private List<Rule> namedRules(Field field) {
    Set<String> used = constraintNames(struct());
    for (Rule r : field.getRules()) {
      if (r.getName() != null && !used.add(r.getName().toLowerCase(Locale.ROOT))) {
        throw new TypeRefusedException("Constraint " + r.getName() + " already exists");
      }
    }
    List<Rule> rules = new ArrayList<>();
    for (Rule r : field.getRules()) {
      rules.add(r.getName() != null ? r : new Rule(Constraints.newName(
          Constraints.base(subject) + "_" + field.getName(), used), r.getDoc(), r.getExpr(),
          r.getSql()));
    }
    return rules;
  }

  /** The given name, unless taken, or one made from the prefix as PostgreSQL makes it. */
  private String constraintName(AlterAction.Check check, String prefix, Schema struct) {
    Set<String> used = constraintNames(struct);
    if (check.name() == null) {
      return Constraints.newName(prefix, used);
    }
    if (used.contains(check.name().toLowerCase(Locale.ROOT))) {
      throw new TypeRefusedException("Constraint " + check.name() + " already exists");
    }
    return check.name();
  }

  // The constraint names taken: a topic's own key and value types share one name space, as in
  // the CREATE TOPIC that made them.
  private Set<String> constraintNames(Schema struct) {
    Set<String> names = Constraints.names(struct);
    String other = !columns ? null
        : subject.endsWith(VALUE_SUFFIX)
          ? subject.substring(0, subject.length() - VALUE_SUFFIX.length()) + KEY_SUFFIX
        : subject.endsWith(KEY_SUFFIX)
          ? subject.substring(0, subject.length() - KEY_SUFFIX.length()) + VALUE_SUFFIX
        : null;
    SchemaMetadata md = other == null ? null : lookup.latest(other);
    if (md != null) {
      Schema otherRoot = targetOf(lookup.logicalType(other, md));
      if (otherRoot != null && otherRoot.getType() == Schema.Type.STRUCT) {
        names.addAll(Constraints.names(otherRoot));
      }
    }
    return names;
  }

  private boolean dropConstraint(String name, boolean ifExists) {
    Schema struct = struct();
    List<Rule> tableRules = new ArrayList<>(struct.getRules());
    if (tableRules.removeIf(r -> name.equalsIgnoreCase(r.getName()))) {
      setStructRules(tableRules);
      return true;
    }
    for (Field f : struct.getFields()) {
      List<Rule> rules = new ArrayList<>(f.getRules());
      if (rules.removeIf(r -> name.equalsIgnoreCase(r.getName()))) {
        setField(Constraints.withRules(f, rules));
        return true;
      }
    }
    if (ifExists) {
      return false;
    }
    throw new TypeRefusedException("Type " + subject + " has no constraint " + name);
  }

  private static Rule renamed(Rule rule, String from, String to) {
    return new Rule(rule.getName(), rule.getDoc(), rule.getExpr(),
        Constraints.rename(rule.getSql(), from, to));
  }

  /** Compiles every CHECK of a struct type against its attributes as they now are. */
  private void recompileRules() {
    Schema target = target();
    if (target.getType() != Schema.Type.STRUCT) {
      return;
    }
    List<Field> fields = new ArrayList<>();
    for (Field f : target.getFields()) {
      List<Rule> rules = new ArrayList<>();
      for (Rule r : f.getRules()) {
        rules.add(Constraints.compileColumn(r, f, namedTypes));
      }
      fields.add(Constraints.withRules(f, rules));
    }
    List<Rule> tableRules = new ArrayList<>();
    for (Rule r : target.getRules()) {
      tableRules.add(Constraints.compileTable(r, fields, namedTypes));
    }
    setStruct(fields);
    setStructRules(tableRules);
  }

  /** The type's own definition: the root, or the named type it refers to. */
  private Schema target() {
    if (root.getType() == Schema.Type.NAMED_TYPE_REF) {
      Schema named = namedTypes.get(root.getQualifiedName());
      if (named != null) {
        return named;
      }
    }
    return root;
  }

  private void setTarget(Schema schema) {
    if (root.getType() == Schema.Type.NAMED_TYPE_REF
        && namedTypes.containsKey(root.getQualifiedName())) {
      namedTypes.put(root.getQualifiedName(), schema);
    } else {
      root = schema;
    }
  }

  // A Protobuf type of several messages, read as all of them, has no one definition to change:
  // edited as a struct, it would be written back as one message, leaving the others unreadable.
  private void refuseSeveralMessages() {
    if (format == SchemaType.PROTOBUF && ProtoToLogicalTypeConverter.isMultiMessageRoot(root)) {
      throw new TypeRefusedException("Type " + subject + " is several Protobuf messages, with no "
          + "one definition to alter");
    }
  }

  private Schema struct() {
    Schema target = target();
    if (target.getType() != Schema.Type.STRUCT) {
      throw new TypeRefusedException("Type " + subject + " is not a struct; use SET DATA TYPE");
    }
    return target;
  }

  private void setStruct(List<Field> fields) {
    List<Field> positioned = new ArrayList<>(fields.size());
    for (int i = 0; i < fields.size(); i++) {
      Field f = fields.get(i);
      positioned.add(new Field(f.getName(), f.getSchema(), i, f.getDefaultValue(),
          f.hasDefaultValue(), f.hasDerivedDefault(), f.getDoc(), f.getTags(), f.getParams(),
          f.getRules()));
    }
    Schema struct = struct();
    setTarget(copyMetadata(struct, Schema.createStruct(positioned)).setParams(struct.getParams()));
  }

  /**
   * Records every top-level field's Protobuf number, so that the writer, which otherwise numbers
   * fields by position, keeps them through drops and additions.
   */
  private void recordProtobufNumbers() {
    Schema target = target();
    if (target.getType() != Schema.Type.STRUCT) {
      return;
    }
    Map<Object, Integer> implied = ProtoToLogicalTypeConverter.impliedFieldNumbers(target);
    List<Field> fields = new ArrayList<>();
    for (Field f : target.getFields()) {
      Map<String, Object> params = new LinkedHashMap<>(f.getParams());
      Schema type = f.getSchema();
      if (type.getType() == Schema.Type.UNION) {
        type = numberBranches(type, implied);
      } else {
        int n = number(f.getFieldNumber(), f, implied);
        if (n > 0) {
          params.put(Schema.PROTOBUF_FIELD_NUMBER, String.valueOf(n));
        }
      }
      fields.add(copy(f, f.getName(), type, params));
    }
    setStruct(fields);
  }

  private void recordUsedNumbers() {
    for (int version : lookup.versions(subject, true)) {
      SchemaMetadata md = lookup.version(subject, version);
      // A version registered in another format, by another client, numbers nothing.
      if (md == null || Formats.ofSchemaType(md.getSchemaType()) != SchemaType.PROTOBUF) {
        continue;
      }
      LogicalType lt = lookup.logicalType(subject, md);
      Message message = Message.of(targetOf(lt), lt.getNamedTypes());
      versions.add(message);
      if (message != null) {
        message.members().stream().filter(m -> m.number() > 0)
            .forEach(m -> usedNumbers.add(m.number()));
      }
    }
  }

  // A type's root, or the named type it refers to.
  private static Schema targetOf(LogicalType lt) {
    Schema root = lt.getRootSchema();
    if (root.getType() == Schema.Type.NAMED_TYPE_REF) {
      Schema named = lt.getNamedTypes().get(root.getQualifiedName());
      if (named != null) {
        return named;
      }
    }
    return root;
  }

  // A recorded Protobuf number, or the one the writer gives a member that records none; 0 if
  // neither is known.
  private static int number(Integer recorded, Object member, Map<?, Integer> implied) {
    Integer n = recorded != null ? recorded : implied.get(member);
    return n != null ? n : 0;
  }

  /** A Protobuf field or oneof member of a message, with its number and its type. */
  private static final class Member {

    private final String name;
    private final int number;
    private final Schema type;

    Member(String name, int number, Schema type) {
      this.name = name;
      this.number = number;
      this.type = type;
    }

    String name() {
      return name;
    }

    int number() {
      return number;
    }

    Schema type() {
      return type;
    }

    @Override
    public boolean equals(Object o) {
      if (!(o instanceof Member)) {
        return false;
      }
      Member other = (Member) o;
      return Objects.equals(name, other.name)
          && Objects.equals(number, other.number)
          && Objects.equals(type, other.type);
    }

    @Override
    public int hashCode() {
      return Objects.hash(name, number, type);
    }
  }

  /**
   * A Protobuf message as a version's logical type describes it: the fields of a struct and the
   * members of its unions' oneofs, with the named types of that version to follow them by.
   */
  private static final class Message {

    private final List<Member> members;
    private final Map<String, Schema> namedTypes;

    Message(List<Member> members, Map<String, Schema> namedTypes) {
      this.members = members;
      this.namedTypes = namedTypes;
    }

    List<Member> members() {
      return members;
    }

    Map<String, Schema> namedTypes() {
      return namedTypes;
    }

    @Override
    public boolean equals(Object o) {
      if (!(o instanceof Message)) {
        return false;
      }
      Message other = (Message) o;
      return Objects.equals(members, other.members)
          && Objects.equals(namedTypes, other.namedTypes);
    }

    @Override
    public int hashCode() {
      return Objects.hash(members, namedTypes);
    }


    // A struct's message; null for any other type.
    static Message of(Schema type, Map<String, Schema> namedTypes) {
      if (type == null || type.getType() != Schema.Type.STRUCT) {
        return null;
      }
      Map<Object, Integer> implied = ProtoToLogicalTypeConverter.impliedFieldNumbers(type);
      List<Member> members = new ArrayList<>();
      for (Field f : type.getFields()) {
        if (f.getSchema().getType() == Schema.Type.UNION) {
          for (UnionBranch b : f.getSchema().getBranches()) {
            members.add(new Member(memberName(b),
                number(b.getFieldNumber(), b, implied), b.getSchema()));
          }
        } else {
          members.add(new Member(f.getName(), number(f.getFieldNumber(), f, implied),
              f.getSchema()));
        }
      }
      return new Message(members, namedTypes);
    }

    Member find(String name) {
      return members.stream().filter(m -> m.name().equals(name)).findFirst().orElse(null);
    }

    Member find(int number) {
      return number <= 0 ? null
          : members.stream().filter(m -> m.number() == number).findFirst().orElse(null);
    }

    /**
     * The message a member of this one holds: through the messages the writer wraps arrays
     * in, a struct's, a union's own wrapper, whose oneof is its only content, or the entries
     * of a map or multiset; null for none.
     */
    Message message(Schema type) {
      type = elementOf(type);
      if (type == null) {
        return null;
      }
      switch (type.getType()) {
        case STRUCT:
          return of(type, namedTypes);
        case UNION:
          return of(entry(new Field(VALUE, type, 0)), namedTypes);
        case MAP: {
          // No logical type records the number of a key or value that is no union, once the
          // other's branches record theirs: the writer gives it the lowest one they leave
          // free.
          Message entry = of(entry(new Field("key", type.getKeyType(), 0),
              new Field(VALUE, type.getValueType(), 1)), namedTypes);
          Set<Integer> taken = entry.members().stream().map(Member::number)
              .collect(Collectors.toSet());
          List<Member> members = new ArrayList<>();
          int free = 1;
          for (Member m : entry.members()) {
            if (m.number() > 0) {
              members.add(m);
              continue;
            }
            while (taken.contains(free)) {
              free++;
            }
            taken.add(free);
            members.add(new Member(m.name(), free, m.type()));
          }
          return new Message(members, namedTypes);
        }
        case MULTISET: {
          // No logical type records the count's number: the writer gives it the lowest
          // one the key's members leave free.
          Message key = of(entry(new Field("key", type.getElementType(), 0)), namedTypes);
          Set<Integer> taken = key.members().stream().map(Member::number)
              .collect(Collectors.toSet());
          int count = 1;
          while (taken.contains(count)) {
            count++;
          }
          List<Member> members = new ArrayList<>(key.members());
          members.add(new Member(VALUE, count, Schema.create(Schema.Type.INT)));
          return new Message(members, namedTypes);
        }
        default:
          return null;
      }
    }

    // The enum a member of this one holds, through any arrays; null for none.
    Schema enumOf(Schema type) {
      type = elementOf(type);
      return type != null && type.getType() == Schema.Type.ENUM ? type : null;
    }

    // A type through its arrays and the named types it refers to.
    private Schema elementOf(Schema type) {
      while (type != null && (type.getType() == Schema.Type.ARRAY
          || type.getType() == Schema.Type.NAMED_TYPE_REF)) {
        type = type.getType() == Schema.Type.ARRAY ? type.getElementType()
            : namedTypes.get(type.getQualifiedName());
      }
      return type;
    }

    private static Schema entry(Field... fields) {
      return Schema.createStruct(List.of(fields));
    }
  }

  /**
   * The messages at one place in a type: in the version being edited, and in every version.
   * A field is followed through the messages the writer wraps arrays in, to the message of its
   * row, of its union's oneof, or of its map's or multiset's entries. In other versions it is
   * followed by the number it has in the edited one, so that a renamed attribute leads to its
   * own messages.
   */
  private static final class Messages {

    private final Message current;
    private final List<Message> all;
    private final List<Schema> enums;

    Messages(Message current, List<Message> all, List<Schema> enums) {
      this.current = current;
      this.all = all;
      this.enums = enums;
    }

    Message current() {
      return current;
    }

    List<Message> all() {
      return all;
    }

    List<Schema> enums() {
      return enums;
    }

    @Override
    public boolean equals(Object o) {
      if (!(o instanceof Messages)) {
        return false;
      }
      Messages other = (Messages) o;
      return Objects.equals(current, other.current)
          && Objects.equals(all, other.all)
          && Objects.equals(enums, other.enums);
    }

    @Override
    public int hashCode() {
      return Objects.hash(current, all, enums);
    }


    Messages(Message current, List<Message> all) {
      this(current, all, List.of());
    }

    Messages field(String name) {
      return field(current == null ? null : current.find(name), name);
    }

    // By number, for an attribute an earlier action of the statement may have renamed.
    Messages field(int number) {
      return field(current == null ? null : current.find(number), null);
    }

    private Messages field(Member member, String name) {
      List<Message> messages = new ArrayList<>();
      // The edited version's enum first, so that its numbers win.
      List<Schema> enums = new ArrayList<>();
      addEnum(enums, current, member);
      for (Message m : all) {
        Member same = m == null ? null : member != null && member.number() > 0
            ? m.find(member.number()) : name != null ? m.find(name) : null;
        addEnum(enums, m, same);
        messages.add(same == null ? null : m.message(same.type()));
      }
      return new Messages(member == null ? null : current.message(member.type()), messages,
          enums);
    }

    private static void addEnum(List<Schema> enums, Message message, Member member) {
      Schema e = member == null ? null : message.enumOf(member.type());
      if (e != null) {
        enums.add(e);
      }
    }

    // The number a field or branch has in the version being edited, or 0.
    int number(String name) {
      Member m = current == null ? null : current.find(name);
      return m == null ? 0 : m.number();
    }

    // The highest number any version used, so that a new field is numbered above it.
    int maxUsed() {
      int max = 0;
      for (Message m : all) {
        if (m != null) {
          for (Member member : m.members()) {
            max = Math.max(max, member.number());
          }
        }
      }
      return max;
    }
  }

  // The field a map's entry holds its value in.
  private static final String VALUE = "value";

  /**
   * A type given anew, with the Protobuf numbers that the fields and oneof branches it shares by
   * name have in the version being edited, at every level; the others are numbered above every
   * number their message has used, so that none reads a dropped field's data. A oneof's branches
   * belong to the message holding it, numbered by {@code next}: the enclosing row's counter, or
   * for a oneof in a wrapper message, null for one of its own.
   */
  private Schema renumbered(Schema type, Schema old, Messages at, int[] next) {
    Schema result;
    switch (type.getType()) {
      case STRUCT: {
        int[] inner = {at.maxUsed() + 1};
        List<Field> fields = new ArrayList<>();
        for (Field f : type.getFields()) {
          Field was = old != null && old.getType() == Schema.Type.STRUCT
              ? old.getField(f.getName()) : null;
          Schema wasType = was != null ? was.getSchema() : null;
          // A union attribute is a oneof of this row; others are messages of their own.
          Schema s = f.getSchema().getType() == Schema.Type.UNION
              ? renumbered(f.getSchema(), wasType, at, inner)
              : renumbered(f.getSchema(), wasType, at.field(f.getName()), null);
          Map<String, Object> params = new LinkedHashMap<>(f.getParams());
          if (s.getType() != Schema.Type.UNION) {
            int n = at.number(f.getName());
            params.put(Schema.PROTOBUF_FIELD_NUMBER,
                String.valueOf(n > 0 ? n : take(inner)));
          }
          fields.add(copy(f, f.getName(), s, params));
        }
        result = Schema.createStruct(fields);
        break;
      }
      case UNION: {
        int[] counter = next != null ? next : new int[] {at.maxUsed() + 1};
        List<UnionBranch> branches = new ArrayList<>();
        for (UnionBranch b : type.getBranches()) {
          // A branch's member may be named <oneof>_<branch>, so it is found by the
          // number its namesake had, or else by its namesake's native name: the reader
          // records numbers only where they are not sequential.
          UnionBranch was = old != null && old.getType() == Schema.Type.UNION
              ? branch(old, b.getName()) : null;
          int n = was == null ? 0 : numberIn(was.getParams()) > 0
              ? numberIn(was.getParams()) : at.number(memberName(was));
          Map<String, Object> params = new LinkedHashMap<>(b.getParams());
          params.put(Schema.PROTOBUF_FIELD_NUMBER,
              String.valueOf(n > 0 ? n : take(counter)));
          branches.add(new UnionBranch(b.getName(),
              renumbered(b.getSchema(), was != null ? was.getSchema() : null,
                at.field(n), null), b.getDoc(), params));
        }
        result = Schema.createUnion(branches);
        break;
      }
      case ARRAY:
        result = Schema.createArray(renumbered(type.getElementType(),
            old != null && old.getType() == Schema.Type.ARRAY ? old.getElementType() : null,
            at, null));
        break;
      case MULTISET: {
        // Entries of the element, as the key, and its count.
        boolean wasMultiset = old != null && old.getType() == Schema.Type.MULTISET;
        result = Schema.createMultiset(renumbered(type.getElementType(),
            wasMultiset ? old.getElementType() : null,
            entryPart(at, type.getElementType(), "key"), null));
        if (wasMultiset) {
          Message now = entryOf(result);
          refuseMovedEntryField("multiset's count", last(at.current()), last(now));
          if (!isUnion(old.getElementType()) && !isUnion(type.getElementType())) {
            refuseMovedEntryField("multiset's element", first(at.current()), first(now));
          }
        }
        break;
      }
      case MAP: {
        boolean wasMap = old != null && old.getType() == Schema.Type.MAP;
        // A key and a value that are unions are oneofs of the one entry message, numbered
        // by one counter.
        int[] entry = {at.maxUsed() + 1};
        result = Schema.createMap(
            renumbered(type.getKeyType(), wasMap ? old.getKeyType() : null,
              entryPart(at, type.getKeyType(), "key"),
              isUnion(type.getKeyType()) ? entry : null),
            renumbered(type.getValueType(), wasMap ? old.getValueType() : null,
              entryPart(at, type.getValueType(), VALUE),
              isUnion(type.getValueType()) ? entry : null));
        if (wasMap) {
          Message now = entryOf(result);
          if (!isUnion(old.getKeyType()) && !isUnion(type.getKeyType())) {
            refuseMovedEntryField("map's key", first(at.current()), first(now));
          }
          if (!isUnion(old.getValueType()) && !isUnion(type.getValueType())) {
            refuseMovedEntryField("map's value", last(at.current()), last(now));
          }
        }
        break;
      }
      case ENUM: {
        // A value keeps the number it had in any version, the edited one first; a new
        // one is numbered above every number used, so that none reads a dropped value.
        Map<String, Integer> numbers = new LinkedHashMap<>();
        int unused = 0;
        for (Schema e : at.enums()) {
          Map<EnumValue, Integer> implied =
              ProtoToLogicalTypeConverter.impliedEnumNumbers(e);
          for (EnumValue v : e.getEnumValues()) {
            Integer n = v.getEnumNumber() != null ? v.getEnumNumber() : implied.get(v);
            if (n != null) {
              numbers.putIfAbsent(v.getSymbol(), n);
              unused = Math.max(unused, n + 1);
            }
          }
        }
        // A number given is refused where it would read stored values otherwise: one a value
        // had is kept, and one a value had is no other's.
        Map<Integer, String> symbols = new HashMap<>();
        numbers.forEach((symbol, n) -> symbols.putIfAbsent(n, symbol));
        // A number given is no other value's to be given, nor a new value's to take.
        Map<Integer, String> givenTo = new HashMap<>();
        for (EnumValue v : type.getEnumValues()) {
          Object given = v.getParams().get(Schema.PROTOBUF_ENUM_NUMBER);
          if (given != null) {
            String same = givenTo.putIfAbsent(Integer.parseInt(given.toString()),
                v.getSymbol());
            if (same != null) {
              throw new TypeRefusedException("Values " + same + " and " + v.getSymbol()
                  + " cannot both be numbered " + given + ": they would be one value in "
                  + "the data");
            }
          }
        }
        List<EnumValue> values = new ArrayList<>();
        for (EnumValue v : type.getEnumValues()) {
          Map<String, Object> params = new LinkedHashMap<>(v.getParams());
          Integer had = numbers.get(v.getSymbol());
          Object given = params.get(Schema.PROTOBUF_ENUM_NUMBER);
          if (given != null) {
            int n = Integer.parseInt(given.toString());
            String other = symbols.get(n);
            if (had != null && had != n || had == null && other != null) {
              throw new TypeRefusedException("Value " + v.getSymbol() + " cannot be numbered "
                  + n + ": " + (had != null ? "it is numbered " + had
                  : "that is the number of value " + other) + " in the data written so far");
            }
          } else {
            while (had == null && givenTo.containsKey(unused)) {
              unused++;
            }
            params.put(Schema.PROTOBUF_ENUM_NUMBER,
                String.valueOf(had != null ? had : unused++));
          }
          values.add(new EnumValue(v.getSymbol(), v.getDoc(), params));
        }
        // Protobuf takes an enum only with its first value numbered 0.
        if (!values.isEmpty() && enumNumber(values.get(0)) != 0) {
          String zero = values.stream().filter(v -> enumNumber(v) == 0)
              .map(EnumValue::getSymbol).findFirst().orElse(null);
          String was = symbols.get(0);
          throw new TypeRefusedException("A Protobuf enum's first value is the one numbered "
              + "0: " + (zero != null ? "list " + zero + " first"
              : was != null ? was + ", which is numbered 0 in the data written so far, "
                + "cannot be dropped"
              : values.get(0).getSymbol() + " is numbered " + enumNumber(values.get(0))));
        }
        result = Schema.createEnum(values);
        break;
      }
      default:
        return type;
    }
    return copyMetadata(type, result).setParams(type.getParams());
  }

  // The entry message of a map or multiset, numbered as the writer will number it.
  private Message entryOf(Schema mapOrMultiset) {
    return new Message(List.of(), namedTypes).message(mapOrMultiset);
  }

  private static boolean isUnion(Schema type) {
    return type.getType() == Schema.Type.UNION;
  }

  // An entry's key, when it is no union, is its first member; its value or count, its last.
  private static Member first(Message entry) {
    return entry == null || entry.members().isEmpty() ? null : entry.members().get(0);
  }

  private static Member last(Message entry) {
    return entry == null || entry.members().isEmpty() ? null
        : entry.members().get(entry.members().size() - 1);
  }

  // No logical type records the number of an entry's key, value or count that is no union:
  // the writer gives it the lowest one the branches of a union beside it leave free. So a
  // change that frees a lower number would move it, and its stored data would not be read.
  private static void refuseMovedEntryField(String what, Member was, Member now) {
    if (was != null && now != null && was.number() > 0 && now.number() != was.number()) {
      throw new TypeRefusedException("A " + what + " would move from Protobuf field number "
          + was.number() + " to " + now.number() + ", and what is stored under "
          + was.number() + " would not be read: it takes the lowest number the branches of the "
          + "union beside it leave free, so a branch numbered below it cannot be dropped");
    }
  }

  private static int enumNumber(EnumValue value) {
    return Integer.parseInt(value.getParams().get(Schema.PROTOBUF_ENUM_NUMBER).toString());
  }

  // A union part of an entry is a oneof of the entry itself, its members numbered among its own.
  private static Messages entryPart(Messages entry, Schema part, String name) {
    return part.getType() == Schema.Type.UNION ? entry : entry.field(name);
  }

  // The name of a branch's oneof member, which the reader keeps as its native name.
  private static String memberName(UnionBranch b) {
    List<String> nativeNames = b.getNativeNames();
    return nativeNames != null && !nativeNames.isEmpty() ? nativeNames.get(0) : b.getName();
  }

  private static UnionBranch branch(Schema union, String name) {
    for (UnionBranch b : union.getBranches()) {
      if (b.getName().equals(name)) {
        return b;
      }
    }
    return null;
  }

  /**
   * A type given anew, in Avro, with the defaults Avro reads earlier data by. A nullable field
   * it adds to a struct that was already there gets a null default, as an added attribute
   * does. A field it keeps without giving it a default keeps the one it had, read under its
   * new type, as an attribute whose type changes does. Fields match by name, union branches
   * by branch name.
   */
  private static Schema withDefaults(Schema old, Schema type) {
    if (old == null || old.getType() != type.getType()) {
      return type;
    }
    Schema result;
    switch (type.getType()) {
      case STRUCT: {
        List<Field> fields = new ArrayList<>();
        for (Field f : type.getFields()) {
          Field was = old.getField(f.getName());
          if (was != null) {
            fields.add(defaulted(f, was, withDefaults(was.getSchema(), f.getSchema())));
          } else if (!f.hasDefaultValue() && f.getSchema().isNullable()) {
            fields.add(new Field(f.getName(), f.getSchema(), f.getPosition(), null,
                true, f.getDoc(), f.getTags(), f.getParams(), f.getRules()));
          } else {
            fields.add(f);
          }
        }
        result = Schema.createStruct(fields);
        break;
      }
      case UNION: {
        List<UnionBranch> branches = new ArrayList<>();
        for (UnionBranch b : type.getBranches()) {
          UnionBranch was = branch(old, b.getName());
          branches.add(was == null ? b : new UnionBranch(b.getName(),
              withDefaults(was.getSchema(), b.getSchema()), b.getDoc(),
              b.getParams()));
        }
        result = Schema.createUnion(branches);
        break;
      }
      case ARRAY:
        result = Schema.createArray(
            withDefaults(old.getElementType(), type.getElementType()));
        break;
      case MULTISET:
        result = Schema.createMultiset(
            withDefaults(old.getElementType(), type.getElementType()));
        break;
      case MAP:
        result = Schema.createMap(withDefaults(old.getKeyType(), type.getKeyType()),
            withDefaults(old.getValueType(), type.getValueType()));
        break;
      default:
        return type;
    }
    return copyMetadata(type, result).setParams(type.getParams());
  }

  private static Schema numberBranches(Schema union, Map<Object, Integer> implied) {
    List<UnionBranch> branches = new ArrayList<>();
    for (UnionBranch b : union.getBranches()) {
      Map<String, Object> params = new LinkedHashMap<>(b.getParams());
      int n = number(b.getFieldNumber(), b, implied);
      if (n > 0) {
        params.put(Schema.PROTOBUF_FIELD_NUMBER, String.valueOf(n));
      }
      branches.add(new UnionBranch(b.getName(), b.getSchema(), b.getDoc(), params));
    }
    return copyMetadata(union, Schema.createUnion(branches)).setParams(union.getParams());
  }

  /**
   * Numbers the branches of a union added as a oneof, from {@code next} on.
   */
  private static Schema numberBranches(Schema type, int next) {
    if (type.getType() != Schema.Type.UNION) {
      return type;
    }
    List<UnionBranch> branches = new ArrayList<>();
    for (UnionBranch b : type.getBranches()) {
      Map<String, Object> params = new LinkedHashMap<>(b.getParams());
      next = unreserved(next);
      params.put(Schema.PROTOBUF_FIELD_NUMBER, String.valueOf(next++));
      branches.add(new UnionBranch(b.getName(), b.getSchema(), b.getDoc(), params));
    }
    return copyMetadata(type, Schema.createUnion(branches)).setParams(type.getParams());
  }

  // Above every number any version used, so that a new field never takes the data of a
  // dropped one.
  private int nextNumber(List<Field> fields) {
    int max = usedNumbers.isEmpty() ? 0 : usedNumbers.last();
    for (Field f : fields) {
      max = Math.max(max, numberIn(f.getParams()));
      if (f.getSchema().getType() == Schema.Type.UNION) {
        for (UnionBranch b : f.getSchema().getBranches()) {
          max = Math.max(max, numberIn(b.getParams()));
        }
      }
    }
    return unreserved(max + 1);
  }

  // The number given, or the first after those Protobuf keeps for itself.
  private static int unreserved(int number) {
    return number >= RESERVED_FIRST && number <= RESERVED_LAST ? RESERVED_LAST + 1 : number;
  }

  // The next number of a counter, past those Protobuf keeps for itself.
  private static int take(int[] counter) {
    int number = unreserved(counter[0]);
    counter[0] = number + 1;
    return number;
  }

  private static int numberIn(Map<String, Object> params) {
    Object n = params.get(Schema.PROTOBUF_FIELD_NUMBER);
    return n == null ? 0 : Integer.parseInt(n.toString());
  }

  // The attribute a statement names; a column is named as the database names it, in any case.
  private int named(List<Field> fields, String name) {
    int i = find(fields, name);
    for (int j = 0; i < 0 && columns && j < fields.size(); j++) {
      if (fields.get(j).getName().equalsIgnoreCase(name)) {
        i = j;
      }
    }
    return i;
  }

  private static int find(List<Field> fields, String name) {
    for (int i = 0; i < fields.size(); i++) {
      if (fields.get(i).getName().equals(name)) {
        return i;
      }
    }
    return -1;
  }

  // A field of a struct given anew, of the type given, with the default the field it
  // continues had, read as Avro reads it under the new type, unless it is given one of its own.
  private static Field defaulted(Field f, Field was, Schema type) {
    if (f.hasDefaultValue() || !was.hasDefaultValue()) {
      return copy(f, f.getName(), type, f.getParams());
    }
    if (was.getDefaultValue() == null) {
      // A null default does not apply once the field is NOT NULL.
      return type.isNullable() ? new Field(f.getName(), type, f.getPosition(), null, true,
          was.hasDerivedDefault(), f.getDoc(), f.getTags(), f.getParams(), f.getRules())
          : copy(f, f.getName(), type, f.getParams());
    }
    return new Field(f.getName(), type, f.getPosition(),
        AvroDefaults.converted(f.getName(), was.getSchema(), type, was.getDefaultValue()),
        true, was.hasDerivedDefault(), f.getDoc(), f.getTags(), f.getParams(), f.getRules());
  }

  // A union with the given params added to its branches, by branch name.
  private static Schema withBranchParams(Schema type, Map<String, Map<String, Object>> params) {
    if (type.getType() != Schema.Type.UNION || params.isEmpty()) {
      return type;
    }
    List<UnionBranch> branches = new ArrayList<>();
    for (UnionBranch b : type.getBranches()) {
      Map<String, Object> merged = new LinkedHashMap<>(b.getParams());
      merged.putAll(params.getOrDefault(b.getName(), Map.of()));
      branches.add(new UnionBranch(b.getName(), b.getSchema(), b.getDoc(), merged));
    }
    return copyMetadata(type, Schema.createUnion(branches));
  }

  // A replacing union keeps the native names pinned on the branches it keeps.
  private static Schema withBranchNameParams(Schema type, Schema old) {
    if (type.getType() != Schema.Type.UNION) {
      return type;
    }
    Map<String, Map<String, Object>> params = new LinkedHashMap<>();
    for (UnionBranch b : old.getBranches()) {
      Map<String, Object> names = new LinkedHashMap<>(b.getParams());
      names.keySet().removeIf(key -> !Schema.isTypeNameParam(key));
      params.put(b.getName(), names);
    }
    return withBranchParams(type, params);
  }

  // The same type, down through collections; a struct's or union's members may differ, and so
  // may a leaf no format names, as a widened number.
  private static boolean sameKind(Schema a, Schema b) {
    if (a.getType() != b.getType()) {
      return isUnnamedLeaf(a) && isUnnamedLeaf(b);
    }
    switch (a.getType()) {
      case ARRAY:
      case MULTISET:
        return sameKind(a.getElementType(), b.getElementType());
      case MAP:
        return sameKind(a.getKeyType(), b.getKeyType())
            && sameKind(a.getValueType(), b.getValueType());
      default:
        return true;
    }
  }

  // A type that is no collection, struct or union, and has no native name: an enum has one,
  // and so has a fixed-length binary, an Avro fixed.
  private static boolean isUnnamedLeaf(Schema schema) {
    switch (schema.getType()) {
      case STRUCT:
      case ENUM:
      case UNION:
      case MAP:
      case ARRAY:
      case MULTISET:
      case BINARY:
      case NAMED_TYPE_REF:
        return false;
      default:
        return true;
    }
  }

  private static Field copy(Field f, String name, Schema schema, Map<String, Object> params) {
    return new Field(name, schema, f.getPosition(), f.getDefaultValue(), f.hasDefaultValue(),
        f.hasDerivedDefault(), f.getDoc(), f.getTags(), params, f.getRules());
  }

  private static Schema copyMetadata(Schema from, Schema to) {
    to.setNullable(from.isNullable());
    to.setDoc(from.getDoc());
    to.setTags(from.getTags());
    to.setRules(from.getRules());
    return to;
  }
}
