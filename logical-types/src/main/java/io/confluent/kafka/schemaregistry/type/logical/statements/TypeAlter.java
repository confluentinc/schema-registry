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

package io.confluent.kafka.schemaregistry.type.logical.statements;

import com.fasterxml.jackson.core.JsonPointer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.MissingNode;
import com.google.protobuf.DescriptorProtos.DescriptorProto;
import com.google.protobuf.DescriptorProtos.EnumDescriptorProto;
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto;
import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Descriptors.OneofDescriptor;
import io.confluent.kafka.schemaregistry.CompatibilityLevel;
import io.confluent.kafka.schemaregistry.CompatibilityPolicy;
import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.SimpleParsedSchemaHolder;
import io.confluent.kafka.schemaregistry.client.SchemaMetadata;
import io.confluent.kafka.schemaregistry.client.rest.entities.Config;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaReference;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.RegisterSchemaRequest;
import io.confluent.kafka.schemaregistry.json.JsonSchema;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import io.confluent.kafka.schemaregistry.type.logical.LogicalType;
import io.confluent.kafka.schemaregistry.type.logical.LogicalTypeConversion;
import io.confluent.kafka.schemaregistry.type.logical.Rule;
import io.confluent.kafka.schemaregistry.type.logical.Schema;
import io.confluent.kafka.schemaregistry.type.logical.Schema.EnumValue;
import io.confluent.kafka.schemaregistry.type.logical.Schema.Field;
import io.confluent.kafka.schemaregistry.type.logical.Schema.UnionBranch;
import io.confluent.kafka.schemaregistry.type.logical.SchemaType;
import io.confluent.kafka.schemaregistry.type.logical.json.JsonToLogicalTypeConverter;
import io.confluent.kafka.schemaregistry.type.logical.protobuf.CommonConstants;
import io.confluent.kafka.schemaregistry.type.logical.protobuf.LogicalTypeToProtoConverter;
import io.confluent.kafka.schemaregistry.type.logical.protobuf.ProtoToLogicalTypeConverter;
import io.confluent.kafka.schemaregistry.utils.JacksonMapper;
import io.confluent.kafka.schemaregistry.type.logical.provenance.ProvenanceReport;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.IntFunction;
import java.util.function.Predicate;
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
  private static final int MAX_NUMBER = 536_870_911;
  // The words of a JSON schema that say nothing of what a value is.
  private static final Set<String> SAYS_NOTHING = Set.of("$ref", "title", "description",
      "$comment", "default", "examples", "deprecated", "readOnly", "writeOnly",
      "contentMediaType", "contentEncoding", "connect.index", "logical.ref");
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

  /** The new version a CREATE TYPE or ALTER TYPE makes, for the caller to register. */
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

    /**
     * The version edited: the most recent one, soft-deleted or not. Of a created type, the last
     * version it had when it was dropped, or null if it is new.
     */
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

    /**
     * The new version, or null if the statement makes none: it changes only the subject's
     * settings, or leaves the type as it is stored.
     */
    public LogicalType edited() {
      return edited;
    }

    /**
     * The compatibility policy to give the subject, or null to leave it: the statement's, or
     * for a created type whose statement names none, the caller's.
     */
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
  // The named types of the type text parsed last, those it refers to among them.
  private Map<String, Schema> parsedTypes = Map.of();
  private Map<String, String> parsedImports = Map.of();
  private final Map<String, KnownTypes> referredTypes = new HashMap<>();
  private final Map<String, TypeReferences.Referred> referredRead = new HashMap<>();
  private final Set<String> externalTypes = new LinkedHashSet<>();
  private final Map<String, String> externalImports = new LinkedHashMap<>();
  private final List<SchemaReference> references = new ArrayList<>();
  private final Map<String, String> resolved = new LinkedHashMap<>();
  // Where the $refs of the JSON schema edited go, by the names the type is read by.
  private Map<String, Set<String>> placesStored = Map.of();
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
  // The version the statement edits.
  private SchemaMetadata editing;
  // Whether the Avro names are pinned as the type stands: no action since has given it a type
  // that could have a name of its own.
  private boolean avroNamesPinned;
  // The type an action gives anew, whose names are no more the type's; null outside such
  // an action.
  private Schema replacing;
  // Whether a name the type had for a definition is now a document's: the reference that
  // had the definition may be of no more use, though no name is.
  private boolean renamed;
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
  static int nextVersion(TypeLookup lookup, String subject) {
    return lookup.versions(subject, true).stream().mapToInt(Integer::intValue).max()
        .orElse(0) + 1;
  }

  /** Removes a property by its name, ignoring case, returning its value or the fallback. */
  static String removeIgnoreCase(Map<String, String> props, String key,
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
  static String removePolicy(Map<String, String> with) {
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
   * Refuses a Protobuf type created again over its dropped versions if the last of them has
   * what a statement's type cannot carry, or is several messages, as an ALTER of it is
   * refused: the new version would lose it, and stored values could be read wrongly.
   */
  static void refuseUncarriedAfterDrop(TypeLookup lookup, String subject, SchemaMetadata md) {
    new TypeAlter(lookup, subject).refuseUncarried(md, "created again");
    // Whatever the subject's compatibility level, which may be none.
    if (ProtoToLogicalTypeConverter.isMultiMessageRoot(
        lookup.logicalType(subject, md).getRootSchema())) {
      throw new TypeRefusedException("Type " + subject + " cannot be created again: it is "
          + "several Protobuf messages, and would be registered as one, leaving the records "
          + "of the others unreadable. Register its new version as a Protobuf schema");
    }
  }

  /**
   * A Protobuf type created again over its dropped versions, numbered as an ALTER of the last of
   * them would number it: an attribute keeps the number of its namesake, at every level, and any
   * other is numbered above every number used, so none reads another's data.
   */
  static LogicalType numberedAfterDrop(TypeLookup lookup, String subject, SchemaMetadata md,
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
    setStruct(renumbered(target(), old, at, null, null).getFields());
    // And the name and package its last version's root message had, which a record written
    // by it is read by: under another it is no record of the type.
    boolean named = last.getName() != null && lt.getName() != null;
    return new LogicalType(named ? last.getName() : lt.getName(),
        named ? last.getNamespace() : lt.getNamespace(), root, namedTypes,
        lt.getExternalTypes(), lt.getExternalImports(), lt.getReferences(),
        lt.getResolvedReferences(), lt.getDefaultValues());
  }

  /**
   * An Avro type created again over its dropped versions, with a null default for each nullable
   * field it adds to the last of them, as an ALTER of it gives one: Avro reads their data by it.
   */
  static LogicalType defaultedAfterDrop(TypeLookup lookup, String subject, SchemaMetadata md,
                       LogicalType lt) {
    return new TypeAlter(lookup, subject).defaultAfterDrop(md, lt);
  }

  private LogicalType defaultAfterDrop(SchemaMetadata md, LogicalType lt) {
    Schema old = targetOf(lookup.logicalType(subject, md));
    root = lt.getRootSchema();
    namedTypes.putAll(lt.getNamedTypes());
    setTarget(withDefaults(old, target(), null));
    return new LogicalType(lt.getName(), lt.getNamespace(), root, namedTypes,
        lt.getExternalTypes(), lt.getExternalImports(), lt.getReferences(),
        lt.getResolvedReferences(), lt.getDefaultValues());
  }

  /**
   * Refuses an Avro or JSON type created again over its dropped versions whose attributes Schema
   * Registry's provenance pairs with the last of them otherwise than an ALTER of it would, as if
   * it kept every attribute of the same name and added the others.
   */
  static void refuseOtherColumnsAfterDrop(TypeLookup lookup, String subject,
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
    // And the names the others are stored under, which this statement may have changed.
    target.getFields().stream().map(Field::getName).filter(n -> !added.contains(n))
        .map(this::previousName).forEach(names::add);
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
    List<Integer> versions = lookup.versions(subject, true);
    int version = versions.stream().mapToInt(Integer::intValue).max().orElse(0) + 1;
    SchemaMetadata md = lookup.latest(subject);
    if (md == null) {
      throw new TypeRefusedException("Unknown type " + subject);
    }
    // A new version continues the most recent one, soft-deleted or not, as provenance pairs it.
    boolean softDeleted = md.getVersion() < version - 1;
    if (softDeleted) {
      md = mostRecent(version - 1);
    }
    editing = md;
    format = Formats.ofSchemaType(md.getSchemaType());
    original = lookup.logicalType(subject, md);
    root = original.getRootSchema();
    namedTypes.putAll(original.getNamedTypes());
    externalTypes.addAll(original.getExternalTypes());
    externalImports.putAll(original.getExternalImports());
    references.addAll(original.getReferences());
    resolved.putAll(original.getResolvedReferences());
    if (format == SchemaType.PROTOBUF) {
      pinProtobufNames(md);
      recordProtobufNumbers();
      current = Message.of(target(), namedTypes);
      // A move takes no number: every version's numbers are read only for a statement
      // that may take one.
      if (!actions.stream().allMatch(a -> a instanceof AlterAction.MoveAttribute)) {
        recordUsedNumbers();
      }
    }

    List<String> order = attributeNames();
    boolean changesSchema = false;
    boolean moved = false;
    for (AlterAction action : actions) {
      if (action instanceof AlterAction.MoveAttribute) {
        moved |= apply(action);
      } else {
        changesSchema |= apply(action);
      }
    }
    // Moves that leave the attributes in the order they were in change nothing.
    changesSchema |= moved && !order.equals(attributeNames());
    // A struct of no attributes is no schema in any format; judged on what the actions leave.
    if (target().getType() == Schema.Type.STRUCT && target().getFields().isEmpty()) {
      throw new TypeRefusedException("Type " + subject + " would have no attributes; drop the "
          + (columns ? "topic" : "type") + " instead");
    }

    LogicalType edited = null;
    if (changesSchema) {
      if (format == SchemaType.PROTOBUF) {
        refuseUncarried(md, "altered");
      }
      dropStaleAliases();
      recompileRules();
      if (format == SchemaType.JSON) {
        refuseReadAsOne(md);
        restoreDirectRefs();
      }
      dropUnusedReferences();
      moveDeletedReferences();
      refuseOlderReferences(lookup, subject, original.getReferences(), references, policy,
          "Type " + subject + " cannot be changed so", "drop");
      if (format == SchemaType.JSON) {
        refuseNewOverlaps(null, null);
      }
      edited = edited();
      ParsedSchema stored = lookup.parse(md);
      ParsedSchema written;
      try {
        written = ColumnIds.nativeOf(subject, format, edited, refused());
      } catch (TypeRefusedException e) {
        // No schema can be made of a name that is two types or two versions: say that.
        TypeReferences.requireOneOfEach(lookup, subject, references, refused());
        refuseWrappedOneof(stored);
        throw e;
      }
      if (writesWhatIsStored(md, stored, written)) {
        edited = null;
      } else {
        TypeReferences.requireOneOfEach(lookup, subject, references, refused());
        if (format == SchemaType.PROTOBUF) {
          refuseWrittenOtherwise(stored, written, "altered");
          refuseTakenNames(stored, written);
        }
        if (format == SchemaType.JSON) {
          refuseWrittenOtherwise(stored, written, "altered");
          // And by the definitions of the documents referred to, as the two schemas have
          // them: each at the version its reference is to.
          refuseNewOverlaps(stored, written);
          if (written instanceof JsonSchema) {
            refuseReadAsOne((JsonSchema) written, "changed so");
          }
        }
        refuseOtherColumns(md, edited, version);
        refuseIncompatible(lookup, subject, format, md, softDeleted, versions, edited, policy,
            refused(), v -> "version " + v + " of type " + subject);
        check.accept(edited);
      }
    }
    return new Result(format, md, version, edited, policy);
  }

  // Whether the statement leaves the version edited as it is stored: the same schema, the same
  // references, and each property the writer sets. Such a statement makes no version, however
  // its actions came to it: a type said as it is read, or an attribute added and dropped.
  private static boolean writesWhatIsStored(SchemaMetadata md, ParsedSchema stored,
      ParsedSchema written) {
    if (!written.canonicalString().equals(stored.canonicalString())
        && !declaresTheSameInAnotherOrder(stored, written)
        || !new HashSet<>(written.references()).equals(new HashSet<>(stored.references()))) {
      return false;
    }
    Map<String, String> had = md.getMetadata() == null ? null : md.getMetadata().getProperties();
    Map<String, String> sets = written.metadata() == null ? null
        : written.metadata().getProperties();
    return sets == null || sets.isEmpty()
        || had != null && had.entrySet().containsAll(sets.entrySet());
  }

  // The file that declares the options metadata is written in: a writer before this one
  // imported it only for the file's own option, and a type stored so is the same type.
  private static final String META_IMPORT = "import \"confluent/meta.proto\";";

  // A Protobuf message's nested messages and enums are written in the order of the attributes
  // they are of, and a oneof is read after the fields beside it: the two files may so declare
  // the same types in another order. The same lines, and the same messages by their names.
  private static boolean declaresTheSameInAnotherOrder(ParsedSchema stored,
      ParsedSchema written) {
    if (!(stored instanceof ProtobufSchema) || !(written instanceof ProtobufSchema)
        || !sortedLines(stored).equals(sortedLines(written))) {
      return false;
    }
    try {
      Descriptor was = ((ProtobufSchema) stored).toDescriptor();
      Descriptor now = ((ProtobufSchema) written).toDescriptor();
      return was != null && now != null
          && unordered(was.getFile().toProto()).equals(unordered(now.getFile().toProto()));
    } catch (RuntimeException e) {
      return false;
    }
  }

  private static List<String> sortedLines(ParsedSchema schema) {
    return schema.canonicalString().lines()
        .filter(line -> !line.isBlank() && !line.equals(META_IMPORT))
        .sorted().collect(Collectors.toList());
  }

  private static FileDescriptorProto unordered(FileDescriptorProto file) {
    // The files imported are told by the lines.
    FileDescriptorProto.Builder builder = file.toBuilder().clearMessageType()
        .clearDependency().clearPublicDependency().clearWeakDependency();
    file.getMessageTypeList().forEach(message -> builder.addMessageType(unordered(message)));
    return builder.build();
  }

  // A message with the types it declares, at every level, in the order of their names.
  private static DescriptorProto unordered(DescriptorProto message) {
    DescriptorProto.Builder builder = message.toBuilder().clearNestedType().clearEnumType();
    message.getNestedTypeList().stream().map(TypeAlter::unordered)
        .sorted(Comparator.comparing(DescriptorProto::getName))
        .forEach(builder::addNestedType);
    message.getEnumTypeList().stream()
        .sorted(Comparator.comparing(EnumDescriptorProto::getName))
        .forEach(builder::addEnumType);
    return builder.build();
  }

  // A new version is written from the logical type, which carries neither what only a proto2
  // file has, such as required fields and defaults, nor the numbers and names a message or
  // enum reserves, nor which of its encodings an integer has. The first would be lost, the
  // second free for a new field to take, and the third changed, so stored values read wrongly.
  private void refuseUncarried(SchemaMetadata md, String done) {
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
        : reencoded(file.getMessageTypeList());
    if (lost != null) {
      throw new TypeRefusedException("Type " + subject + " cannot be " + done + ": " + lost
          + ". Register its new version as a Protobuf schema");
    }
  }

  // Four kinds of field are written otherwise than the file declares them. A map is written
  // as a repeated field of an entry message: the same bytes, and another kind of field to
  // the registry and to generated code. A google.protobuf wrapper is written as the scalar
  // it wraps, which is other bytes. An entry's key and value are numbered as the writer
  // numbers them. And a oneof wrapped in a message of its own is written as a oneof of the
  // message that has the field. A version that still has such a field is refused.
  private void refuseWrittenOtherwise(ParsedSchema stored, ParsedSchema written, String done) {
    if (format == SchemaType.JSON) {
      refuseEntriesWrittenAsObject(stored, written, done);
      return;
    }
    if (!(stored instanceof ProtobufSchema) || !(written instanceof ProtobufSchema)) {
      return;
    }
    Descriptor was;
    Descriptor now;
    try {
      was = ((ProtobufSchema) stored).toDescriptor();
      now = ((ProtobufSchema) written).toDescriptor();
    } catch (RuntimeException e) {
      // A file that is no valid Protobuf is refused as that (refuseTakenNames).
      return;
    }
    if (was == null || now == null) {
      return;
    }
    Set<String> seen = new HashSet<>();
    // The root's fields are the attributes, which a statement tells what becomes of; a type
    // created again has no statement to tell it.
    String field = writtenOtherwise(was, now, seen, root == null ? null : unionsKept());
    // And the messages beside the root that no field of it comes to, which are written by
    // their names.
    for (Descriptor peer : was.getFile().getMessageTypes()) {
      if (field == null) {
        field = writtenOtherwise(peer, now.getFile().findMessageTypeByName(peer.getName()),
            seen, null);
      }
    }
    if (field != null) {
      throw new TypeRefusedException("Type " + subject + " cannot be " + done + ": field "
          + field + ". Drop the attribute, or register its new version as a Protobuf schema");
    }
  }

  static void refuseWrittenOtherwiseAfterDrop(TypeLookup lookup, String subject,
      SchemaType format, SchemaMetadata md, LogicalType lt) {
    TypeAlter alter = new TypeAlter(lookup, subject);
    alter.format = format;
    alter.refuseWrittenOtherwise(lookup.parse(md), ColumnIds.nativeOf(subject, format, lt,
        "Type " + subject + " cannot be created again so"), "created again");
  }

  // A JSON map keyed by strings is written as an object, and is read from an array of key
  // and value entries too, as Connect writes one whose key may be null. One stored so would
  // be written as an object, which its stored values are not: a version that still has it
  // is refused.
  private void refuseEntriesWrittenAsObject(ParsedSchema stored, ParsedSchema written,
      String done) {
    LogicalType was;
    LogicalType now;
    try {
      was = LogicalTypeConversion.toLogicalType(stored);
      now = LogicalTypeConversion.toLogicalType(written);
    } catch (RuntimeException e) {
      // A schema that cannot be read is refused as that.
      return;
    }
    String in = new EntriesWalk(was, now).find(was.getRootSchema(), now.getRootSchema(), null);
    if (in != null) {
      // A type that is no row has no attribute to tell, or to drop.
      throw new TypeRefusedException("Type " + subject + " cannot be " + done + ": "
          + (in.isEmpty() ? "it is, or holds," : in + " has") + " a map stored as an array "
          + "of key and value entries, which would be written as an object, so its stored "
          + "values would be read wrongly. "
          + (in.isEmpty() ? "Register" : "Drop the attribute, or register")
          + " its new version as a JSON schema");
    }
  }

  // Finds the first such map that the type written in its place still has as a map, and
  // tells the attribute it is in. A named type is come to from the attributes that are of
  // it: one that none is of any more is written as no value is, and is not judged. Nor is
  // a type referred to, which no statement on this one writes.
  private static final class EntriesWalk {
    private final LogicalType stored;
    private final Map<String, Schema> wasNamed;
    private final Map<String, Schema> nowNamed;
    private final Set<String> seen = new HashSet<>();

    EntriesWalk(LogicalType was, LogicalType now) {
      this.stored = was;
      this.wasNamed = was.getNamedTypes();
      this.nowNamed = now.getNamedTypes();
    }

    // A map of other keys is stored as entries and written as them: only one keyed by
    // strings is written otherwise.
    private static boolean keyedByStrings(Schema key) {
      return key.getType() == Schema.Type.VARCHAR || key.getType() == Schema.Type.CHAR;
    }

    // One given another kind of type is written anew.
    String find(Schema was, Schema now, String in) {
      if (was != null && was.getType() == Schema.Type.NAMED_TYPE_REF) {
        String name = was.getQualifiedName();
        if (stored.isExternal(name)) {
          return null;
        }
        was = wasNamed.get(name);
        now = now != null && now.getType() == Schema.Type.NAMED_TYPE_REF
            ? nowNamed.get(now.getQualifiedName()) : now;
        // A type that names itself is come to again while it is walked.
        if (!seen.add(name + " " + System.identityHashCode(now))) {
          return null;
        }
      } else if (now != null && now.getType() == Schema.Type.NAMED_TYPE_REF) {
        now = nowNamed.get(now.getQualifiedName());
      }
      if (was == null || now == null || was.getType() != now.getType()) {
        return null;
      }
      String found = null;
      switch (was.getType()) {
        case STRUCT:
          for (Field f : was.getFields()) {
            Field kept = now.getField(f.getName());
            if (found == null && kept != null) {
              found = find(f.getSchema(), kept.getSchema(),
                  in != null ? in : "attribute " + Formats.segment(f.getName()));
            }
          }
          return found;
        case UNION:
          for (UnionBranch b : was.getBranches()) {
            UnionBranch kept = branch(now, b.getName());
            if (found == null && kept != null) {
              found = find(b.getSchema(), kept.getSchema(), in);
            }
          }
          return found;
        case ARRAY:
          return find(was.getElementType(), now.getElementType(), in);
        case MULTISET:
          if (isEntries(was.getElementNativeNames()) && keyedByStrings(was.getElementType())
              && !isEntries(now.getElementNativeNames())) {
            return in != null ? in : "";
          }
          return find(was.getElementType(), now.getElementType(), in);
        case MAP:
          if (isEntries(was.getKeyNativeNames()) && keyedByStrings(was.getKeyType())
              && !isEntries(now.getKeyNativeNames())) {
            return in != null ? in : "";
          }
          found = find(was.getKeyType(), now.getKeyType(), in);
          return found != null ? found : find(was.getValueType(), now.getValueType(), in);
        default:
          return null;
      }
    }
  }

  // The steps the JSON reader records to the key of a map read from an array of entries.
  private static boolean isEntries(List<String> steps) {
    return steps != null && steps.size() == 2 && steps.get(0) == null
        && "key".equals(steps.get(1));
  }

  // The first such field of a message, at any depth, that the message written in its place
  // still has a field of the number of, and what would become of it. The fields of the two
  // are paired by number and their messages in turn, whatever those are named and whichever
  // file declares them: an entry another file declares is written as one of this file's.
  private static String writtenOtherwise(Descriptor was, Descriptor now, Set<String> seen,
      Set<String> unions) {
    if (was == null || now == null
        || !seen.add(was.getFullName() + " " + now.getFullName())) {
      return null;
    }
    for (FieldDescriptor field : was.getFields()) {
      // A oneof wrapped in a message is read as a union, which is written as a oneof of
      // the message itself: the field is no more, and its stored values with it.
      // Told by the attribute that continues the field, where a statement says which
      // that is: it is a union still. Else by what the message written has.
      if (wrapsUnion(field) && (unions != null ? unions.contains(field.getName())
          : writtenAsOneof(field, now))) {
        return field.getName() + " of message " + was.getName() + " is a oneof wrapped in a "
            + "message, which would be written as a oneof of the message itself, so its "
            + "stored values would be read as none";
      }
      FieldDescriptor kept = now.findFieldByNumber(field.getNumber());
      if (kept == null) {
        continue;
      }
      String of = field.getName() + " of message " + was.getName();
      if (field.isMapField()) {
        return of + " is a Protobuf map, which would be written as a repeated field of an "
            + "entry message, another kind of field";
      }
      if (field.getJavaType() != FieldDescriptor.JavaType.MESSAGE) {
        continue;
      }
      // A message that wraps a value is read as the value and written as it: the writer
      // wraps a collection, a union, and an element that may be null, and no other value.
      // A member of a oneof is told with its oneof, which is the attribute.
      OneofDescriptor in = field.getRealContainingOneof();
      String told = of + (in != null ? ", a member of its oneof " + in.getName() + "," : "");
      if (wrapsValue(field)) {
        return told
            + " is a message that wraps a value, which would be written as the value "
            + "itself and not as that message, so its stored values would be read wrongly";
      }
      // As is a wrapper whose value is not its field 1, or not its only field: the writer
      // writes the value as field 1 of a message of nothing else.
      if (wrapsOtherwise(field)) {
        return told + " is a message that wraps a value otherwise than as its field 1 and "
            + "its only field, which it would be written as, so its stored values would be "
            + "read wrongly";
      }
      // And one that wraps a oneof with more beside it, which is written as the oneof alone.
      if (wrapsMore(field)) {
        return told + " is a message that wraps a oneof and has more beside it, which it "
            + "would be written without, so what is stored there would be read no more";
      }
      // A message of the name of one of the format's own that is not that one is read as
      // that one, and written as it.
      String own = LogicalTypeToProtoConverter.readAsAnother(field.getMessageType());
      if (own != null) {
        return of + " is a " + field.getMessageType().getFullName() + " that is not the one "
            + own + " declares, which it would be written as, so its stored values would be "
            + "read wrongly";
      }
      // A message another file declares, which the field now refers to, is not written.
      Descriptor to = kept.getJavaType() == FieldDescriptor.JavaType.MESSAGE
          ? kept.getMessageType() : null;
      if (to != null && to.getFile() != now.getFile()) {
        continue;
      }
      String entry = ProtoToLogicalTypeConverter.entryFieldRenumbered(field);
      if (entry != null) {
        return entry + ", so its stored entries would be read wrongly";
      }
      if (ProtoToLogicalTypeConverter.isScalarWrapper(field.getMessageType())) {
        return of + " is a " + field.getMessageType().getFullName() + ", which would be "
            + "written as the value it wraps and not as that message, so its stored values "
            + "would be read wrongly";
      }
      String nested = writtenOtherwise(field.getMessageType(), to, seen, null);
      if (nested != null) {
        return nested;
      }
    }
    return null;
  }

  // Where no schema can be made at all, as where a member of a wrapped oneof has the number
  // of a field of the message: told as the wrapped oneof it is, while the type has its
  // union, and not as the numbers the writer gives.
  private void refuseWrappedOneof(ParsedSchema stored) {
    refuseWrappedOneof(stored, "Drop the attribute");
  }

  private void refuseWrappedOneof(ParsedSchema stored, String advice) {
    Descriptor was;
    try {
      was = stored instanceof ProtobufSchema ? ((ProtobufSchema) stored).toDescriptor() : null;
    } catch (RuntimeException e) {
      return;
    }
    if (was == null) {
      return;
    }
    Set<String> unions = unionsKept();
    for (FieldDescriptor field : was.getFields()) {
      if (wrapsUnion(field) && unions.contains(field.getName())) {
        throw new TypeRefusedException("Type " + subject + " cannot be altered: field "
            + field.getName() + " of message " + was.getName() + " is a oneof wrapped in "
            + "a message, which would be written as a oneof of the message itself, so its "
            + "stored values would be read as none. " + advice + ", or register its "
            + "new version as a Protobuf schema");
      }
    }
  }

  // As an action that writes the type before the statement is done is refused: what the
  // statement does to the attribute after it is not yet done.
  private void refuseWrappedOneof() {
    if (format == SchemaType.PROTOBUF && editing != null) {
      refuseWrappedOneof(lookup.parse(editing), "Drop the attribute, or give it another "
          + "kind of type, before an attribute is renamed: in this statement or one before "
          + "it");
    }
  }

  // The fields of the version edited that are unions as the statement leaves the type: by
  // the names their attributes are stored under, whatever the statement names them.
  private Set<String> unionsKept() {
    Set<String> unions = new HashSet<>();
    Schema target = target();
    if (target.getType() == Schema.Type.STRUCT) {
      for (Field f : target.getFields()) {
        if (f.getSchema().getType() == Schema.Type.UNION && !added.contains(f.getName())) {
          unions.add(previousName(f.getName()));
        }
      }
    }
    return unions;
  }

  // A field that holds a oneof wrapped in a message: not a member of a oneof, which is how
  // the writer itself writes a union that is a member of a union.
  private static boolean wrapsUnion(FieldDescriptor field) {
    return field.getRealContainingOneof() == null
        && ProtoToLogicalTypeConverter.wrapsUnion(field);
  }

  // A field of a message that wraps one value, as none the writer writes: a value that is
  // no collection in a field that is none, or an element that is never null.
  private static boolean wrapsValue(FieldDescriptor field) {
    if (!ProtoToLogicalTypeConverter.isFlinkWrapped(field)) {
      return false;
    }
    FieldDescriptor value = field.getMessageType().findFieldByName(
        CommonConstants.FLINK_WRAPPER_FIELD_NAME);
    if (value == null || value.isRepeated() || value.getRealContainingOneof() != null) {
      return false;
    }
    return !field.isRepeated() || !value.hasPresence();
  }

  private static boolean wrapsMore(FieldDescriptor field) {
    if (!ProtoToLogicalTypeConverter.isFlinkWrapped(field)) {
      return false;
    }
    Descriptor wrapper = field.getMessageType();
    return wrapper.findFieldByName(CommonConstants.FLINK_WRAPPER_FIELD_NAME) == null
        && wrapper.getRealOneofs().stream().anyMatch(oneof -> oneof.getName().equals(
            CommonConstants.FLINK_WRAPPER_FIELD_NAME)
            && oneof.getFieldCount() != wrapper.getFields().size());
  }

  private static boolean wrapsOtherwise(FieldDescriptor field) {
    if (!ProtoToLogicalTypeConverter.isFlinkWrapped(field)) {
      return false;
    }
    Descriptor wrapper = field.getMessageType();
    FieldDescriptor value = wrapper.findFieldByName(CommonConstants.FLINK_WRAPPER_FIELD_NAME);
    return value != null && value.getRealContainingOneof() == null
        && (value.getNumber() != 1 || wrapper.getFields().size() != 1);
  }

  // Whether the message written has, as a oneof of its own, the oneof a field's message
  // wraps: one of the field's name, or one a member of which is a member of that one, by
  // its name and number, as the attribute may be given another name. Not a oneof the
  // message stored has itself, whose members may be named and numbered as the wrapper's:
  // one of its name, or one every member of which is a member of it.
  private static boolean writtenAsOneof(FieldDescriptor field, Descriptor now) {
    List<FieldDescriptor> wrapped = field.getMessageType().getFields();
    List<OneofDescriptor> had = field.getContainingType().getRealOneofs();
    return now.getRealOneofs().stream().anyMatch(oneof -> oneof.getName().equals(
        field.getName()) || had.stream().noneMatch(stored -> stored.getName().equals(
            oneof.getName()) || within(oneof, stored))
        && oneof.getFields().stream().anyMatch(member -> wrapped.stream()
            .anyMatch(w -> w.getNumber() == member.getNumber()
                && (member.getName().equals(w.getName())
                  || member.getName().endsWith("_" + w.getName())))));
  }

  // Whether every member of a oneof is one of another's, by its name and number.
  private static boolean within(OneofDescriptor oneof, OneofDescriptor other) {
    return oneof.getFields().stream().allMatch(member -> other.getFields().stream()
        .anyMatch(o -> o.getNumber() == member.getNumber()
            && o.getName().equals(member.getName())));
  }

  // The first field whose integer encoding the logical type does not carry, or null.
  private static String reencoded(List<DescriptorProto> messages) {
    for (DescriptorProto m : messages) {
      for (FieldDescriptorProto f : m.getFieldList()) {
        String as;
        switch (f.getType()) {
          case TYPE_SINT32:
          case TYPE_SFIXED32:
            as = "int32";
            break;
          case TYPE_FIXED32:
          case TYPE_SINT64:
          case TYPE_FIXED64:
          case TYPE_SFIXED64:
            as = "int64";
            break;
          default:
            continue;
        }
        return "field " + f.getName() + " of message " + m.getName() + " is a "
            + f.getType().name().substring("TYPE_".length()).toLowerCase(Locale.ROOT)
            + ", which would be written as " + as + ", so its stored values would be read "
            + "wrongly";
      }
      String nested = reencoded(m.getNestedTypeList());
      if (nested != null) {
        return nested;
      }
    }
    return null;
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
  static void refuseOlderReferences(TypeLookup lookup, String subject,
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
   * version, {@code md}, and at a transitive level after the dropped versions before it: the
   * registry checks dropped versions only under LOGICAL. A refusal starts with {@code refused}
   * and names a version as {@code which} does.
   */
  static void refuseIncompatible(TypeLookup lookup, String subject, SchemaType format,
      SchemaMetadata md, LogicalType lt, String policy, String refused,
      IntFunction<String> which) {
    refuseIncompatible(lookup, subject, format, md, true, null, lt, policy, refused, which);
  }

  // With dropped false, md is live, and the registry checks it itself. The subject's versions,
  // soft-deleted too, are read if not given.
  private static void refuseIncompatible(TypeLookup lookup, String subject, SchemaType format,
      SchemaMetadata md, boolean dropped, List<Integer> versions, LogicalType lt, String policy,
      String refused, IntFunction<String> which) {
    // With no version dropped, the registry checks all there is to check.
    List<Integer> before = dropped ? null : droppedBefore(lookup, subject, versions, md);
    if (before != null && before.isEmpty()) {
      return;
    }
    Config config = lookup.config(subject);
    CompatibilityLevel level = CompatibilityLevel.forName(config.getCompatibilityLevel());
    CompatibilityPolicy applied = CompatibilityPolicy.forName(policy != null ? policy
        : config.getCompatibilityPolicy());
    boolean transitive = level == CompatibilityLevel.BACKWARD_TRANSITIVE
        || level == CompatibilityLevel.FORWARD_TRANSITIVE
        || level == CompatibilityLevel.FULL_TRANSITIVE;
    if (level == null || level == CompatibilityLevel.NONE
        || applied == CompatibilityPolicy.LOGICAL || !dropped && !transitive) {
      return;
    }
    // The most recent first, as the one the new version follows.
    List<SchemaMetadata> against = new ArrayList<>();
    if (dropped) {
      against.add(md);
    }
    if (transitive) {
      (before != null ? before : droppedBefore(lookup, subject, versions, md))
          .forEach(v -> against.add(lookup.version(subject, v)));
    }
    ParsedSchema schema = ColumnIds.nativeOf(subject, format, lt, refused);
    if (Boolean.TRUE.equals(config.isNormalize())) {
      schema = schema.normalize();
    }
    for (SchemaMetadata version : against) {
      List<String> errors = schema.isCompatible(level, applied,
          List.of(new SimpleParsedSchemaHolder(lookup.parse(version))));
      if (!errors.isEmpty()) {
        throw new TypeRefusedException(refused + ": it is incompatible with "
            + which.apply(version.getVersion()) + ", whose records it reads, at " + level
            + " compatibility: " + String.join("; ", errors));
      }
    }
  }

  // The soft-deleted versions before one, the most recent first.
  private static List<Integer> droppedBefore(TypeLookup lookup, String subject,
      List<Integer> versions, SchemaMetadata md) {
    List<Integer> live = lookup.versions(subject, false);
    return (versions != null ? versions : lookup.versions(subject, true)).stream()
        .filter(v -> v < md.getVersion() && !live.contains(v))
        .sorted(Comparator.reverseOrder()).collect(Collectors.toList());
  }

  private boolean apply(AlterAction action) {
    replacing = null;
    if (!(action instanceof AlterAction.SetWith)) {
      refuseSeveralMessages();
    }
    // Pinned again only after an action that may bring a type with a name of its own.
    final boolean pinned = avroNamesPinned;
    if (!leavesNamesPinned(action)) {
      avroNamesPinned = false;
    }
    if (action instanceof AlterAction.AddAttribute) {
      AlterAction.AddAttribute a = (AlterAction.AddAttribute) action;
      // The text is one attribute's: none follows it after a comma, and after a parenthesis
      // that closes it no clause of a struct, nor a statement of another type.
      Schema one = !TypeScripts.isOneField(a.name(), a.field()) ? null
          : parse(aliases -> TypeScripts.fieldScript(a.name(), a.field(), aliases))
            .getRootSchema();
      if (one == null || one.getFields().size() != 1 || !one.getParams().isEmpty()
          || one.getDoc() != null || !one.getTags().isEmpty() || !one.getRules().isEmpty()) {
        throw new TypeRefusedException("Attribute " + Formats.segment(a.name())
            + " is given more than one "
            + "type and its clauses, or clauses out of their order (the type, DEFAULT, "
            + "CHECK, COMMENT, TAGS, WITH): " + a.field());
      }
      Field field = one.getFields().get(0);
      // A type no format names brings no name: those pinned are all there are.
      avroNamesPinned = pinned && isUnnamedLeaf(field.getSchema());
      List<Field> fields = new ArrayList<>(struct().getFields());
      int same = named(fields, a.name());
      if (same >= 0) {
        throw new TypeRefusedException("Type " + subject + " already has attribute "
            + Formats.segment(fields.get(same).getName()));
      }
      refuseDroppedName(a.name());
      if (format == SchemaType.PROTOBUF) {
        refuseUnionNumbers(field);
      }
      Formats.refuseUnwritable(format, field, knownTypes(field.getSchema()));
      Map<String, Object> params = new LinkedHashMap<>(field.getParams());
      Counter numbers = format == SchemaType.PROTOBUF ? counter(fields) : null;
      if (format == SchemaType.PROTOBUF) {
        params.put(Schema.PROTOBUF_FIELD_NUMBER,
            String.valueOf(numberGiven(field, fields, numbers)));
      }
      // A nullable attribute without a default reads as null for existing data.
      boolean nullDefault = !field.hasDefaultValue() && field.getSchema().isNullable();
      Schema schema = format == SchemaType.PROTOBUF
          ? numberBranches(field.getSchema(), numbers) : field.getSchema();
      int at = a.position() == null ? fields.size() : place(fields, a.position());
      Field created = new Field(field.getName(), schema, fields.size(),
          nullDefault ? null : field.getDefaultValue(),
          nullDefault || field.hasDefaultValue(), field.getDoc(), field.getTags(), params,
          namedRules(field));
      if (a.position() != null) {
        List<Field> placed = new ArrayList<>(fields);
        placed.add(at, created);
        refuseUnionBeforePlain(placed, field.getName(), a.position());
      }
      fields.add(created);
      added.add(field.getName());
      setStruct(fields);
      if (at < fields.size() - 1) {
        // Avro names the types under the attributes in their order, and keeps the order:
        // each keeps the name it has with the new attribute last, which then takes its place.
        fields = format == SchemaType.AVRO ? pinAvroNames()
            : new ArrayList<>(struct().getFields());
        fields.add(at, fields.remove(find(fields, field.getName())));
        setStruct(fields);
      }
      return true;
    } else if (action instanceof AlterAction.DropAttribute) {
      AlterAction.DropAttribute a = (AlterAction.DropAttribute) action;
      List<Field> fields = new ArrayList<>(struct().getFields());
      int i = named(fields, a.name());
      if (i < 0) {
        if (a.ifExists()) {
          return false;
        }
        throw new TypeRefusedException("Type " + subject + " has no attribute "
            + Formats.segment(a.name()));
      }
      for (Rule r : struct().getRules()) {
        if (Constraints.columns(r.getSql()).contains(fields.get(i).getName())) {
          throw new TypeRefusedException("Attribute " + Formats.segment(a.name())
              + " is used by constraint "
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
    } else if (action instanceof AlterAction.MoveAttribute) {
      AlterAction.MoveAttribute a = (AlterAction.MoveAttribute) action;
      List<Field> fields = new ArrayList<>(struct().getFields());
      int i = named(fields, a.name());
      if (i < 0) {
        throw new TypeRefusedException("Type " + subject + " has no attribute "
            + Formats.segment(a.name()));
      }
      Field moved = fields.remove(i);
      String after = a.position().after();
      if (after != null && named(List.of(moved), after) >= 0) {
        throw new TypeRefusedException("Attribute " + Formats.segment(moved.getName())
            + " cannot go after itself");
      }
      int at = place(fields, a.position());
      // Where it already is, the type is as it was.
      if (at == i) {
        return false;
      }
      // As for an attribute added at a position: the names are those of the order there is.
      if (format == SchemaType.AVRO) {
        fields = pinAvroNames();
        moved = fields.remove(i);
      }
      fields.add(at, moved);
      refuseUnionBeforePlain(fields, moved.getName(), a.position());
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
        throw new TypeRefusedException("Type " + subject + " has no attribute "
            + Formats.segment(a.from()));
      }
      // Renamed to the name it has, an attribute is as it was.
      if (fields.get(i).getName().equals(a.to())) {
        return false;
      }
      // A column may take its own name in another case, as no other's.
      int taken = named(fields, a.to());
      if (taken >= 0 && taken != i) {
        throw new TypeRefusedException("Type " + subject + " already has attribute "
            + Formats.segment(fields.get(taken).getName()));
      }
      refuseDroppedName(a.to());
      Field f = fields.get(i);
      // A macro variable of the new name would take the attribute's place in its body.
      List<Rule> using = new ArrayList<>(f.getRules());
      using.addAll(struct().getRules());
      for (Rule r : using) {
        if (Constraints.columns(r.getSql()).contains(f.getName())
            && Constraints.variables(r.getSql()).contains(a.to())) {
          throw new TypeRefusedException("Attribute " + Formats.segment(a.from())
              + " cannot be renamed to " + Formats.segment(a.to()) + ": constraint "
              + (r.getName() != null ? r.getName()
              : "CHECK (" + r.getSql() + ")") + " uses " + a.to()
              + " as an iteration variable");
        }
      }
      Map<String, Object> params = new LinkedHashMap<>(f.getParams());
      if (format == SchemaType.AVRO) {
        // The name it is stored under, which its data was written with: not one this
        // statement gave it, nor that of an attribute the statement adds.
        List<String> aliases = new ArrayList<>(f.getAliases());
        if (previousName(f.getName()).equals(f.getName()) && !added.contains(f.getName())
            && !aliases.contains(f.getName())) {
          aliases.add(f.getName());
          params.put(Schema.AVRO_ALIASES, String.join(",", aliases));
        }
      }
      // The native names of the attribute's types follow its name; pin them to the old one.
      final Schema type;
      try {
        params.putAll(Formats.typeNameParams(format, edited(), subject, f.getName()));
        type = withBranchParams(f.getSchema(),
            Formats.branchTypeNameParams(format, edited(), subject, f.getName()));
      } catch (TypeRefusedException e) {
        refuseWrappedOneof();
        throw e;
      } catch (RuntimeException e) {
        // Told as the wrapped oneof it is, where that is why no schema can be made.
        refuseWrappedOneof();
        throw new TypeRefusedException(refused() + ": no " + format
            + " schema can be made of it: " + e.getMessage(), e);
      }
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
        throw new TypeRefusedException("Type " + subject + " has no attribute "
            + Formats.segment(a.name()));
      }
      Field f = fields.get(i);
      replacing = f.getSchema();
      Schema type = parse(aliases -> TypeScripts.typeScript(a.type(), aliases))
          .getRootSchema();
      Formats.refuseUnwritable(format, type, knownTypes(type),
          Formats.segment(f.getName()));
      Map<String, Object> params = f.getParams();
      // A type of the same kind keeps the native names pinned on it; another kind gets new.
      if (keepsNames(f.getSchema(), type)) {
        type = withNestedNames(type, f.getSchema());
      } else {
        params = new LinkedHashMap<>(params);
        params.keySet().removeIf(Schema::isTypeNameParam);
      }
      if (format == SchemaType.AVRO) {
        type = withDefaults(f.getSchema(), type, Formats.segment(f.getName()));
      }
      // The type it has already: nothing changes, and no version is made of it. One that
      // names a referenced type is said again to refer to that type's latest version.
      Set<String> named = new HashSet<>();
      LogicalType.collectNamedRefs(type, named);
      if (named.isEmpty() && type.equals(f.getSchema()) && params.equals(f.getParams())) {
        return false;
      }
      if (format == SchemaType.PROTOBUF) {
        Messages root = new Messages(current, versions);
        type = type.getType() == Schema.Type.UNION
            ? renumbered(type, f.getSchema(), root, counter(fields),
              Formats.segment(f.getName()))
            : renumbered(type, f.getSchema(), numberIn(f.getParams()) > 0
              ? root.field(numberIn(f.getParams())) : root.field(f.getName()), null,
              Formats.segment(f.getName()));
        // A union has no number of its own, only its branches; what replaces it gets a
        // new one, rather than the lowest free one, a branch's.
        if (type.getType() != Schema.Type.UNION
            && f.getSchema().getType() == Schema.Type.UNION) {
          params = new LinkedHashMap<>(params);
          params.put(Schema.PROTOBUF_FIELD_NUMBER,
              String.valueOf(counter(fields).take()));
        }
      }
      // A null default does not apply once the attribute is NOT NULL.
      if (!type.isNullable() && f.hasDefaultValue() && f.getDefaultValue() == null) {
        fields.set(i, new Field(f.getName(), type, f.getPosition(), null, false, false,
            f.getDoc(), f.getTags(), params, f.getRules()));
      } else if (format == SchemaType.AVRO && f.getDefaultValue() != null) {
        fields.set(i, new Field(f.getName(), type, f.getPosition(),
            AvroDefaults.converted(Formats.segment(f.getName()), f.getSchema(), type,
              f.getDefaultValue()),
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
      replacing = target;
      Schema type = parse(aliases -> TypeScripts.typeScript(a.type(), aliases))
          .getRootSchema();
      Formats.refuseUnwritable(format, type, knownTypes(type), null);
      // An enum keeps its nullability, comment and properties, as with ADD VALUE.
      if (target.getType() == Schema.Type.ENUM && type.getType() == Schema.Type.ENUM) {
        type = copyMetadata(target, Schema.createEnum(type.getEnumValues()))
            .setParams(target.getParams());
      }
      if (format == SchemaType.AVRO) {
        type = withDefaults(target, type, null);
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
      refuseNativeParams(a.with(), params, "attribute", Formats.segment(f.getName()));
      Formats.refuseReservedParams(format, "attribute " + Formats.segment(f.getName()),
          a.with().keySet());
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
      refuseNativeParams(with, params, "type", subject);
      Formats.refuseReservedParams(format, "type " + subject, with.keySet());
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
        String one = Formats.segment(name);
        String two = Formats.segment(other);
        String third = Formats.segment(temp);
        throw new TypeRefusedException("Attribute " + one + " " + why + ": the aliases of "
            + one + " and " + two + " already swap their names, so they cannot be "
            + "swapped back in one statement; swap them in three: RENAME ATTRIBUTE "
            + one + " TO " + third + ", then RENAME ATTRIBUTE " + two + " TO " + one
            + ", then RENAME ATTRIBUTE " + third + " TO " + two);
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
      throw new TypeRefusedException("Attribute " + Formats.segment(name)
          + " is dropped by this statement, so its "
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
    externalImports.forEach((name, to) -> {
      // A document named with a # and no more after its name is the document.
      String uri = to.endsWith("#") ? to.substring(0, to.length() - 1) : to;
      // Not to a name the logical types DDL cannot write, which stays imported.
      if (TypeReferences.nameable(uri)
          && references.stream().anyMatch(r -> r.getName().equals(uri))) {
        renames.put(name, uri);
      }
    });
    if (renames.isEmpty()) {
      return;
    }
    // A reference named as the type as read calls another it refers to, which stays so
    // called: the two would be one, and written as one.
    for (String name : renames.values()) {
      String called = externalImports.get(name);
      if (called == null || renames.containsKey(name)) {
        continue;
      }
      throw new TypeRefusedException("Type " + subject + " cannot be altered: it refers to "
          + "a type named " + name + ", which is what " + toldAs(called)
          + ". The two are read as one, and the one would be written as the other. Register "
          + "its new version as a JSON schema");
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
    // A JSON type's own definitions are written whether an attribute is of them or not,
    // with the types they name.
    if (format == SchemaType.JSON) {
      namedTypes.forEach((name, type) -> {
        if (!externalTypes.contains(name)) {
          work.add(type);
        }
      });
    }
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
    // A JSON type read back names a referenced type Ref<n>, imported by the reference's name.
    Set<String> used = new LinkedHashSet<>(reachable);
    for (String name : reachable) {
      String uri = externalImports.get(name);
      if (uri != null) {
        used.add(uri.contains("#") ? uri.substring(0, uri.indexOf('#')) : uri);
      }
    }
    // A JSON document is of use as well while an attribute is of one of its definitions,
    // which the type has by the definition's own name: unless that name is a reference's,
    // whose document it then is.
    Set<String> documents = new HashSet<>();
    references.forEach(r -> documents.add(r.getName()));
    Set<String> definitions = new HashSet<>(reachable);
    definitions.retainAll(externalTypes);
    definitions.removeAll(documents);
    // Nor is it what the type as read calls a type or a part of one, which is no name of
    // a definition.
    definitions.removeAll(externalImports.keySet());
    // A JSON reference may be of no use though no name is: one only a definition was of
    // that no attribute was of, which the type as read has not and is written no more.
    // One the statement leaves no attribute of is dropped with it. One no attribute was
    // of is dropped too, unless taken away it would leave a name another type or version.
    Set<String> freed = new HashSet<>(unused);
    unused.forEach(name -> {
      // The document of what the name was for: of a definition, the one its $ref was to.
      String uri = externalImports.get(name);
      Set<String> places = uri != null ? Set.of(uri) : placesStored.getOrDefault(name, Set.of());
      places.stream().filter(place -> place.indexOf('#') != 0)
          .forEach(place -> freed.add(place.contains("#")
              ? place.substring(0, place.indexOf('#')) : place));
    });
    Predicate<SchemaReference> unnamed = ref -> !used.contains(ref.getName())
        && definitionsOf(ref).stream().noneMatch(definitions::contains);
    List<SchemaReference> idle = format != SchemaType.JSON ? List.of() : references.stream()
        .filter(ref -> unnamed.test(ref) && !freed.contains(ref.getName()))
        .collect(Collectors.toList());
    Set<SchemaReference> pinned = binding(idle, used, definitions, unnamed);
    if (unused.isEmpty() && !renamed && pinned.size() == idle.size()) {
      return;
    }
    externalTypes.removeAll(unused);
    namedTypes.keySet().removeAll(unused);
    externalImports.keySet().removeAll(unused);
    List<SchemaReference> before = new ArrayList<>(references);
    // A reference whose defined name cannot be told is kept.
    references.removeIf(ref -> {
      Set<String> names = TypeReferences.definedNames(lookup, subject, ref);
      return names != null && names.stream().noneMatch(used::contains)
          && definitionsOf(ref).stream().noneMatch(definitions::contains)
          && !pinned.contains(ref);
    });
    if (format == SchemaType.JSON) {
      refuseMovedWithDropped(before, used);
    }
    resolved.clear();
    resolved.putAll(TypeReferences.resolve(lookup, subject, references));
  }

  // A type an attribute is of may be read through a reference that goes with the statement,
  // and without it be another version, or none: the attribute, which the statement does
  // not name, would be of another type. One the statement names is its own to move.
  private void refuseMovedWithDropped(List<SchemaReference> before, Set<String> used) {
    if (before.size() == references.size()) {
      return;
    }
    Map<String, SchemaMetadata> versions = new HashMap<>();
    Map<String, String> was = TypeReferences.firstMet(lookup, subject, before, versions);
    Map<String, String> now = TypeReferences.firstMet(lookup, subject, references, versions);
    Set<String> own = new HashSet<>();
    references.stream().filter(ref -> !original.getReferences().contains(ref))
        .forEach(ref -> own.add(ref.getName()));
    for (String name : used) {
      String old = was.get(name);
      if (old != null && !own.contains(name) && !old.equals(now.get(name))) {
        throw new TypeRefusedException(refused() + ": it would no longer refer to version "
            + old.substring(old.lastIndexOf('@') + 1) + " of type " + name + ", which it has "
            + "through a type it would refer to no more; name the type again to move it to "
            + "its latest version");
      }
    }
  }

  // Of the references no attribute was of, those that taken away would leave a name
  // another type or version: a name an attribute is of, the document of a definition one
  // is of, or a name below a reference the version edited has and the statement keeps.
  // What the statement adds or moves is its own. The first met binds a name, so each is
  // tried without those that go, and again once another has gone.
  private Set<SchemaReference> binding(List<SchemaReference> idle, Set<String> used,
      Set<String> definitions, Predicate<SchemaReference> unnamed) {
    if (idle.isEmpty()) {
      return new HashSet<>();
    }
    Map<String, SchemaMetadata> versions = new HashMap<>();
    List<SchemaReference> kept = references.stream()
        .filter(ref -> idle.contains(ref) || !unnamed.test(ref)).collect(Collectors.toList());
    Set<String> stored = new HashSet<>();
    original.getReferences().forEach(ref -> stored.add(ref.getName()));
    Set<String> names = new HashSet<>(used);
    Set<String> own = new HashSet<>();
    for (SchemaReference ref : kept) {
      if (idle.contains(ref)) {
        continue;
      }
      if (stored.contains(ref.getName())) {
        names.addAll(TypeReferences.below(lookup, subject, ref, versions));
      }
      if (definitionsOf(ref).stream().anyMatch(definitions::contains)) {
        names.add(ref.getName());
      }
      if (!original.getReferences().contains(ref)) {
        own.add(ref.getName());
      }
    }
    names.removeAll(own);
    Map<String, String> bound = TypeReferences.firstMet(lookup, subject, kept, versions);
    boolean gone = true;
    while (gone) {
      gone = false;
      for (SchemaReference ref : idle) {
        List<SchemaReference> without = new ArrayList<>(kept);
        if (!without.remove(ref)) {
          continue;
        }
        Map<String, String> now = TypeReferences.firstMet(lookup, subject, without, versions);
        if (names.stream().allMatch(name -> Objects.equals(bound.get(name), now.get(name)))) {
          kept = without;
          gone = true;
        }
      }
    }
    return idle.stream().filter(kept::contains).collect(Collectors.toSet());
  }

  // The names of the definitions a JSON document a reference is to has at its root: one
  // further in it is no name a type has for it.
  private Set<String> definitionsOf(SchemaReference ref) {
    Set<String> names = new HashSet<>();
    String text = format == SchemaType.JSON ? resolved.get(ref.getName()) : null;
    if (text == null) {
      return names;
    }
    try {
      JsonNode document = JacksonMapper.INSTANCE.readTree(text);
      for (String word : List.of("$defs", "definitions")) {
        document.path(word).fieldNames().forEachRemaining(names::add);
      }
    } catch (IOException e) {
      // One that is no JSON has none.
      names.clear();
    }
    return names;
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
  // The types a name in a type text just parsed may be of: the type's own and the text's.
  private KnownTypes knownTypes(Schema judged) {
    KnownTypes known = KnownTypes.of(namedTypes);
    parsedTypes.forEach(known::put);
    // Only a JSON type is judged by what the types it refers to are, and only one with a
    // union and a name in it: they are read for no other.
    if (format == SchemaType.JSON && Formats.namesInAUnion(judged)) {
      Map<String, String> imports = new HashMap<>(externalImports);
      imports.putAll(parsedImports);
      // As the references have them, whatever the type holds under their names.
      known.adopt(referred(references, imports), externalTypes::contains);
    }
    return known;
  }

  // The types references are of, read once for a statement.
  private KnownTypes referred(List<SchemaReference> to, Map<String, String> imports) {
    return referredTypes.computeIfAbsent(to + " " + new TreeMap<>(imports),
        k -> TypeReferences.referredTypes(lookup, subject, to, imports, referredRead));
  }

  // A reference moved to another version of its type gives the union branches of that type
  // its attributes there: a union whose branches a value could not be both of, and could
  // then, is refused; the rows stored under one branch would be read as the other's.
  private void refuseNewOverlaps(ParsedSchema stored, ParsedSchema written) {
    Map<String, Integer> versions = new HashMap<>();
    original.getReferences().forEach(r -> versions.put(
        TypeReferences.referenced(subject, r.getSubject()), r.getVersion()));
    List<Schema> types = new ArrayList<>(List.of(root));
    namedTypes.forEach((name, type) -> {
      if (!externalTypes.contains(name)) {
        types.add(type);
      }
    });
    // Told as the references that move: "type t from version 1 to version 2".
    String moved = references.stream().filter(r -> !r.getVersion().equals(versions.getOrDefault(
        TypeReferences.referenced(subject, r.getSubject()), r.getVersion())))
        .map(r -> "type " + r.getSubject() + " from version " + versions.get(
            TypeReferences.referenced(subject, r.getSubject())) + " to version "
            + r.getVersion()).collect(Collectors.joining(" and "));
    if (moved.isEmpty() || types.stream().noneMatch(Formats::namesInAUnion)) {
      return;
    }
    KnownTypes before = KnownTypes.of(original.getNamedTypes());
    before.adopt(referred(original.getReferences(), original.getExternalImports()),
        original.getExternalTypes()::contains);
    withDefinitions(before, definitionsRead(stored));
    Map<String, Schema> after = definitionsRead(written);
    for (Schema type : types) {
      // What a type with no union, or no name, is told by is the same before and after.
      if (!Formats.namesInAUnion(type)) {
        continue;
      }
      String[] found = Formats.newlyOverlapping(type, before,
          withDefinitions(knownTypes(type), after), null);
      if (found != null) {
        throw Formats.newlyOverlapping(refused(), "its reference would move, " + moved,
            found);
      }
    }
  }

  // A definition of a document referred to is no type of that document as it is read by
  // itself, nor of this type as the registry gives it: it is what the schema read with its
  // references has under the definition's name. Known so where it is not known otherwise.
  private static KnownTypes withDefinitions(KnownTypes known, Map<String, Schema> read) {
    read.forEach((name, type) -> {
      if (known.get(name) == null) {
        known.put(name, type);
      }
    });
    return known;
  }

  // The named types of a schema as it is read with its references; none of none.
  private static Map<String, Schema> definitionsRead(ParsedSchema schema) {
    try {
      return schema == null ? Map.of() : LogicalTypeConversion.toLogicalType(schema)
          .getNamedTypes();
    } catch (RuntimeException e) {
      // A schema that cannot be read is refused as that.
      return Map.of();
    }
  }

  // The names the type's attributes are of, and those of the named types they are of in
  // turn: but for the type an action is giving anew, which the statement frees them of.
  private Set<String> namesInUse() {
    Set<String> used = new LinkedHashSet<>();
    Schema target = target();
    if (target != replacing && target.getType() == Schema.Type.STRUCT) {
      for (Field f : target.getFields()) {
        if (f.getSchema() != replacing) {
          LogicalType.collectNamedRefs(f.getSchema(), used);
        }
      }
    } else if (target != replacing) {
      LogicalType.collectNamedRefs(target, used);
    }
    // As are those of a JSON type's own definitions, which are written whether an
    // attribute is of them or not.
    if (format == SchemaType.JSON) {
      namedTypes.forEach((name, type) -> {
        if (type != target && !externalTypes.contains(name)) {
          LogicalType.collectNamedRefs(type, used);
        }
      });
    }
    List<String> todo = new ArrayList<>(used);
    while (!todo.isEmpty()) {
      Schema named = namedTypes.get(todo.remove(todo.size() - 1));
      Set<String> more = new LinkedHashSet<>();
      if (named != null) {
        LogicalType.collectNamedRefs(named, more);
      }
      more.stream().filter(used::add).forEach(todo::add);
    }
    return used;
  }

  // A JSON name is one type's, and a definition is read by its name alone, whichever
  // document it is of. A schema written by hand may have $refs of its own to two places
  // that are read by one name: the two are read as one, and a statement would write the
  // one as the other. Told by where the loader has each $ref go. What a document referred
  // to refers to is not this type's to write.
  private void refuseReadAsOne(SchemaMetadata md) {
    ParsedSchema stored = lookup.parse(md);
    if (stored instanceof JsonSchema) {
      refuseIdOfADocument((JsonSchema) stored);
      placesStored = refuseReadAsOne((JsonSchema) stored, "altered");
      refuseNameThatSaysMore(((JsonSchema) stored).toJsonNode());
    }
  }

  private Map<String, Set<String>> refuseReadAsOne(JsonSchema schema, String done) {
    Map<String, Set<String>> places;
    try {
      places = places(schema);
    } catch (BesideARef e) {
      throw new TypeRefusedException("Type " + subject + " cannot be " + done + ": it refers "
          + "to " + e.place + ", which is beside the $ref of " + e.ref + " and would be "
          + "written no more. Register its new version as a JSON schema");
    }
    if (places == null) {
      return Map.of();
    }
    for (Map.Entry<String, Set<String>> named : places.entrySet()) {
      if (named.getValue().size() > 1) {
        Iterator<String> two = named.getValue().iterator();
        throw new TypeRefusedException("Type " + subject + " cannot be " + done + ": it refers "
            + "to " + two.next() + " and to " + two.next() + ", which are both read by the "
            + "name " + named.getKey() + ". The two are read as one, and the one would be "
            + "written as the other. Register its new version as a JSON schema");
      }
    }
    return places;
  }

  // Where the $refs of a schema's own go, and where each name was first read from, by the
  // names the type as read has: each place as the document it is in has it, under $defs
  // or definitions. Null where the schema is not read.
  private static Map<String, Set<String>> places(JsonSchema schema) {
    Map<String, Set<String>> read;
    try {
      read = JsonToLogicalTypeConverter.placesReferredTo(schema);
    } catch (RuntimeException e) {
      return null;
    }
    Map<String, JsonNode> documents = new HashMap<>();
    Map<String, Set<String>> places = new LinkedHashMap<>();
    for (Map.Entry<String, Set<String>> named : read.entrySet()) {
      for (String place : named.getValue()) {
        int hash = place.indexOf('#');
        String in = hash < 0 ? place : place.substring(0, hash);
        String part = hash < 0 ? "" : place.substring(hash + 1);
        JsonNode document = documents.computeIfAbsent(in, name -> document(schema, name));
        part = defined(document, part);
        if (in.isEmpty()) {
          refuseBesideARef(document, part);
        }
        places.computeIfAbsent(named.getKey(), name -> new LinkedHashSet<>())
            .add(part.isEmpty() ? (in.isEmpty() ? "#" : in) : in + "#" + part);
      }
    }
    return places;
  }

  // A place of the schema's own under a schema that is a $ref: what is beside a $ref is
  // not written again, and a $ref to it would be to nothing.
  private static void refuseBesideARef(JsonNode document, String part) {
    for (int at = part.indexOf('/', 1); at > 0; at = part.indexOf('/', at + 1)) {
      if (definition(document, "#" + part.substring(0, at)).path("$ref").isTextual()) {
        throw new BesideARef("#" + part, "#" + part.substring(0, at));
      }
    }
  }

  private static final class BesideARef extends RuntimeException {
    final String place;
    final String ref;

    BesideARef(String place, String ref) {
      super(null, null, false, false);
      this.place = place;
      this.ref = ref;
    }
  }

  // The schema itself, or a document it refers to by the name it has for it.
  private static JsonNode document(JsonSchema schema, String name) {
    String text = name.isEmpty() ? null : schema.resolvedReferences().get(name);
    try {
      return name.isEmpty() ? schema.toJsonNode() : text == null ? MissingNode.getInstance()
          : JacksonMapper.INSTANCE.readTree(text);
    } catch (IOException e) {
      return MissingNode.getInstance();
    }
  }

  // A schema whose own name is the name it has for a document it refers to: the loader has
  // a $ref by that name go to either, by the order it comes to them.
  private void refuseIdOfADocument(JsonSchema schema) {
    JsonNode root = schema.toJsonNode();
    // By the word the schema's draft has for its name: id in draft-04, $id since.
    String word = root.path("$schema").asText("").contains("draft-04") ? "id" : "$id";
    String id = root.path(word).isTextual() ? root.path(word).asText() : "";
    if (schema.resolvedReferences().containsKey(
        id.endsWith("#") ? id.substring(0, id.length() - 1) : id)) {
      throw new TypeRefusedException("Type " + subject + " cannot be altered: its " + word
          + ", " + id + ", is the name it has for a schema it refers to as well, and what a "
          + "$ref by that name is to cannot be told. Register its new version as a JSON "
          + "schema");
    }
  }

  // A name the type has for what it refers to is written as that name alone. In a later
  // draft what a schema says beside its $ref is read: a statement would write it no more.
  private void refuseNameThatSaysMore(JsonNode root) {
    if (!later(root)) {
      return;
    }
    for (String word : List.of("$defs", "definitions")) {
      for (Iterator<Map.Entry<String, JsonNode>> all = root.path(word).fields();
          all.hasNext(); ) {
        Map.Entry<String, JsonNode> definition = all.next();
        if (!entry(definition.getValue())) {
          continue;
        }
        for (Iterator<String> said = definition.getValue().fieldNames(); said.hasNext(); ) {
          String more = said.next();
          if (!SAYS_NOTHING.contains(more) && !more.startsWith("x-")) {
            throw new TypeRefusedException("Type " + subject + " cannot be altered: its "
                + "definition " + definition.getKey() + " is a name for "
                + definition.getValue().path("$ref").asText() + " that says more of a value "
                + "beside it (" + more + "), which would be written no more. Register its "
                + "new version as a JSON schema");
          }
        }
      }
    }
  }

  private static boolean entry(JsonNode definition) {
    return definition.path("logical.ref").asBoolean() && definition.path("$ref").isTextual();
  }

  // A schema as it is read by itself, or null: one that cannot be read is refused as that.
  private static LogicalType readAlone(ParsedSchema schema) {
    try {
      return LogicalTypeConversion.toLogicalType(schema);
    } catch (RuntimeException e) {
      return null;
    }
  }

  private static boolean later(JsonNode root) {
    String draft = root.path("$schema").asText("");
    return draft.contains("2019-09") || draft.contains("2020-12");
  }

  // A place under $defs as it is in a document that has it under definitions, and the
  // other way: the loader reads the one word for the other.
  private static String defined(JsonNode document, String part) {
    for (String[] words : new String[][] {{"/$defs/", "/definitions/"},
        {"/definitions/", "/$defs/"}}) {
      if (document != null && part.startsWith(words[0])
          && definition(document, "#" + part).isMissingNode()
          && !definition(document, "#" + words[1] + part.substring(words[0].length()))
              .isMissingNode()) {
        return words[1] + part.substring(words[0].length());
      }
    }
    return part;
  }

  // What the type as read gives a name of its making to, as a refusal tells it: a document
  // whole, with or without a # after its name, or a part of one.
  private static String toldAs(String to) {
    int hash = to.indexOf('#');
    if (to.equals("#")) {
      return "the schema is called where it refers to itself whole: #";
    }
    return (hash < 0 || hash == to.length() - 1 ? "a type is called that it refers to whole: "
        : "a part of a schema is called that it refers to: ") + to;
  }

  // What a $ref to a part of the schema itself is to, or nothing where it is to none.
  private static JsonNode definition(JsonNode schema, String ref) {
    try {
      return schema.at(JsonPointer.compile(ref.substring(ref.indexOf('#') + 1)));
    } catch (IllegalArgumentException e) {
      return MissingNode.getInstance();
    }
  }

  private LogicalType parse(TypeReferences.ScriptBuilder builder) {
    // A JSON type's reference to a type it already refers to keeps its name, which its $refs use.
    Map<String, String> names = new HashMap<>();
    if (format == SchemaType.JSON) {
      for (SchemaReference r : references) {
        names.put(TypeReferences.referenced(subject, r.getSubject()), r.getName());
      }
    } else if (!references.isEmpty()) {
      // A type the statement has dropped is no longer referred to: its names for the types
      // it refers to are not this type's.
      dropUnusedReferences();
    }
    LogicalType lt = parseWithReferences(builder, names);
    parsedTypes = lt.getNamedTypes();
    parsedImports = lt.getExternalImports();
    // The names the type has a use for other than as its references': a definition of its
    // own or of a type it refers to. And what the type as read calls a type it refers to,
    // in use or not: the name is that type's until the statement is done.
    Set<String> defined = namesInUse();
    defined.addAll(externalImports.keySet());
    references.forEach(r -> defined.remove(r.getName()));
    Set<String> named = new HashSet<>(externalTypes);
    references.forEach(r -> named.remove(r.getName()));
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
      // A JSON name is one type's: a definition of a type referred to that the type has
      // under a name, and a type of that name, would both be written as the one.
      // And one that is what the type as read calls another type is that type's there,
      // though a reference of the type has the name as well.
      boolean called = externalImports.containsKey(ref.getName())
          && !ref.getName().equals(externalImports.get(ref.getName()));
      if (format == SchemaType.JSON && (called || !found && defined.contains(ref.getName()))) {
        throw new TypeRefusedException(refused() + ": it has a type named " + ref.getName()
            + " already, " + (externalImports.containsKey(ref.getName())
              ? "as it calls a type it refers to" : named.contains(ref.getName())
              ? "a definition of a type it refers to" : "a definition of its own")
            + ", and would have type " + ref.getSubject() + " by that name as well");
      }
      // A name the type had for a definition, and has no more a use for as that, is the
      // new reference's: the reference the definition was of may be of no use.
      renamed |= !found && format == SchemaType.JSON && named.contains(ref.getName());
      if (!found) {
        // One the statement dropped and names again is the reference it was, by the name it
        // had: a file another tool wrote imports it by that name.
        SchemaReference had = format == SchemaType.JSON ? null
            : original.getReferences().stream()
                .filter(r -> TypeReferences.referenced(subject, r.getSubject())
                    .equals(TypeReferences.referenced(subject, ref.getSubject())))
                .findFirst().orElse(null);
        references.add(had == null ? ref
            : new SchemaReference(had.getName(), had.getSubject(), ref.getVersion()));
      }
    }
    // Read anew from the references: a type the statement names otherwise than its reference
    // is would else be held twice, under both names.
    if (moved || !lt.getReferences().isEmpty()) {
      resolved.clear();
      resolved.putAll(TypeReferences.resolve(lookup, subject, references));
    }
    if (format != SchemaType.JSON && !lt.getReferences().isEmpty()) {
      nameReferencesAsBelow();
    }
    return lt;
  }

  // An Avro or Protobuf reference takes the name the types this one refers to give its
  // subject, whichever was named first: by two names, a Protobuf file is imported twice.
  private void nameReferencesAsBelow() {
    Map<String, String> below = TypeReferences.namesBelow(lookup, subject, references);
    boolean renamed = false;
    for (int i = 0; i < references.size(); i++) {
      SchemaReference r = references.get(i);
      String name = below.get(TypeReferences.referenced(subject, r.getSubject()));
      if (name != null && !name.equals(r.getName())
          && !TypeReferences.isNameOf(subject, name)) {
        references.set(i, new SchemaReference(name, r.getSubject(), r.getVersion()));
        renamed = true;
      }
    }
    if (renamed) {
      List<SchemaReference> once = new ArrayList<>(new LinkedHashSet<>(references));
      references.clear();
      references.addAll(once);
      resolved.clear();
      resolved.putAll(TypeReferences.resolve(lookup, subject, references));
    }
  }

  private Field field(String name) {
    List<Field> fields = struct().getFields();
    int i = named(fields, name);
    if (i < 0) {
      throw new TypeRefusedException("Type " + subject + " has no attribute "
          + Formats.segment(name));
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

  // The attributes in their order; none for a type that is no struct.
  private List<String> attributeNames() {
    Schema target = target();
    return target.getType() != Schema.Type.STRUCT ? List.of()
        : target.getFields().stream().map(Field::getName).collect(Collectors.toList());
  }

  private Schema struct() {
    Schema target = target();
    if (target.getType() != Schema.Type.STRUCT) {
      throw new TypeRefusedException("Type " + subject + (target.getType() == Schema.Type.ENUM
          ? " is an enum, which has values and no attributes; use ADD VALUE"
          : " is not a struct; use SET DATA TYPE"));
    }
    return target;
  }

  private void setStruct(List<Field> given) {
    List<Field> fields = format == SchemaType.PROTOBUF ? plainThenUnions(given) : given;
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

  // A Protobuf message's unions are declared after its other fields, and read so: the
  // attributes are kept in that order, each kind in the order it has.
  private static List<Field> plainThenUnions(List<Field> fields) {
    List<Field> ordered = new ArrayList<>(fields.size());
    for (Field f : fields) {
      if (f.getSchema().getType() != Schema.Type.UNION) {
        ordered.add(f);
      }
    }
    for (Field f : fields) {
      if (f.getSchema().getType() == Schema.Type.UNION) {
        ordered.add(f);
      }
    }
    return ordered;
  }

  // A position that would put a union before an attribute that is not one cannot be kept.
  private void refuseUnionBeforePlain(List<Field> fields, String name,
      AlterAction.Position position) {
    if (format != SchemaType.PROTOBUF || plainThenUnions(fields).equals(fields)) {
      return;
    }
    throw new TypeRefusedException("Attribute " + Formats.segment(name) + " cannot be placed "
        + (position.after() == null ? "first" : "after " + Formats.segment(position.after()))
        + " in PROTOBUF "
        + "type " + subject + ": a Protobuf message declares its unions after its other "
        + "fields, so a union follows every attribute that is not a union");
  }

  /**
   * Pins the name of every message and enum the version declares for one field, at every level,
   * that the writer would name otherwise, as it would one Flink named ({@code addr_Row} for
   * {@code AddrRow}): the registry takes a type of another name for another type, and refuses
   * the version that loses the first. And the field name of every union member: which name the
   * writer gives one turns on the fields before it, in an order the version is not read in.
   */
  private void pinProtobufNames(SchemaMetadata md) {
    ParsedSchema schema = lookup.parse(md);
    if (!(schema instanceof ProtobufSchema)) {
      return;
    }
    pinNames(ProtoToLogicalTypeConverter.toLogicalType((ProtobufSchema) schema, true));
  }

  // Pins on the type being edited the names another reading of it records, where it has none.
  private void pinNames(LogicalType named) {
    root = pinned(root, named.getRootSchema());
    for (Map.Entry<String, Schema> type : new ArrayList<>(namedTypes.entrySet())) {
      Schema other = named.getNamedTypes().get(type.getKey());
      if (other != null) {
        namedTypes.put(type.getKey(), pinned(type.getValue(), other));
      }
    }
  }

  /**
   * Pins the names the Avro types under the attributes have as they stand, wherever the writer
   * would not derive them again: read back from the schema it writes of the type now, as the
   * next statement would read it. In another order no attribute then takes another's name.
   * Returns the attributes.
   */
  private List<Field> pinAvroNames() {
    if (!avroNamesPinned) {
      ParsedSchema written = ColumnIds.nativeOf(subject, format, withoutChecks(), refused());
      try {
        pinNames(LogicalTypeConversion.toLogicalType(written));
      } catch (RuntimeException e) {
        throw new TypeRefusedException(refused() + ": " + e.getMessage(), e);
      }
      avroNamesPinned = true;
    }
    return new ArrayList<>(struct().getFields());
  }

  // Whether an action gives no attribute a type: a move, a drop, a comment, tags or a CHECK.
  // An attribute it adds is told in its own place.
  private static boolean leavesNamesPinned(AlterAction action) {
    return action instanceof AlterAction.MoveAttribute
        || action instanceof AlterAction.DropAttribute
        || action instanceof AlterAction.SetComment
        || action instanceof AlterAction.SetTags
        || action instanceof AlterAction.SetAttributeComment
        || action instanceof AlterAction.SetAttributeTags
        || action instanceof AlterAction.AddConstraint
        || action instanceof AlterAction.DropConstraint
        || action instanceof AlterAction.AddAttributeCheck;
  }

  // The type as it stands with no CHECK on its struct or attributes: one may only hold of the
  // attributes the statement leaves, and no name depends on it.
  private LogicalType withoutChecks() {
    Schema struct = struct();
    List<Field> fields = new ArrayList<>();
    for (Field f : struct.getFields()) {
      fields.add(Constraints.withRules(f, List.of()));
    }
    Schema bare = copyMetadata(struct, Schema.createStruct(fields))
        .setParams(struct.getParams()).setRules(List.of());
    Map<String, Schema> types = new LinkedHashMap<>(namedTypes);
    boolean named = struct != root;
    if (named) {
      types.put(root.getQualifiedName(), bare);
    }
    return new LogicalType(original.getName(), original.getNamespace(), named ? root : bare,
        types, externalTypes, externalImports, references, resolved, Map.of());
  }

  /**
   * Refuses a version that renames a message or enum the fields of the version before had
   * because its name is taken: an attribute, a oneof member and the types a message declares
   * share its names, and the writer gives a type whose name is taken another. The registry
   * would take it for another type. A file that is no valid Protobuf is refused here too,
   * where the registry would only say so. The names of the version edited are pinned, so
   * this is left for an attribute given another kind, whose types are named anew.
   */
  private void refuseTakenNames(ParsedSchema stored, ParsedSchema written) {
    if (!(stored instanceof ProtobufSchema)) {
      return;
    }
    Descriptor now;
    try {
      now = ((ProtobufSchema) written).toDescriptor();
    } catch (RuntimeException e) {
      throw new TypeRefusedException(refused() + ": no valid " + format
          + " schema can be made of it: " + e.getMessage(), e);
    }
    Descriptor was = ((ProtobufSchema) stored).toDescriptor();
    if (was != null && now != null) {
      refuseTakenNames(was, now);
    }
  }

  // By their numbers, which a field keeps: the types declared in the two messages for a field
  // of both, and so on down.
  private void refuseTakenNames(Descriptor was, Descriptor now) {
    for (FieldDescriptor old : was.getFields()) {
      FieldDescriptor kept = now.findFieldByNumber(old.getNumber());
      if (kept == null || kept.getType() != old.getType()) {
        continue;
      }
      String before;
      String after;
      if (old.getType() == FieldDescriptor.Type.MESSAGE
          && old.getMessageType().getContainingType() == was
          && kept.getMessageType().getContainingType() == now) {
        before = old.getMessageType().getName();
        after = kept.getMessageType().getName();
        refuseTakenNames(old.getMessageType(), kept.getMessageType());
      } else if (old.getType() == FieldDescriptor.Type.ENUM
          && old.getEnumType().getContainingType() == was
          && kept.getEnumType().getContainingType() == now) {
        before = old.getEnumType().getName();
        after = kept.getEnumType().getName();
      } else {
        continue;
      }
      // A oneof's name is one of the message's too.
      if (!before.equals(after) && (now.findFieldByName(before) != null
          || now.findNestedTypeByName(before) != null
          || now.findEnumTypeByName(before) != null
          || now.getOneofs().stream().anyMatch(oneof -> oneof.getName().equals(before)))) {
        throw new TypeRefusedException(refused() + ": " + before + ", the type of "
            + old.getName() + " in Protobuf message " + was.getName() + ", would be renamed "
            + after + ", as another member or type of the message would have its name");
      }
    }
  }

  // A type with the protobuf.name params another reading of it has, where it has none itself;
  // where the two differ in shape, as it is.
  private static Schema pinned(Schema type, Schema named) {
    if (named == null || type.getType() != named.getType()) {
      return type;
    }
    Schema result;
    switch (type.getType()) {
      case STRUCT: {
        List<Field> fields = new ArrayList<>();
        for (Field f : type.getFields()) {
          Field other = named.getField(f.getName());
          fields.add(other == null ? f : copy(f, f.getName(),
              pinned(f.getSchema(), other.getSchema()),
              withName(f.getParams(), other.getParams())));
        }
        result = Schema.createStruct(fields);
        break;
      }
      case UNION: {
        List<UnionBranch> branches = new ArrayList<>();
        for (UnionBranch b : type.getBranches()) {
          UnionBranch other = branch(named, b.getName());
          branches.add(other == null ? b : new UnionBranch(b.getName(),
              pinned(b.getSchema(), other.getSchema()), b.getDoc(),
              withName(b.getParams(), other.getParams())));
        }
        result = Schema.createUnion(branches);
        break;
      }
      case ARRAY:
        result = Schema.createArray(pinned(type.getElementType(), named.getElementType()));
        break;
      case MULTISET:
        result = Schema.createMultiset(pinned(type.getElementType(), named.getElementType()));
        break;
      case MAP:
        result = Schema.createMap(pinned(type.getKeyType(), named.getKeyType()),
            pinned(type.getValueType(), named.getValueType()));
        break;
      default:
        return type;
    }
    return copyMetadata(type, result).setParams(type.getParams());
  }

  private static Map<String, Object> withName(Map<String, Object> params,
      Map<String, Object> named) {
    Map<String, Object> out = null;
    for (Map.Entry<String, Object> name : named.entrySet()) {
      if (Schema.isTypeNameParam(name.getKey()) && !params.containsKey(name.getKey())) {
        out = out != null ? out : new LinkedHashMap<>(params);
        out.put(name.getKey(), name.getValue());
      }
    }
    return out != null ? out : params;
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
        message.numbers().stream().filter(n -> n > 0).forEach(usedNumbers::add);
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
    // The numbers of the fields that hold a oneof wrapped in a message, which the union
    // read of each has: no member's, and no new attribute's once the field is dropped.
    private final Set<Integer> wrappers;

    Message(List<Member> members, Map<String, Schema> namedTypes) {
      this(members, namedTypes, Set.of());
    }

    Message(List<Member> members, Map<String, Schema> namedTypes, Set<Integer> wrappers) {
      this.members = members;
      this.namedTypes = namedTypes;
      this.wrappers = wrappers;
    }

    List<Member> members() {
      return members;
    }

    // Every number the message has used: its members' and its wrappers'.
    Set<Integer> numbers() {
      Set<Integer> numbers = new HashSet<>(wrappers);
      members.forEach(member -> numbers.add(member.number()));
      return numbers;
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
          && Objects.equals(namedTypes, other.namedTypes)
          && Objects.equals(wrappers, other.wrappers);
    }

    @Override
    public int hashCode() {
      return Objects.hash(members, namedTypes, wrappers);
    }


    // A struct's message; null for any other type.
    static Message of(Schema type, Map<String, Schema> namedTypes) {
      if (type == null || type.getType() != Schema.Type.STRUCT) {
        return null;
      }
      Map<Object, Integer> implied = ProtoToLogicalTypeConverter.impliedFieldNumbers(type);
      List<Member> members = new ArrayList<>();
      Set<Integer> wrappers = new HashSet<>();
      for (Field f : type.getFields()) {
        if (f.getSchema().getType() == Schema.Type.UNION) {
          for (UnionBranch b : f.getSchema().getBranches()) {
            members.add(new Member(memberName(b),
                number(b.getFieldNumber(), b, implied), b.getSchema()));
          }
          if (numberIn(f.getParams()) > 0) {
            wrappers.add(numberIn(f.getParams()));
          }
        } else {
          members.add(new Member(f.getName(), number(f.getFieldNumber(), f, implied),
              f.getSchema()));
        }
      }
      return new Message(members, namedTypes, wrappers);
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
          for (int number : m.numbers()) {
            max = Math.max(max, number);
          }
        }
      }
      return max;
    }

    // The numbers for the message's new fields: above every one a version used, and where
    // none is left there, the lowest that none used.
    Counter counter() {
      Set<Integer> taken = new HashSet<>();
      for (Message m : all) {
        if (m != null) {
          taken.addAll(m.numbers());
        }
      }
      return new Counter(maxUsed() + 1, taken);
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
  private Schema renumbered(Schema type, Schema old, Messages at, Counter next,
      String path) {
    Schema result;
    switch (type.getType()) {
      case STRUCT: {
        Counter inner = at.counter();
        List<Field> fields = new ArrayList<>();
        for (Field f : type.getFields()) {
          Field was = old != null && old.getType() == Schema.Type.STRUCT
              ? old.getField(f.getName()) : null;
          Schema wasType = was != null ? was.getSchema() : null;
          String member = Formats.joined(path, f.getName());
          // A union attribute is a oneof of this row; others are messages of their own.
          Schema s = f.getSchema().getType() == Schema.Type.UNION
              ? renumbered(f.getSchema(), wasType, at, inner, member)
              : renumbered(f.getSchema(), wasType, at.field(f.getName()), null, member);
          Map<String, Object> params = new LinkedHashMap<>(f.getParams());
          if (s.getType() != Schema.Type.UNION) {
            int n = at.number(f.getName());
            refuseOtherNumber("Attribute " + member, params, n);
            params.put(Schema.PROTOBUF_FIELD_NUMBER,
                String.valueOf(n > 0 ? n : inner.take()));
          }
          fields.add(copy(f, f.getName(), s, params));
        }
        result = Schema.createStruct(fields);
        break;
      }
      case UNION: {
        Counter counter = next != null ? next : at.counter();
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
          refuseOtherNumber("Branch " + b.getName()
              + (path != null ? " of attribute " + path : ""), params, n);
          params.put(Schema.PROTOBUF_FIELD_NUMBER,
              String.valueOf(n > 0 ? n : counter.take()));
          branches.add(new UnionBranch(b.getName(),
              renumbered(b.getSchema(), was != null ? was.getSchema() : null,
                at.field(n), null, Formats.joined(path, b.getName())),
              b.getDoc(), params));
        }
        result = Schema.createUnion(branches);
        break;
      }
      case ARRAY:
        result = Schema.createArray(renumbered(type.getElementType(),
            old != null && old.getType() == Schema.Type.ARRAY ? old.getElementType() : null,
            at, null, path));
        break;
      case MULTISET: {
        // Entries of the element, as the key, and its count.
        boolean wasMultiset = old != null && old.getType() == Schema.Type.MULTISET;
        result = Schema.createMultiset(renumbered(type.getElementType(),
            wasMultiset ? old.getElementType() : null,
            entryPart(at, type.getElementType(), "key"), null, path));
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
        Counter entry = at.counter();
        result = Schema.createMap(
            renumbered(type.getKeyType(), wasMap ? old.getKeyType() : null,
              entryPart(at, type.getKeyType(), "key"),
              isUnion(type.getKeyType()) ? entry : null, path),
            renumbered(type.getValueType(), wasMap ? old.getValueType() : null,
              entryPart(at, type.getValueType(), VALUE),
              isUnion(type.getValueType()) ? entry : null, path));
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
    Object pinned = b.getParams().get(Schema.PROTOBUF_FIELD_NAME);
    if (pinned != null) {
      return pinned.toString();
    }
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
  private static Schema withDefaults(Schema old, Schema type, String path) {
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
            String member = Formats.joined(path, f.getName());
            fields.add(defaulted(f, was,
                withDefaults(was.getSchema(), f.getSchema(), member), member));
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
              withDefaults(was.getSchema(), b.getSchema(),
                path == null ? null : Formats.joined(path, b.getName())), b.getDoc(),
              b.getParams()));
        }
        result = Schema.createUnion(branches);
        break;
      }
      case ARRAY:
        result = Schema.createArray(
            withDefaults(old.getElementType(), type.getElementType(), path));
        break;
      case MULTISET:
        result = Schema.createMultiset(
            withDefaults(old.getElementType(), type.getElementType(), path));
        break;
      case MAP:
        result = Schema.createMap(withDefaults(old.getKeyType(), type.getKeyType(), path),
            withDefaults(old.getValueType(), type.getValueType(), path));
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
   * Numbers the branches of a union added as a oneof, as {@code next} counts.
   */
  private static Schema numberBranches(Schema type, Counter next) {
    if (type.getType() != Schema.Type.UNION) {
      return type;
    }
    List<UnionBranch> branches = new ArrayList<>();
    for (UnionBranch b : type.getBranches()) {
      Map<String, Object> params = new LinkedHashMap<>(b.getParams());
      params.put(Schema.PROTOBUF_FIELD_NUMBER, String.valueOf(next.take()));
      branches.add(new UnionBranch(b.getName(), b.getSchema(), b.getDoc(), params));
    }
    return copyMetadata(type, Schema.createUnion(branches)).setParams(type.getParams());
  }

  // The number a new attribute says, where no field has it and none of a version had; the
  // next free one where it says none, as for a union, whose members are numbered.
  private int numberGiven(Field field, List<Field> fields, Counter numbers) {
    int given = numberIn(field.getParams());
    if (given <= 0 || field.getSchema().getType() == Schema.Type.UNION) {
      return numbers.take();
    }
    String has = null;
    for (Field f : fields) {
      // A union is no field: its members are.
      if (f.getSchema().getType() != Schema.Type.UNION && numberIn(f.getParams()) == given) {
        has = "attribute " + Formats.segment(f.getName()) + " has it";
      }
      if (f.getSchema().getType() == Schema.Type.UNION) {
        for (UnionBranch b : f.getSchema().getBranches()) {
          if (numberIn(b.getParams()) == given) {
            has = "branch " + b.getName() + " of attribute " + Formats.segment(f.getName())
                + " has it";
          }
        }
      }
    }
    if (has == null && (usedNumbers.contains(given) || unreserved(given) != given)) {
      has = usedNumbers.contains(given) ? "an attribute of an earlier version had it"
          : "Protobuf keeps it for itself";
    }
    if (has != null) {
      throw new TypeRefusedException("Attribute " + Formats.segment(field.getName())
          + " cannot have the "
          + "Protobuf field number " + given + ": " + has + "; give it another, or none");
    }
    return given;
  }

  // A member of a type given anew keeps the number it has, and one new to it takes the
  // next its message has free: another number said for it is refused, not left out.
  private static void refuseOtherNumber(String member, Map<String, Object> params,
      int has) {
    if (!params.containsKey(Schema.PROTOBUF_FIELD_NUMBER) || numberIn(params) == has) {
      return;
    }
    throw new TypeRefusedException(member
        + " cannot be given the Protobuf field number " + numberIn(params) + ": "
        + (has > 0 ? "it has " + has : "a member new to a type given anew takes the next "
          + "number its message has free") + "; give it none");
  }

  // A new union's members are numbered as the type's new fields are: a number said for
  // one is refused, not left out. One said for the union itself is refused with every
  // attribute's, at any depth.
  private static void refuseUnionNumbers(Field field) {
    if (field.getSchema().getType() != Schema.Type.UNION) {
      return;
    }
    for (UnionBranch b : field.getSchema().getBranches()) {
      if (b.getParams().containsKey(Schema.PROTOBUF_FIELD_NUMBER)) {
        throw new TypeRefusedException("Branch " + b.getName() + " of attribute "
            + Formats.segment(field.getName())
            + " cannot be given a Protobuf field number: the members of "
            + "a new UNION are numbered as the type's new fields are; give it none");
      }
    }
  }

  // The numbers for the type's new fields, with every one a field or a branch has, and
  // every one a version had.
  private Counter counter(List<Field> fields) {
    Set<Integer> taken = new HashSet<>(usedNumbers);
    for (Field f : fields) {
      taken.add(numberIn(f.getParams()));
      if (f.getSchema().getType() == Schema.Type.UNION) {
        f.getSchema().getBranches().forEach(b -> taken.add(numberIn(b.getParams())));
      }
    }
    int max = 0;
    for (int number : taken) {
      max = Math.max(max, number);
    }
    return new Counter(max + 1, taken);
  }

  // The numbers of a message's new fields: above every one used, so that a new field never
  // takes the data of a dropped one, and where none is left there, the lowest never used.
  private static final class Counter {
    private final Set<Integer> taken;
    private int next;
    private boolean below;

    Counter(int next, Set<Integer> taken) {
      this.next = next;
      this.taken = taken;
    }

    int take() {
      int number = unreserved(next);
      if (below || number > MAX_NUMBER) {
        number = below ? number : 1;
        below = true;
        while (taken.contains(number)) {
          number = unreserved(number + 1);
        }
      }
      next = number + 1;
      return number;
    }
  }

  // The number given, or the first after those Protobuf keeps for itself.
  private static int unreserved(int number) {
    return number >= RESERVED_FIRST && number <= RESERVED_LAST ? RESERVED_LAST + 1 : number;
  }

  private static int numberIn(Map<String, Object> params) {
    Object n = params.get(Schema.PROTOBUF_FIELD_NUMBER);
    return n == null ? 0 : Integer.parseInt(n.toString());
  }

  // An avro. or protobuf. property, such as a field number, an alias or a pinned type name,
  // is how the format reads existing data, which this package keeps. One as it is may be given.
  private void refuseNativeParams(Map<String, String> with, Map<String, Object> params,
      String kind, String name) {
    for (Map.Entry<String, String> e : with.entrySet()) {
      Object was = params.get(e.getKey());
      if (Schema.isFormatNativeParam(e.getKey())
          && (was == null || !was.toString().equals(e.getValue()))) {
        throw new TypeRefusedException("Property " + e.getKey() + " of " + kind + " " + name
            + " cannot be set: it is how the " + format + " format reads the " + kind
            + "'s existing data");
      }
    }
  }

  // The index an attribute takes among the others at a position.
  private int place(List<Field> others, AlterAction.Position position) {
    if (position.after() == null) {
      return 0;
    }
    int i = named(others, position.after());
    if (i < 0) {
      throw new TypeRefusedException("Type " + subject + " has no attribute "
          + Formats.segment(position.after()));
    }
    return i + 1;
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
  private static Field defaulted(Field f, Field was, Schema type, String path) {
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
        AvroDefaults.converted(path, was.getSchema(), type, was.getDefaultValue()),
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

  // A replacing type keeps, at every level below it, the native names pinned on the members it
  // keeps, one of the name and the kind it was, whatever name the replacement pins on it: the
  // type is the one it was, which another name would not be.
  private Schema withNestedNames(Schema type, Schema old) {
    if (old == null || type.getType() != old.getType()) {
      return type;
    }
    Schema result;
    switch (type.getType()) {
      case STRUCT: {
        List<Field> fields = new ArrayList<>();
        for (Field f : type.getFields()) {
          Field was = old.getField(f.getName());
          fields.add(was == null || !keepsNames(was.getSchema(), f.getSchema()) ? f
              : copy(f, f.getName(), withNestedNames(f.getSchema(), was.getSchema()),
                  withTypeNames(f.getParams(), was.getParams())));
        }
        result = Schema.createStruct(fields);
        break;
      }
      case UNION: {
        List<UnionBranch> branches = new ArrayList<>();
        for (UnionBranch b : type.getBranches()) {
          UnionBranch was = branch(old, b.getName());
          // A member is the field it was whatever its type becomes: its field name stays.
          branches.add(was == null ? b : !keepsNames(was.getSchema(), b.getSchema(), true)
              ? new UnionBranch(b.getName(), b.getSchema(), b.getDoc(),
                  withFieldName(b.getParams(), was.getParams()))
              : new UnionBranch(b.getName(), withNestedNames(b.getSchema(), was.getSchema()),
                  b.getDoc(), withTypeNames(b.getParams(), was.getParams())));
        }
        result = Schema.createUnion(branches);
        break;
      }
      case ARRAY:
        result = Schema.createArray(
            withNestedNames(type.getElementType(), old.getElementType()));
        break;
      case MULTISET:
        result = Schema.createMultiset(
            withNestedNames(type.getElementType(), old.getElementType()));
        break;
      case MAP:
        result = Schema.createMap(withNestedNames(type.getKeyType(), old.getKeyType()),
            withNestedNames(type.getValueType(), old.getValueType()));
        break;
      default:
        return type;
    }
    return copyMetadata(type, result).setParams(type.getParams());
  }

  private static Map<String, Object> withFieldName(Map<String, Object> params,
      Map<String, Object> was) {
    Object name = was.get(Schema.PROTOBUF_FIELD_NAME);
    if (name == null || name.equals(params.get(Schema.PROTOBUF_FIELD_NAME))) {
      return params;
    }
    Map<String, Object> out = new LinkedHashMap<>(params);
    out.put(Schema.PROTOBUF_FIELD_NAME, name);
    return out;
  }

  // Params with the native names another member had, in place of any they give.
  private static Map<String, Object> withTypeNames(Map<String, Object> params,
      Map<String, Object> was) {
    Map<String, Object> out = null;
    for (Map.Entry<String, Object> name : was.entrySet()) {
      if (Schema.isTypeNameParam(name.getKey())
          && !name.getValue().equals(params.get(name.getKey()))) {
        out = out != null ? out : new LinkedHashMap<>(params);
        out.put(name.getKey(), name.getValue());
      }
    }
    return out != null ? out : params;
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

  // Whether a member keeps the native names pinned on it: its type is of the same kind, and
  // in Protobuf the writer generates the same sort of type for it. There a collection that
  // may be null is wrapped, one that may not is its entries or elements, and a nullable
  // element is wrapped; in Avro it is the same type with a null beside it.
  private boolean keepsNames(Schema a, Schema b) {
    return keepsNames(a, b, false);
  }

  // asBranch: a union's member may be null whatever is said of it, and is wrapped so.
  private boolean keepsNames(Schema a, Schema b, boolean asBranch) {
    if (!sameKind(a, b)) {
      return false;
    }
    if (format != SchemaType.PROTOBUF) {
      return true;
    }
    boolean wrapped = asBranch || a.isNullable() == b.isNullable();
    switch (a.getType()) {
      case ARRAY:
        return wrapped && a.getElementType().isNullable() == b.getElementType().isNullable();
      case MAP:
      case MULTISET:
        return wrapped;
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
