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

import com.google.protobuf.Descriptors.Descriptor;
import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.client.SchemaMetadata;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaReference;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import io.confluent.protobuf.type.Variant;
import io.confluent.kafka.schemaregistry.type.logical.LogicalType;
import io.confluent.kafka.schemaregistry.type.logical.SchemaType;
import io.confluent.kafka.schemaregistry.type.logical.protobuf.ProtoToLogicalTypeConverter;
import io.confluent.kafka.schemaregistry.utils.QualifiedSubject;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * The types a type names: each is another subject, which the type refers to by a schema
 * reference to its latest version.
 */
public final class TypeReferences {

  /** Builds a script from the aliases it needs. */
  public interface ScriptBuilder {
    String build(Map<String, String> aliases);
  }

  private TypeReferences() {
  }

  /**
   * Parses a script whose type names may refer to other types, resolving each to the latest
   * version of its subject. A type whose schema declares a full name other than its subject is
   * aliased to it.
   * <ul>
   * <li>For the type of the referrer subject, if given, a name without a context is of the
   * subject's context, as the registry reads a reference so, the default context having no name
   * another context's subject can give it.</li>
   * <li>A JSON reference to a type takes the name a reference to it is stored with, if names
   * holds one for its subject; otherwise its name, or for a type in a context, which is no URI
   * a $ref can name, that name encoded.</li>
   * <li>An Avro or Protobuf reference to a type takes the name a type named beside it refers
   * to it by: referred to by two names, a Protobuf file would be imported twice.</li>
   * </ul>
   */
  public static LogicalType parse(TypeLookup lookup, SchemaType format, ScriptBuilder builder,
      String referrer, Map<String, String> names) {
    LogicalType lt = TypeScripts.parse(builder.build(Map.of()));
    if (lt.getExternalTypes().isEmpty()) {
      return lt;
    }
    Map<String, String> aliases = new LinkedHashMap<>();
    List<SchemaReference> references = new ArrayList<>();
    Map<String, String> resolved = new LinkedHashMap<>();
    // A JSON $ref names one document: a name two types would take is refused.
    Map<String, String> named = format == SchemaType.JSON ? new HashMap<>() : null;
    // The names the types named refer to their own types by, by subject.
    Map<String, String> bound = format == SchemaType.JSON ? null : new HashMap<>();
    // The name each subject is referred to by: one named two ways, with its context and
    // without, is referred to once.
    Map<String, String> spelt = new HashMap<>();
    // The type itself, as a reference would name it; null when it has no subject yet.
    String self = referrer == null ? null : referenced(referrer, referrer);
    for (String name : lt.getExternalTypes()) {
      String subject = referenced(referrer, name);
      if (subject.equals(self)) {
        throw new TypeRefusedException("Type " + self + " cannot refer to itself");
      }
      SchemaMetadata md = lookup.latest(subject);
      if (md == null) {
        throw new TypeRefusedException("Unknown type " + subject);
      }
      SchemaType refFormat = Formats.ofSchemaType(md.getSchemaType());
      if (refFormat != format) {
        throw new TypeRefusedException("Type " + name + " is " + refFormat
            + ", so it cannot be used in a " + format + " type");
      }
      String stored = names.get(subject);
      String refName = format != SchemaType.JSON ? name
          : stored != null && nameable(stored) ? stored : jsonReferenceName(name);
      String once = format != SchemaType.JSON ? spelt.putIfAbsent(subject, refName) : null;
      if (once != null) {
        refName = once;
      } else {
        references.add(new SchemaReference(refName, name, md.getVersion()));
      }
      requireOneSubject(named, refName, subject);
      // A name bound already, as one a type named before it refers to, keeps that binding,
      // as the registry's resolver has it: a schema holds one version of a name.
      if (!resolved.containsKey(refName)) {
        try {
          collectReferences(lookup, subject, md.getReferences(), resolved, named, self, bound);
        } catch (RefersBack e) {
          throw new TypeRefusedException("Type " + self + " cannot refer to type " + subject
              + ", which refers to " + self);
        }
        resolved.put(refName, md.getSchema());
      }
      if (format == SchemaType.JSON) {
        if (!refName.equals(name)) {
          aliases.put(name, refName);
        }
      } else {
        ParsedSchema parsed = lookup.parse(md);
        refuseWrappedValue(name, parsed);
        String fullName = parsed.name();
        if (fullName != null && !fullName.equals(name)) {
          aliases.put(name, fullName);
        }
      }
    }
    if (!aliases.isEmpty()) {
      lt = TypeScripts.parse(builder.build(aliases));
    }
    if (bound != null) {
      nameAsBound(referrer, references, resolved, bound);
    }
    return TypeScripts.withReferences(lt, references, resolved);
  }

  // A Protobuf type that is a value and no row is a google.protobuf wrapper, which a field
  // of is read as the value it wraps: the type referring to it would be read as one that
  // does not, and written otherwise than it is stored. So is a VARIANT, which is written
  // as the format's own message of that name.
  private static void refuseWrappedValue(String name, ParsedSchema parsed) {
    Descriptor root;
    try {
      root = parsed instanceof ProtobufSchema ? ((ProtobufSchema) parsed).toDescriptor() : null;
    } catch (RuntimeException e) {
      return;
    }
    if (root != null && (ProtoToLogicalTypeConverter.isScalarWrapper(root)
        || root.getFullName().equals(Variant.getDescriptor().getFullName()))) {
      throw new TypeRefusedException("Type " + name + " cannot be used in a PROTOBUF type: "
          + "it is a value and no row, written as a " + root.getFullName() + ", and an "
          + "attribute of it would be read as that value and not as the type. Give the "
          + "attribute the value's type");
    }
  }

  // Gives a reference the name a type named beside it refers to its subject by, where it has
  // another: its schema is then held once, under the one name.
  private static void nameAsBound(String referrer, List<SchemaReference> references,
      Map<String, String> resolved, Map<String, String> bound) {
    for (int i = 0; i < references.size(); i++) {
      SchemaReference ref = references.get(i);
      String name = bound.get(referenced(referrer, ref.getSubject()));
      if (name == null || name.equals(ref.getName()) || !resolved.containsKey(name)
          || isNameOf(referrer, name)) {
        continue;
      }
      references.set(i, new SchemaReference(name, ref.getSubject(), ref.getVersion()));
      resolved.remove(ref.getName());
    }
  }

  /**
   * Whether a name is the one a type is referred to by: its subject, with its context or
   * without. A type it refers to is not given that name, which a type referring to this one
   * would read as this one's.
   */
  static boolean isNameOf(String subject, String name) {
    if (subject == null) {
      return false;
    }
    QualifiedSubject qualified = QualifiedSubject.create(QualifiedSubject.DEFAULT_TENANT,
        subject);
    return name.equals(subject) || qualified != null && name.equals(qualified.getSubject());
  }

  /**
   * The names the types a subject refers to give the types they refer to in turn, by subject,
   * the first for each: the names its own references to those subjects are to have.
   */
  static Map<String, String> namesBelow(TypeLookup lookup, String referrer,
      List<SchemaReference> references) {
    Map<String, String> bound = new HashMap<>();
    Map<String, String> seen = new LinkedHashMap<>();
    for (SchemaReference ref : references) {
      String subject = referenced(referrer, ref.getSubject());
      SchemaMetadata md = lookup.version(subject, ref.getVersion());
      if (md != null) {
        collectReferences(lookup, subject, md.getReferences(), seen, null, null, bound);
      }
    }
    return bound;
  }

  /**
   * The logical type of a stored version, from the logical types DDL the registry gives for it
   * as {@code format=logical}, with the version's references resolved: what
   * {@link TypeLookup#logicalType} answers.
   */
  public static LogicalType read(TypeLookup lookup, String subject, SchemaMetadata version,
      String ddl) {
    List<SchemaReference> references = version.getReferences() != null
        ? version.getReferences() : List.of();
    boolean protobuf = Formats.ofSchemaType(version.getSchemaType()) == SchemaType.PROTOBUF;
    return TypeScripts.withReferences(TypeScripts.parse(ddl, protobuf), references,
        resolve(lookup, subject, references));
  }

  /**
   * The names a reference may be known by in a referring type; null if they cannot be told,
   * because the registry has no such version or its schema cannot be parsed. A lookup that
   * fails is neither, and fails here as it does.
   */
  /**
   * The types a type's references are of, by the names the type has for them: each
   * reference's name and the names imported from it whole. Its subject and the name the
   * referred type gives itself are no names of the type's, and may be another reference's.
   * Each has the names of its own: its named types, and the types it refers to in turn. A
   * reference that cannot be read is left out.
   */
  static KnownTypes referredTypes(TypeLookup lookup, String referrer,
      List<SchemaReference> references, Map<String, String> imports) {
    return referredTypes(lookup, referrer, references, imports, new HashMap<>());
  }

  /**
   * As {@link #referredTypes(TypeLookup, String, List, Map)}, with the versions read so far,
   * each read once: a type many refer to is come to by as many ways.
   */
  static KnownTypes referredTypes(TypeLookup lookup, String referrer,
      List<SchemaReference> references, Map<String, String> imports,
      Map<String, Referred> read) {
    return referredTypes(lookup, referrer, references, imports, read, new HashSet<>());
  }

  /** A version referred to, as read: its type and the names it has of its own. */
  static final class Referred {
    private final LogicalType type;
    private final KnownTypes names;

    private Referred(LogicalType type, KnownTypes names) {
      this.type = type;
      this.names = names;
    }
  }

  private static KnownTypes referredTypes(TypeLookup lookup, String referrer,
      List<SchemaReference> references, Map<String, String> imports,
      Map<String, Referred> read, Set<String> within) {
    KnownTypes types = KnownTypes.none();
    if (references == null) {
      return types;
    }
    for (SchemaReference ref : references) {
      String subject = referenced(referrer, ref.getSubject());
      String key = subject + " " + ref.getVersion();
      Referred referred = read.get(key);
      // One that refers back to a type being read is left out.
      if (referred == null && within.add(key)) {
        // A registry that cannot be asked fails the statement; a version that is not
        // there, or cannot be read as a type, is left out.
        SchemaMetadata md = lookup.version(subject, ref.getVersion());
        LogicalType type;
        try {
          type = md == null ? null : lookup.logicalType(subject, md);
        } catch (RuntimeException e) {
          type = null;
        }
        if (type != null) {
          KnownTypes names = referredTypes(lookup, subject, type.getReferences(),
              type.getExternalImports(), read, within);
          type.getNamedTypes().forEach(names::put);
          referred = new Referred(type, names);
          read.put(key, referred);
        }
        within.remove(key);
      }
      if (referred == null) {
        continue;
      }
      Set<String> called = new LinkedHashSet<>(List.of(ref.getName()));
      // A name imported with a fragment is of a part of the document, which is not told.
      imports.forEach((name, from) -> {
        if (ref.getName().equals(from)) {
          called.add(name);
        }
      });
      for (String name : called) {
        types.putReferred(name, referred.type.getRootSchema(), referred.names);
      }
    }
    return types;
  }

  public static Set<String> definedNames(TypeLookup lookup, String referrer,
      SchemaReference ref) {
    SchemaMetadata md = lookup.version(referenced(referrer, ref.getSubject()),
        ref.getVersion());
    if (md == null) {
      return null;
    }
    Set<String> names = new LinkedHashSet<>(List.of(ref.getName(), ref.getSubject()));
    String fullName;
    try {
      fullName = lookup.parse(md).name();
    } catch (RuntimeException e) {
      return null;
    }
    if (fullName != null) {
      names.add(fullName);
    }
    return names;
  }

  /**
   * The schema texts of a subject's references and of everything they refer to, by reference
   * name.
   */
  public static Map<String, String> resolve(TypeLookup lookup, String referrer,
      List<SchemaReference> references) {
    Map<String, String> resolved = new LinkedHashMap<>();
    collectReferences(lookup, referrer, references, resolved, null, null, null);
    return resolved;
  }

  /**
   * Whether a reference's name can name a type in the logical types DDL, whose writer splits a
   * name on its dots: not one with an empty part, as a relative path's ./ or ../ has.
   */
  public static boolean nameable(String name) {
    return Arrays.stream(name.split("\\.", -1)).noneMatch(String::isEmpty);
  }

  /**
   * The subject a reference names, as the registry resolves it: one without a context of its
   * own is in the referring subject's.
   */
  public static String referenced(String referrer, String subject) {
    if (referrer == null || subject == null) {
      return subject;
    }
    QualifiedSubject qualified = QualifiedSubject.qualifySubjectWithParent(
        QualifiedSubject.DEFAULT_TENANT, referrer, subject);
    return qualified == null ? subject
        : QualifiedSubject.DEFAULT_CONTEXT.equals(qualified.getContext())
            ? qualified.getSubject() : qualified.toQualifiedSubject();
  }

  // A name a JSON $ref can take: a context's prefix and delimiters are no URI's.
  private static String jsonReferenceName(String name) {
    return name.startsWith(QualifiedSubject.CONTEXT_PREFIX)
        ? URLEncoder.encode(name, StandardCharsets.UTF_8).replace(".", "%2E") : name;
  }

  // Each schema after those it refers to, which is the order the schema parsers read them in.
  // One that is self, the type these are collected for, would make it refer to itself.
  private static void collectReferences(TypeLookup lookup, String referrer,
      List<SchemaReference> references, Map<String, String> resolved,
      Map<String, String> named, String self, Map<String, String> bound) {
    if (references == null) {
      return;
    }
    for (SchemaReference ref : references) {
      String subject = referenced(referrer, ref.getSubject());
      if (subject.equals(self)) {
        throw new RefersBack();
      }
      requireOneSubject(named, ref.getName(), subject);
      if (bound != null) {
        bound.putIfAbsent(subject, ref.getName());
      }
      if (resolved.containsKey(ref.getName())) {
        continue;
      }
      // Soft-deleted or not, as the registry resolves a stored schema's references.
      SchemaMetadata md = lookup.version(subject, ref.getVersion());
      if (md == null) {
        throw new TypeRefusedException("Could not resolve reference " + ref.getName()
            + ": type " + subject + " has no version " + ref.getVersion());
      }
      collectReferences(lookup, subject, md.getReferences(), resolved, named, self, bound);
      resolved.put(ref.getName(), md.getSchema());
    }
  }

  // A type named leads back, through its references, to the type that names it.
  private static final class RefersBack extends RuntimeException {
    RefersBack() {
      super(null, null, false, false);
    }
  }

  /**
   * Refuses references that reach two types by one name, or two versions of one, their own or
   * those of the types they refer to. A schema holds one type and one version of a name, the
   * first its references are read to, and they are stored in another order than a statement
   * gives them: the version checked would not be the one stored. Each reference is taken with
   * what it binds read alone, and those are set beside one another, in whatever order: two
   * versions a stored type has of its own are left, as it is read through a reference as it
   * is read alone. A refusal of two versions starts with {@code refused}.
   */
  static void requireOneOfEach(TypeLookup lookup, String referrer,
      List<SchemaReference> references, String refused) {
    if (references == null) {
      return;
    }
    Map<String, SchemaMetadata> versions = new HashMap<>();
    Map<String, String[]> met = new HashMap<>();
    for (SchemaReference ref : references) {
      Map<String, String[]> alone = new LinkedHashMap<>();
      bind(lookup, referrer, null, List.of(ref), alone, versions, refused);
      for (Map.Entry<String, String[]> name : alone.entrySet()) {
        String[] now = name.getValue();
        String[] first = met.putIfAbsent(name.getKey(), now);
        if (first == null) {
          continue;
        }
        if (!first[0].equals(now[0])) {
          throw twoTypes(first[0], now[0], name.getKey());
        }
        if (!first[1].equals(now[1])) {
          throw twoVersions(refused, first, now);
        }
      }
    }
  }

  // Each name at the type and version, and through the type, it is first met, which is the
  // one that binds it; via is null for the reference itself, and else the referring type and
  // its version. A version is looked up once. The reference's own name met again below it,
  // as another type or version, is refused: read there as the reference, not as the type
  // below reads it alone.
  private static void bind(TypeLookup lookup, String referrer, String via,
      List<SchemaReference> references, Map<String, String[]> bound,
      Map<String, SchemaMetadata> versions, String refused) {
    if (references == null) {
      return;
    }
    for (SchemaReference ref : references) {
      String subject = referenced(referrer, ref.getSubject());
      String[] first = bound.get(ref.getName());
      if (first != null) {
        if (first[2] == null && via != null) {
          String[] now = {subject, String.valueOf(ref.getVersion()), via};
          if (!first[0].equals(subject)) {
            throw twoTypes(first[0], subject, ref.getName());
          }
          if (!first[1].equals(now[1])) {
            throw twoVersions(refused, first, now);
          }
        }
        continue;
      }
      bound.put(ref.getName(), new String[] {subject, String.valueOf(ref.getVersion()), via});
      String key = subject + "@" + ref.getVersion();
      SchemaMetadata md = versions.get(key);
      if (md == null) {
        md = lookup.version(subject, ref.getVersion());
        if (md == null) {
          continue;
        }
        versions.put(key, md);
      }
      bind(lookup, subject, "version " + ref.getVersion() + " of type " + subject,
          md.getReferences(), bound, versions, refused);
    }
  }

  private static TypeRefusedException twoVersions(String refused, String[] first,
      String[] now) {
    return new TypeRefusedException(refused + ": it would refer to two versions of type "
        + now[0] + ", " + met(first[1], first[2]) + " and " + met(now[1], now[2])
        + "; a schema has one version of a type, so name the type again to move it to "
        + "its latest version, or first move the types that refer to it");
  }

  private static String met(String version, String via) {
    return "version " + version + " (" + (via == null ? "named directly" : "through " + via)
        + ")";
  }

  private static TypeRefusedException twoTypes(String one, String other, String name) {
    return new TypeRefusedException("Types " + one + " and " + other + " would both be "
        + "referred to as " + name + ", which can name only one of them");
  }

  // Refuses a reference name that, among a type's references and theirs, names another type
  // too: the registry would read one of the two for both.
  private static void requireOneSubject(Map<String, String> named, String name,
      String subject) {
    if (named == null) {
      return;
    }
    String other = named.putIfAbsent(name, subject);
    if (other != null && !other.equals(subject)) {
      throw twoTypes(other, subject, name);
    }
  }
}
