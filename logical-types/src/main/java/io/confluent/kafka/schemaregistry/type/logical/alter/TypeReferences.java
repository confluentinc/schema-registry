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

import io.confluent.kafka.schemaregistry.client.SchemaMetadata;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaReference;
import io.confluent.kafka.schemaregistry.type.logical.LogicalType;
import io.confluent.kafka.schemaregistry.type.logical.SchemaType;
import io.confluent.kafka.schemaregistry.utils.QualifiedSubject;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
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
    for (String name : lt.getExternalTypes()) {
      String subject = referenced(referrer, name);
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
      references.add(new SchemaReference(refName, name, md.getVersion()));
      requireOneSubject(named, refName, subject);
      collectReferences(lookup, subject, md.getReferences(), resolved, named);
      resolved.put(refName, md.getSchema());
      if (format == SchemaType.JSON) {
        if (!refName.equals(name)) {
          aliases.put(name, refName);
        }
      } else {
        String fullName = lookup.parse(md).name();
        if (fullName != null && !fullName.equals(name)) {
          aliases.put(name, fullName);
        }
      }
    }
    if (!aliases.isEmpty()) {
      lt = TypeScripts.parse(builder.build(aliases));
    }
    return TypeScripts.withReferences(lt, references, resolved);
  }

  /** The names a reference may be known by in a referring type, or null if unknown. */
  public static Set<String> definedNames(TypeLookup lookup, String referrer,
      SchemaReference ref) {
    try {
      SchemaMetadata md = lookup.version(referenced(referrer, ref.getSubject()),
          ref.getVersion());
      if (md == null) {
        return null;
      }
      Set<String> names = new LinkedHashSet<>(List.of(ref.getName(), ref.getSubject()));
      String fullName = lookup.parse(md).name();
      if (fullName != null) {
        names.add(fullName);
      }
      return names;
    } catch (RuntimeException e) {
      return null;
    }
  }

  /**
   * The schema texts of a subject's references and of everything they refer to, by reference
   * name.
   */
  public static Map<String, String> resolve(TypeLookup lookup, String referrer,
      List<SchemaReference> references) {
    Map<String, String> resolved = new LinkedHashMap<>();
    collectReferences(lookup, referrer, references, resolved, null);
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
  private static void collectReferences(TypeLookup lookup, String referrer,
      List<SchemaReference> references, Map<String, String> resolved,
      Map<String, String> named) {
    for (SchemaReference ref : references) {
      String subject = referenced(referrer, ref.getSubject());
      requireOneSubject(named, ref.getName(), subject);
      if (resolved.containsKey(ref.getName())) {
        continue;
      }
      // Soft-deleted or not, as the registry resolves a stored schema's references.
      SchemaMetadata md = lookup.version(subject, ref.getVersion());
      if (md == null) {
        throw new TypeRefusedException("Could not resolve reference " + ref.getName()
            + ": type " + subject + " has no version " + ref.getVersion());
      }
      collectReferences(lookup, subject, md.getReferences(), resolved, named);
      resolved.put(ref.getName(), md.getSchema());
    }
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
      throw new TypeRefusedException("Types " + other + " and " + subject + " would both be "
          + "referred to as " + name + ", which can name only one of them");
    }
  }
}
