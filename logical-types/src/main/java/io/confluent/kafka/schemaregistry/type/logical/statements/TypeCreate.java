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

import io.confluent.kafka.schemaregistry.client.SchemaMetadata;
import io.confluent.kafka.schemaregistry.type.logical.LogicalType;
import io.confluent.kafka.schemaregistry.type.logical.SchemaType;
import io.confluent.kafka.schemaregistry.type.logical.statements.TypeScripts.BodyKind;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Makes a type from the text of a CREATE TYPE statement, checked as far as it can be without
 * the registry, and gives it for the caller to register.
 *
 * <p>A type created again after it was dropped keeps the versions it had, whose data the new
 * version reads, so it is made as an ALTER of the last of them would make it.
 */
public final class TypeCreate {

  private TypeCreate() {
  }

  /**
   * The first version of a type, or its next one over the versions it had when it was dropped:
   * the caller sets the policy, if the result has one, and registers the type as the result's
   * version.
   *
   * @param name the type's name, dotted if it is in a namespace: its last part is the name its
   *     schema declares
   * @param kind how the statement's body was written
   * @param body the statement's body, as written
   * @param clauses a struct's or enum's COMMENT and TAGS clauses, as written, or empty
   * @param with the statement's WITH: its format, Avro if it names none, and its compatibility
   *     policy are taken from it, and the rest are the type's properties
   * @param policy the compatibility policy the subject gets if the WITH names none, or null to
   *     leave it
   */
  public static TypeAlter.Result create(TypeLookup lookup, String subject, String name,
      BodyKind kind, String body, String clauses, Map<String, String> with, String policy) {
    Map<String, String> params = new LinkedHashMap<>(with);
    SchemaType format = Formats.of(TypeAlter.removeIgnoreCase(params, TypeAlter.FORMAT, "AVRO"));
    String stated = TypeAlter.removePolicy(params);
    String applied = stated != null ? stated : policy;
    // Read first, so that a version registered after this is caught.
    int version = TypeAlter.nextVersion(lookup, subject);
    if (lookup.latest(subject) != null) {
      throw new TypeRefusedException("Type " + subject + " already exists");
    }
    // The last version of the type as it was dropped, if it was; null if none.
    SchemaMetadata last = version > 1 ? lookup.version(subject, version - 1) : null;
    refuseOtherFormat(subject, last, format);

    int dot = name.lastIndexOf('.');
    LogicalType parsed = TypeReferences.parse(lookup, format,
        aliases -> TypeScripts.createScript(name.substring(dot + 1), kind, body, clauses,
            params, aliases), subject, Map.of());
    LogicalType lt = Constraints.named(kind != BodyKind.OTHER && dot > 0
        ? TypeScripts.withNamespace(parsed, name.substring(0, dot)) : parsed, subject);
    Formats.refuseUnwritable(format, lt);
    if (last != null) {
      lt = afterDrop(lookup, subject, format, last, lt, applied);
    }
    return new TypeAlter.Result(format, last, version, lt, applied);
  }

  private static LogicalType afterDrop(TypeLookup lookup, String subject, SchemaType format,
      SchemaMetadata last, LogicalType created, String policy) {
    String refused = "Type " + subject + " cannot be created again so";
    LogicalType lt = created;
    // Its dropped versions are read with the new ones, so a Protobuf type keeps their numbers.
    if (format == SchemaType.PROTOBUF) {
      lt = TypeAlter.numberedAfterDrop(lookup, subject, last, lt);
    }
    // And an Avro type gives the nullable attributes it adds a null default, as an ALTER does.
    if (format == SchemaType.AVRO) {
      lt = TypeAlter.defaultedAfterDrop(lookup, subject, last, lt);
    }
    // And it is refused where an ALTER of the last of them would be: it refers to no older
    // version of a type than the last did, pairs its attributes as one, and is compatible.
    TypeAlter.refuseOlderReferences(lookup, subject, last.getReferences(), lt.getReferences(),
        policy, refused, "leave out");
    TypeAlter.refuseOtherColumnsAfterDrop(lookup, subject, format, last, lt);
    TypeAlter.refuseIncompatible(lookup, subject, format, last, lt, policy, refused,
        v -> "version " + v);
    return lt;
  }

  // A dropped type keeps its versions, which provenance reads with the new ones, so it is
  // created again only in its format.
  private static void refuseOtherFormat(String subject, SchemaMetadata last, SchemaType format) {
    if (last == null) {
      return;
    }
    SchemaType was = Formats.ofSchemaType(last.getSchemaType());
    if (was != format) {
      throw new TypeRefusedException("Type " + subject + " was dropped as " + was + ", and "
          + "keeps the versions it had; it can be created again only as " + was);
    }
  }
}
