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

package io.confluent.kafka.serializers;

import io.confluent.kafka.serializers.provenance.ProvenanceMapping;
import io.confluent.kafka.serializers.provenance.ProvenanceUnavailableException;
import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Resolver;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.apache.kafka.common.errors.SerializationException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;

/**
 * Carries provenance into Avro's resolver by renaming, the way {@link Schema#applyAliases} carries
 * aliases: the writer schema is rewritten so that each field bears the name of the reader field
 * with the same provenance id, and a field with none bears a name nothing matches. The resolver
 * then does everything else — promotion, enum symbols, unions, defaults — as it always does.
 *
 * <p>The binary layout is untouched: only names change, never order or types. Both schemas are
 * walked by the response's native names. Only locations are paired; anything else — a Connect map
 * entry's {@code key} and {@code value}, a Variant's fields — keeps its name and is matched as
 * without provenance. A new id never receives the writer's value: a field with no counterpart is
 * skipped, a record branch with none is sent to a sink that fails the records containing it, and
 * any other union choice the resolver would make against provenance falls back.
 */
final class AvroProvenanceRenamer {

  private static final String UNMATCHED = "__provenance_unmatched_";
  private static final String SINK_FIELD = "__provenance_no_value";

  private final ProvenanceMapping mapping;

  // Every named type built, by full name, to catch one name given two definitions.
  private final Map<String, Schema> byName = new HashMap<>();
  // Reader types a writer type was renamed against; their aliases, and a record's field aliases,
  // must not apply again.
  private final Set<Schema> matchedReaderTypes =
      Collections.newSetFromMap(new IdentityHashMap<>());
  // Reader types at a location, paired or not. Avro applies a type alias across the whole writer,
  // so one left on a new type could rename a writer type provenance already placed elsewhere.
  private final Set<Schema> locatedReaderTypes =
      Collections.newSetFromMap(new IdentityHashMap<>());
  // The reader's own type names, which no throwaway name may take.
  private final Set<String> readerTypeNames = new HashSet<>();
  // Sink branches to append to a reader union, by the original reader union.
  private final Map<Schema, List<Schema>> sinks = new IdentityHashMap<>();
  // Clones made inside a union, by full name, and the reader type each was renamed after.
  private final Map<String, String> cloneTargets = new HashMap<>();
  // The writer's own names for a built record's fields and a built union's branches.
  private final Map<Schema, List<String>> writerNames = new IdentityHashMap<>();
  private int throwaway;

  private AvroProvenanceRenamer(ProvenanceMapping mapping) {
    this.mapping = mapping;
  }

  /**
   * A writer schema and a reader schema to hand to the resolver in place of the originals.
   */
  static final class Renamed {
    final Schema writer;
    final Schema reader;

    private Renamed(Schema writer, Schema reader) {
      this.writer = writer;
      this.reader = reader;
    }
  }

  /**
   * Renames {@code writer} after {@code reader} as {@code mapping} pairs them.
   *
   * <p>The reader comes back too: the aliases of every type provenance matched, and a matched
   * record's field aliases, are removed, since provenance has already decided those pairings, and
   * any sink branches are added.
   *
   * @throws ProvenanceUnavailableException if one named type would need two definitions inside a
   *     union, or the resolver would read a writer value into a union branch provenance gives a
   *     different id
   */
  static Renamed rename(Schema writer, Schema reader, ProvenanceMapping mapping) {
    // A location the walk cannot find would escape provenance without a trace.
    mapping.requireNames();
    requireReachable(writer, mapping.writerPaths(), mapping::writerNamesOf, mapping.writerId());
    requireReachable(reader, mapping.readerPaths(), mapping::readerNamesOf, mapping.readerId());
    final AvroProvenanceRenamer renamer = new AvroProvenanceRenamer(mapping);
    renamer.locate(reader, Collections.emptyList());
    final Schema renamedWriter = renamer.renameAt(
        writer, Collections.emptyList(), reader, Collections.emptyList(), false);
    final Renamed renamed = new Renamed(
        renamedWriter, renamer.readerCopy(reader, new IdentityHashMap<>()));
    renamer.verify(Resolver.resolve(renamed.writer, renamed.reader),
        Collections.emptyList(), Collections.emptyList(),
        Collections.newSetFromMap(new IdentityHashMap<>()));
    return renamed;
  }

  /**
   * Rejects a reader record that the resolver would find a field missing from with no default to
   * fall back to. Avro fails that on every record; this fails it once, naming the field. A sink is
   * the exception: only the records containing its branch fail, as they come.
   */
  static void requireEveryFieldHasAValue(Renamed renamed) {
    check(
        Resolver.resolve(renamed.writer, renamed.reader),
        Collections.newSetFromMap(new IdentityHashMap<>()));
  }

  private static void check(Resolver.Action action, Set<Resolver.Action> seen) {
    if (!seen.add(action)) {
      return;
    }
    if (action instanceof Resolver.ErrorAction) {
      final Resolver.ErrorAction error = (Resolver.ErrorAction) action;
      if (error.error == Resolver.ErrorAction.ErrorType.MISSING_REQUIRED_FIELD
          && error.reader.getField(SINK_FIELD) == null) {
        throw new SerializationException(
            "Field '"
                + missingField(error.writer, error.reader)
                + "' of reader record "
                + error.reader.getFullName()
                + " has no counterpart in the writer schema and declares no "
                + "default. There is no value to read.");
      }
    } else if (action instanceof Resolver.RecordAdjust) {
      for (Resolver.Action field : ((Resolver.RecordAdjust) action).fieldActions) {
        check(field, seen);
      }
    } else if (action instanceof Resolver.Container) {
      check(((Resolver.Container) action).elementAction, seen);
    } else if (action instanceof Resolver.WriterUnion) {
      for (Resolver.Action branch : ((Resolver.WriterUnion) action).actions) {
        check(branch, seen);
      }
    } else if (action instanceof Resolver.ReaderUnion) {
      check(((Resolver.ReaderUnion) action).actualAction, seen);
    }
  }

  private static String missingField(Schema writer, Schema reader) {
    for (Field field : reader.getFields()) {
      if (writer.getField(field.name()) == null && !field.hasDefaultValue()) {
        return field.name();
      }
    }
    return "?";
  }

  // -------------------------------------------------------------------------------------------
  // The writer
  // -------------------------------------------------------------------------------------------

  /**
   * {@code writer}, spelled {@code writerAt} natively, renamed after {@code reader}, spelled
   * {@code readerAt}; a null reader means there is no counterpart.
   */
  private Schema renameAt(Schema writer, List<String> writerAt, Schema reader,
      List<String> readerAt, boolean inUnion) {
    if (writer.getType() != Type.UNION && ofType(reader, Type.UNION) != null) {
      // As the resolver does, read into the reader's branch: its one branch if it is nullable,
      // else the one of the same name.
      final Schema branch =
          branchNamed(reader, writer.getFullName(), nonNull(reader).size() == 1);
      if (branch == null) {
        return isNamed(writer) ? unmatchedBranch(writer, reader)
            : renameAt(writer, writerAt, null, readerAt, false);
      }
      return renameAt(writer, writerAt, branch, append(readerAt, branch.getFullName()), true);
    }
    switch (writer.getType()) {
      case RECORD:
        return renameRecord(writer, writerAt, reader, readerAt, inUnion);
      case ENUM:
        return register(renameEnum(writer, nameAfter(writer, reader)), inUnion);
      case FIXED:
        return register(renameFixed(writer, nameAfter(writer, reader)), inUnion);
      case ARRAY:
        return withProps(writer, Schema.createArray(renameAt(writer.getElementType(),
            append(writerAt, null), ofType(reader, Type.ARRAY) != null
                ? reader.getElementType() : null, append(readerAt, null), false)));
      case MAP:
        return withProps(writer, Schema.createMap(renameAt(writer.getValueType(),
            append(writerAt, null), ofType(reader, Type.MAP) != null
                ? reader.getValueType() : null, append(readerAt, null), false)));
      case UNION:
        return renameUnion(writer, writerAt, reader, readerAt);
      default:
        return writer;
    }
  }

  private Schema renameRecord(Schema writer, List<String> writerAt, Schema reader,
      List<String> readerAt, boolean inUnion) {
    final Schema target = ofType(reader, Type.RECORD);
    if (target != null) {
      matchedReaderTypes.add(target);
    }
    final List<Field> fields = new ArrayList<>(writer.getFields().size());
    for (Field field : writer.getFields()) {
      final List<String> fieldAt = append(writerAt, field.name());
      final List<Integer> location = mapping.writerPathAt(fieldAt);
      Field counterpart;
      if (location != null) {
        final List<Integer> paired = mapping.readerPathOf(location);
        counterpart = paired == null ? null : pairedChild(target, readerAt, fieldAt, paired);
        if (counterpart == null) {
          fields.add(new Field(unmatchedField(target, field.pos()), discard(field.schema()),
              field.doc()));
          continue;
        }
      } else {
        // Not a location, so matched by name as without provenance.
        counterpart = target != null ? target.getField(field.name()) : null;
      }
      final String name = counterpart != null ? counterpart.name() : field.name();
      fields.add(new Field(name, renameAt(field.schema(), fieldAt,
          counterpart != null ? counterpart.schema() : null, append(readerAt, name), false),
          field.doc()));
    }
    final Schema record = Schema.createRecord(
        target != null ? target.getFullName() : throwawayName(writer),
        writer.getDoc(), null, writer.isError());
    record.setFields(fields);
    writerNames.put(record, fieldNames(writer));
    return register(withProps(writer, record), inUnion);
  }

  private Schema renameUnion(Schema writer, List<String> writerAt, Schema reader,
      List<String> readerAt) {
    final Schema target = ofType(reader, Type.UNION);
    final List<Schema> branches = new ArrayList<>(writer.getTypes().size());
    for (Schema branch : writer.getTypes()) {
      if (branch.getType() == Type.NULL) {
        branches.add(branch);
        continue;
      }
      final List<String> branchAt = append(writerAt, branch.getFullName());
      final List<Integer> location = mapping.writerPathAt(branchAt);
      if (location != null) {
        // A branch of a proper union: renamed after the branch provenance pairs it with.
        final List<Integer> paired = mapping.readerPathOf(location);
        final List<String> pairedAt = paired != null ? mapping.readerNamesOf(paired) : null;
        final Schema counterpart = pairedAt == null ? null : pairedBranch(
            target, readerAt, branchAt, pairedAt);
        branches.add(counterpart != null
            ? renameAt(branch, branchAt, counterpart, pairedAt, true)
            : unmatchedBranch(branch, reader));
      } else if (target == null) {
        // The branch of a nullable union the logical type collapses, against a reader that is no
        // union: it stands where the union does.
        branches.add(renameAt(branch, branchAt, reader, readerAt, false));
      } else {
        final Schema counterpart =
            branchNamed(target, branch.getFullName(), nonNull(target).size() == 1);
        branches.add(counterpart != null
            ? renameAt(branch, branchAt, counterpart,
                append(readerAt, counterpart.getFullName()), true)
            : unmatchedBranch(branch, reader));
      }
    }
    final Schema union = Schema.createUnion(branches);
    writerNames.put(union, branchNames(writer));
    return union;
  }

  /**
   * A branch with no counterpart, renamed so nothing matches it. A record also gets a sink in the
   * reader union, so the resolver cannot match it by structure: a record containing it fails.
   */
  private Schema unmatchedBranch(Schema branch, Schema reader) {
    final Schema unmatched = discard(branch);
    if (unmatched.getType() == Type.RECORD && ofType(reader, Type.UNION) != null) {
      final Schema sink = Schema.createRecord(unmatched.getFullName(), null, null, false);
      sink.setFields(Collections.singletonList(
          new Field(SINK_FIELD, Schema.create(Type.INT), null)));
      sinks.computeIfAbsent(reader, k -> new ArrayList<>()).add(sink);
    }
    return unmatched;
  }

  /**
   * {@code writer} with every named type given a throwaway name: a subtree the resolver skips or
   * matches nowhere, kept apart from every renamed definition.
   */
  private Schema discard(Schema writer) {
    switch (writer.getType()) {
      case RECORD: {
        final List<Field> fields = new ArrayList<>();
        for (Field field : writer.getFields()) {
          fields.add(new Field(UNMATCHED + field.pos(), discard(field.schema()), field.doc()));
        }
        final Schema record = Schema.createRecord(throwawayName(writer), null, null, false);
        record.setFields(fields);
        writerNames.put(record, fieldNames(writer));
        return withProps(writer, record);
      }
      case ENUM:
        return renameEnum(writer, throwawayName(writer));
      case FIXED:
        return renameFixed(writer, throwawayName(writer));
      case ARRAY:
        return withProps(writer, Schema.createArray(discard(writer.getElementType())));
      case MAP:
        return withProps(writer, Schema.createMap(discard(writer.getValueType())));
      case UNION: {
        final List<Schema> branches = new ArrayList<>();
        for (Schema branch : writer.getTypes()) {
          branches.add(discard(branch));
        }
        final Schema union = Schema.createUnion(branches);
        writerNames.put(union, branchNames(writer));
        return union;
      }
      default:
        return writer;
    }
  }

  private static Schema renameEnum(Schema writer, String name) {
    return withProps(writer, Schema.createEnum(
        name, writer.getDoc(), null, writer.getEnumSymbols(), writer.getEnumDefault()));
  }

  private static Schema renameFixed(Schema writer, String name) {
    final Schema fixed = Schema.createFixed(name, writer.getDoc(), null, writer.getFixedSize());
    final LogicalType logicalType = LogicalTypes.fromSchemaIgnoreInvalid(writer);
    if (logicalType != null) {
      logicalType.addToSchema(fixed);
    }
    return withProps(writer, fixed);
  }

  private String nameAfter(Schema writer, Schema reader) {
    if (reader != null && reader.getType() == writer.getType()) {
      matchedReaderTypes.add(reader);
      return reader.getFullName();
    }
    return throwawayName(writer);
  }

  private String throwawayName(Schema writer) {
    String name;
    do {
      name = UNMATCHED + "type_" + throwaway++ + "_" + writer.getName();
    } while (readerTypeNames.contains(name));
    return name;
  }

  /** A name for an unpaired writer field that no field of the reader record has. */
  private static String unmatchedField(Schema target, int position) {
    String name = UNMATCHED + position;
    while (target != null && target.getField(name) != null) {
      name += "_";
    }
    return name;
  }

  private String cloneNamespace(Schema built) {
    String namespace;
    do {
      namespace = UNMATCHED + "clone_" + throwaway++;
    } while (readerTypeNames.contains(namespace + "." + built.getName()));
    return namespace;
  }

  /**
   * {@code built}, or an equal definition already built under its name. A different one under the
   * same name is a clone under a fresh namespace. Outside a union the resolver ignores record
   * names and needs only an enum's or fixed's short name. Inside one, a record branch matching no
   * reader branch by full name is matched by structure, preferring the same short name; the
   * verifier then confirms the branch chosen is the one the clone was renamed after.
   */
  private Schema register(Schema built, boolean inUnion) {
    final Schema existing = byName.get(built.getFullName());
    if (existing == null) {
      byName.put(built.getFullName(), built);
      return built;
    }
    if (existing.equals(built)
        && Objects.equals(writerNames.get(existing), writerNames.get(built))) {
      return existing;
    }
    if (inUnion && built.getType() != Type.RECORD) {
      throw new ProvenanceUnavailableException(
          "Provenance would give the Avro type " + built.getFullName()
              + " two different definitions inside a union, which one schema cannot express.");
    }
    final Schema clone = register(cloneAs(built, cloneNamespace(built)), false);
    if (inUnion) {
      cloneTargets.put(clone.getFullName(), built.getFullName());
    }
    return clone;
  }

  private Schema cloneAs(Schema built, String namespace) {
    switch (built.getType()) {
      case RECORD: {
        final Schema clone = Schema.createRecord(built.getName(), built.getDoc(), namespace,
            built.isError());
        final List<Field> fields = new ArrayList<>();
        for (Field field : built.getFields()) {
          fields.add(new Field(field.name(), field.schema(), field.doc()));
        }
        clone.setFields(fields);
        writerNames.put(clone, writerNames.get(built));
        return withProps(built, clone);
      }
      case ENUM:
        return renameEnum(built, namespace + "." + built.getName());
      default:
        return renameFixed(built, namespace + "." + built.getName());
    }
  }

  // -------------------------------------------------------------------------------------------
  // The reader
  // -------------------------------------------------------------------------------------------

  /**
   * {@code reader} with the aliases of every matched type or type at a location, and the field
   * aliases of every matched record, removed, and the sinks added to their unions. Avro applies a
   * reader's aliases to the writer before resolving, across the whole writer; the writer already
   * bears the reader's names at every location, so such an alias could only rename a placed type
   * a second time — onto another type, into a duplicate, or into a branch with a new id.
   */
  private Schema readerCopy(Schema reader, Map<Schema, Schema> copies) {
    final Schema copied = copies.get(reader);
    if (copied != null) {
      return copied;
    }
    switch (reader.getType()) {
      case RECORD:
        return recordCopy(reader, copies);
      case ARRAY:
        return withProps(reader, Schema.createArray(readerCopy(reader.getElementType(), copies)));
      case MAP:
        return withProps(reader, Schema.createMap(readerCopy(reader.getValueType(), copies)));
      case UNION: {
        final List<Schema> branches = new ArrayList<>(reader.getTypes().size());
        for (Schema branch : reader.getTypes()) {
          branches.add(readerCopy(branch, copies));
        }
        // Appended last, so every branch the application knows keeps its index.
        branches.addAll(sinks.getOrDefault(reader, Collections.emptyList()));
        return Schema.createUnion(branches);
      }
      case ENUM:
      case FIXED: {
        if (!stripsAliases(reader)) {
          return reader;
        }
        final Schema copy = reader.getType() == Type.ENUM
            ? withProps(reader, Schema.createEnum(reader.getName(), reader.getDoc(),
                reader.getNamespace(), reader.getEnumSymbols(), reader.getEnumDefault()))
            : fixedCopy(reader);
        copies.put(reader, copy);
        return copy;
      }
      default:
        return reader;
    }
  }

  /**
   * Collects every named type of {@code reader} held by a location, {@code at} natively.
   */
  private void locate(Schema reader, List<String> at) {
    if (isNamed(reader)) {
      readerTypeNames.add(reader.getFullName());
      if (mapping.readerPathAt(at) != null) {
        locatedReaderTypes.add(reader);
      }
    }
    switch (reader.getType()) {
      case RECORD:
        for (Field field : reader.getFields()) {
          locate(field.schema(), append(at, field.name()));
        }
        break;
      case ARRAY:
        locate(reader.getElementType(), append(at, null));
        break;
      case MAP:
        locate(reader.getValueType(), append(at, null));
        break;
      case UNION: {
        // A nullable union the logical type collapses has no branch step.
        final boolean collapsed = nonNull(reader).size() == 1;
        for (Schema branch : reader.getTypes()) {
          locate(branch, collapsed ? at : append(at, branch.getFullName()));
        }
        break;
      }
      default:
        break;
    }
  }

  private boolean stripsAliases(Schema reader) {
    return matchedReaderTypes.contains(reader) || locatedReaderTypes.contains(reader);
  }

  private static Schema fixedCopy(Schema reader) {
    final Schema fixed = Schema.createFixed(
        reader.getName(), reader.getDoc(), reader.getNamespace(), reader.getFixedSize());
    final LogicalType logicalType = LogicalTypes.fromSchemaIgnoreInvalid(reader);
    if (logicalType != null) {
      logicalType.addToSchema(fixed);
    }
    return withProps(reader, fixed);
  }

  private Schema recordCopy(Schema reader, Map<Schema, Schema> copies) {
    final Schema record = Schema.createRecord(
        reader.getName(), reader.getDoc(), reader.getNamespace(), reader.isError());
    final boolean matched = matchedReaderTypes.contains(reader);
    if (!stripsAliases(reader)) {
      reader.getAliases().forEach(record::addAlias);
    }
    copies.put(reader, record);
    final List<Field> fields = new ArrayList<>(reader.getFields().size());
    for (Field field : reader.getFields()) {
      final Field copy = new Field(field.name(), readerCopy(field.schema(), copies),
          field.doc(), field.defaultVal(), field.order());
      field.getObjectProps().forEach(copy::addProp);
      if (!matched) {
        field.aliases().forEach(copy::addAlias);
      }
      fields.add(copy);
    }
    record.setFields(fields);
    return withProps(reader, record);
  }

  // -------------------------------------------------------------------------------------------
  // The resolver's union choices, checked against provenance
  // -------------------------------------------------------------------------------------------

  /**
   * Walks the resolver's plan and falls back where it would read a writer value into a union
   * branch provenance gives a different id — a branch dropped and re-added, or a promotion into
   * a new branch. A record branch is checked through its fields instead.
   */
  private void verify(Resolver.Action action, List<String> writerAt, List<String> readerAt,
      Set<Resolver.Action> seen) {
    if (!seen.add(action)) {
      return;
    }
    if (action instanceof Resolver.RecordAdjust) {
      final Resolver.RecordAdjust record = (Resolver.RecordAdjust) action;
      final List<String> names = writerNames.get(record.writer);
      for (int i = 0; i < record.fieldActions.length; i++) {
        final Resolver.Action field = record.fieldActions[i];
        if (field instanceof Resolver.Skip) {
          continue;
        }
        final Field written = record.writer.getFields().get(i);
        verify(field, append(writerAt, names != null ? names.get(i) : written.name()),
            append(readerAt, record.reader.getField(written.name()).name()), seen);
      }
    } else if (action instanceof Resolver.Container) {
      verify(((Resolver.Container) action).elementAction, append(writerAt, null),
          append(readerAt, null), seen);
    } else if (action instanceof Resolver.WriterUnion) {
      verifyWriterUnion((Resolver.WriterUnion) action, writerAt, readerAt, seen);
    } else if (action instanceof Resolver.ReaderUnion) {
      final Resolver.ReaderUnion union = (Resolver.ReaderUnion) action;
      final Schema chosen = union.reader.getTypes().get(union.firstMatch);
      final List<String> chosenAt = append(readerAt, chosen.getFullName());
      requireIntended(union.writer, chosen);
      requirePaired(writerAt, chosenAt, chosen);
      verify(union.actualAction, writerAt, chosenAt, seen);
    }
  }

  private void verifyWriterUnion(Resolver.WriterUnion union, List<String> writerAt,
      List<String> readerAt, Set<Resolver.Action> seen) {
    final List<String> names = writerNames.get(union.writer);
    final List<Schema> branches = union.writer.getTypes();
    for (int i = 0; i < branches.size(); i++) {
      if (branches.get(i).getType() == Type.NULL) {
        continue;
      }
      final List<String> branchAt = append(writerAt,
          names != null ? names.get(i) : branches.get(i).getFullName());
      final Resolver.Action branch = union.actions[i];
      if (union.unionEquiv) {
        final Schema chosen = union.reader.getTypes().get(i);
        final List<String> chosenAt = append(readerAt, chosen.getFullName());
        requirePaired(branchAt, chosenAt, chosen);
        verify(branch, branchAt, chosenAt, seen);
      } else if (branch instanceof Resolver.ReaderUnion) {
        final Resolver.ReaderUnion reading = (Resolver.ReaderUnion) branch;
        final Schema chosen = reading.reader.getTypes().get(reading.firstMatch);
        final List<String> chosenAt = append(readerAt, chosen.getFullName());
        requireIntended(branches.get(i), chosen);
        requirePaired(branchAt, chosenAt, chosen);
        verify(reading.actualAction, branchAt, chosenAt, seen);
      } else {
        verify(branch, branchAt, readerAt, seen);
      }
    }
  }

  /** Fails unless a clone inside a union was matched to the reader branch it was named after. */
  private void requireIntended(Schema written, Schema chosen) {
    final String intended = isNamed(written) ? cloneTargets.get(written.getFullName()) : null;
    if (intended != null && !intended.equals(chosen.getFullName())) {
      throw new ProvenanceUnavailableException("The resolver would read the clone of "
          + intended + " into " + chosen.getFullName());
    }
  }

  private void requirePaired(List<String> writerAt, List<String> readerAt, Schema chosen) {
    if (chosen.getType() == Type.RECORD) {
      return;
    }
    final List<Integer> reader = mapping.readerPathAt(readerAt);
    if (reader == null) {
      return;
    }
    final List<Integer> writer = mapping.writerPathAt(writerAt);
    if (writer == null || !reader.equals(mapping.readerPathOf(writer))) {
      throw new ProvenanceUnavailableException("The resolver would read " + writerAt
          + " into " + readerAt + ", which provenance does not pair with it");
    }
  }

  // -------------------------------------------------------------------------------------------
  // Consistency with the response
  // -------------------------------------------------------------------------------------------

  /**
   * Fails unless every location's names lead through {@code schema}: a field by name, a union
   * branch by its type's full name, null for an array element or map value.
   */
  private static void requireReachable(Schema schema, List<List<Integer>> paths,
      Function<List<Integer>, List<String>> namesOf, int schemaId) {
    for (List<Integer> path : paths) {
      final List<String> names = namesOf.apply(path);
      if (!reaches(schema, names)) {
        throw new SerializationException("Location " + names + " of schema id " + schemaId
            + " is not in the schema");
      }
    }
  }

  private static boolean reaches(Schema schema, List<String> names) {
    Schema at = schema;
    for (String step : names) {
      if (at == null) {
        return false;
      }
      if (step == null) {
        at = at.getType() == Type.ARRAY ? at.getElementType()
            : at.getType() == Type.MAP ? at.getValueType() : null;
      } else if (at.getType() == Type.RECORD) {
        at = at.getField(step) != null ? at.getField(step).schema() : null;
      } else if (at.getType() == Type.UNION) {
        at = branchNamed(at, step, false);
      } else {
        return false;
      }
    }
    return at != null;
  }

  /**
   * The reader field provenance pairs a writer field with, which must be a child of the reader
   * record the walk stands on: a pid continues only where its parent does.
   */
  private Field pairedChild(Schema record, List<String> recordAt, List<String> writerAt,
      List<Integer> paired) {
    final List<String> pairedAt = mapping.readerNamesOf(paired);
    final Field field = record != null ? childOf(record, recordAt, pairedAt) : null;
    if (field == null) {
      throw parentsDiffer(writerAt, pairedAt);
    }
    return field;
  }

  private Schema pairedBranch(Schema union, List<String> unionAt, List<String> writerAt,
      List<String> pairedAt) {
    final Schema branch = union != null ? branchOf(union, unionAt, pairedAt) : null;
    if (branch == null) {
      throw parentsDiffer(writerAt, pairedAt);
    }
    return branch;
  }

  private SerializationException parentsDiffer(List<String> writerAt, List<String> readerAt) {
    return new SerializationException("Writer location " + writerAt + " of schema id "
        + mapping.writerId() + " and reader location " + readerAt + " of schema id "
        + mapping.readerId() + " have different parents");
  }

  // -------------------------------------------------------------------------------------------
  // Helpers
  // -------------------------------------------------------------------------------------------

  /**
   * The field of {@code record} spelled {@code names}, if it is a direct child of the record
   * spelled {@code recordAt}; a counterpart under another parent cannot be expressed by renaming.
   */
  private static Field childOf(Schema record, List<String> recordAt, List<String> names) {
    return names != null && isChildOf(names, recordAt)
        ? record.getField(names.get(names.size() - 1)) : null;
  }

  private static Schema branchOf(Schema union, List<String> unionAt, List<String> names) {
    return isChildOf(names, unionAt)
        ? branchNamed(union, names.get(names.size() - 1), false) : null;
  }

  /**
   * The branch of {@code union} with {@code fullName}, or, for a nullable union, its one branch.
   */
  private static Schema branchNamed(Schema union, String fullName, boolean anyIfNullable) {
    final List<Schema> branches = nonNull(union);
    for (Schema branch : branches) {
      if (branch.getFullName().equals(fullName)) {
        return branch;
      }
    }
    return anyIfNullable ? branches.get(0) : null;
  }

  private static List<Schema> nonNull(Schema union) {
    final List<Schema> branches = new ArrayList<>(union.getTypes().size());
    for (Schema branch : union.getTypes()) {
      if (branch.getType() != Type.NULL) {
        branches.add(branch);
      }
    }
    return branches;
  }

  private static boolean isNamed(Schema schema) {
    return schema.getType() == Type.RECORD || schema.getType() == Type.ENUM
        || schema.getType() == Type.FIXED;
  }

  private static Schema ofType(Schema schema, Type type) {
    return schema != null && schema.getType() == type ? schema : null;
  }

  private static List<String> fieldNames(Schema record) {
    final List<String> names = new ArrayList<>();
    for (Field field : record.getFields()) {
      names.add(field.name());
    }
    return names;
  }

  private static List<String> branchNames(Schema union) {
    final List<String> names = new ArrayList<>();
    for (Schema branch : union.getTypes()) {
      names.add(branch.getFullName());
    }
    return names;
  }

  private static Schema withProps(Schema from, Schema to) {
    from.getObjectProps().forEach(to::addProp);
    return to;
  }

  private static boolean isChildOf(List<String> names, List<String> parent) {
    return names.size() == parent.size() + 1 && names.subList(0, parent.size()).equals(parent);
  }

  private static List<String> append(List<String> names, String step) {
    final List<String> extended = new ArrayList<>(names.size() + 1);
    extended.addAll(names);
    extended.add(step);
    return extended;
  }
}
