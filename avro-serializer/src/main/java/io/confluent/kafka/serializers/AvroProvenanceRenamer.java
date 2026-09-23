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
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;

/**
 * Carries provenance into Avro's resolver by renaming, the way {@link Schema#applyAliases} carries
 * aliases: the writer schema is rewritten so that each field bears the name of the reader field
 * provenance pairs it with, and a field with no reader counterpart bears a name nothing matches.
 * The resolver then does everything else — promotion, enum symbols, unions, defaults — exactly as
 * it does without provenance.
 *
 * <p>The binary layout is untouched: only names change, never order or types. Provenance speaks
 * only where it has a mapping; a subtree it has no counterpart for keeps its writer names, and the
 * resolver matches it by name as it always has.
 */
final class AvroProvenanceRenamer {

  private static final String UNMATCHED = "__provenance_unmatched_";

  private final ProvenanceMapping mapping;

  /**

   * Every named type rebuilt, by full name, to catch one name given two definitions.

   */
  private final Map<String, Schema> byName = new HashMap<>();

  /**

   * Reader records a writer record was renamed against; their field aliases must not apply.

   */
  private final Set<Schema> matchedReaderRecords =
      Collections.newSetFromMap(new IdentityHashMap<>());

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
   * <p>The reader comes back too, with the field aliases of every record provenance matched
   * removed: provenance has already decided those pairings, and an alias applied on top of it
   * would move a field the renaming put in place.
   *
   * @throws ProvenanceUnavailableException if one writer named type would need different names at
   *     different locations, which a single Avro schema cannot express
   */
  static Renamed rename(Schema writer, Schema reader, ProvenanceMapping mapping) {
    final AvroProvenanceRenamer renamer = new AvroProvenanceRenamer(mapping);
    final Schema renamedWriter =
        renamer.renameAt(writer, Collections.emptyList(), reader, Collections.emptyList());
    return new Renamed(
        renamedWriter, renamer.withoutMatchedAliases(reader, new IdentityHashMap<>()));
  }

  /**
   * Rejects a reader record that the resolver would find a field missing from with no default to
   * fall back to. Avro fails that on every record; this fails it once, naming the field.
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
      if (error.error == Resolver.ErrorAction.ErrorType.MISSING_REQUIRED_FIELD) {
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
   * {@code writer} at {@code writerPath}, renamed after {@code reader} at {@code readerPath}; a
   * null reader means provenance has no counterpart here, and the writer is kept as it is.
   */
  private Schema renameAt(
      Schema writer, List<Integer> writerPath, Schema reader, List<Integer> readerPath) {
    if (reader == null) {
      return keep(writer);
    }
    switch (writer.getType()) {
      case RECORD:
        return reader.getType() == Type.RECORD
            ? named(() -> renameRecord(writer, writerPath, reader, readerPath))
            : keep(writer);
      case ENUM:
        return reader.getType() == Type.ENUM
            ? named(
                () ->
                    Schema.createEnum(
                        reader.getFullName(),
                        writer.getDoc(),
                        null,
                        writer.getEnumSymbols(),
                        writer.getEnumDefault()))
            : keep(writer);
      case FIXED:
        return reader.getType() == Type.FIXED
            ? named(() -> renameFixed(writer, reader))
            : keep(writer);
      case ARRAY:
        return reader.getType() == Type.ARRAY
            ? withProps(
                writer,
                Schema.createArray(
                    renameAt(
                        writer.getElementType(),
                        append(writerPath, 0),
                        reader.getElementType(),
                        append(readerPath, 0))))
            : keep(writer);
      case MAP:
        return reader.getType() == Type.MAP
            ? withProps(
                writer,
                Schema.createMap(
                    renameAt(
                        writer.getValueType(),
                        append(writerPath, 1),
                        reader.getValueType(),
                        append(readerPath, 1))))
            : keep(writer);
      case UNION:
        return renameUnion(writer, writerPath, reader, readerPath);
      default:
        return writer;
    }
  }

  private Schema renameRecord(
      Schema writer, List<Integer> writerPath, Schema reader, List<Integer> readerPath) {
    matchedReaderRecords.add(reader);
    final List<Field> fields = new ArrayList<>(writer.getFields().size());
    for (Field field : writer.getFields()) {
      final List<Integer> fieldPath = append(writerPath, field.pos());
      final Field counterpart = readerFieldFor(fieldPath, reader, readerPath);
      fields.add(
          counterpart == null
              ? new Field(UNMATCHED + field.pos(), keep(field.schema()), field.doc())
              : new Field(
                  counterpart.name(),
                  renameAt(
                      field.schema(),
                      fieldPath,
                      counterpart.schema(),
                      append(readerPath, counterpart.pos())),
                  field.doc()));
    }
    final Schema record =
        Schema.createRecord(reader.getFullName(), writer.getDoc(), null, writer.isError());
    record.setFields(fields);
    return withProps(writer, record);
  }

  /**

   * The reader field provenance pairs the writer member at {@code writerPath} with, if any.

   */
  private Field readerFieldFor(
      List<Integer> writerPath, Schema reader, List<Integer> readerPath) {
    final List<Integer> mapped = mapping.readerPathOf(writerPath);
    if (!isChildOf(mapped, readerPath)) {
      return null;
    }
    final int index = mapped.get(readerPath.size());
    return index < reader.getFields().size() ? reader.getFields().get(index) : null;
  }

  private Schema renameUnion(
      Schema writer, List<Integer> writerPath, Schema reader, List<Integer> readerPath) {
    final List<Schema> branches = writer.getTypes();
    final List<Schema> renamedBranches = new ArrayList<>(branches.size());
    if (collapses(writer)) {
      // A nullable wrap takes no path step: its one real branch stands where the union does.
      final Schema counterpart = collapses(reader) ? nonNull(reader).get(0) : reader;
      for (Schema branch : branches) {
        renamedBranches.add(
            branch.getType() == Type.NULL
                ? branch
                : renameAt(branch, writerPath, counterpart, readerPath));
      }
      return Schema.createUnion(renamedBranches);
    }
    final List<Schema> readerBranches =
        reader.getType() == Type.UNION && !collapses(reader) ? nonNull(reader) : null;
    int index = 0;
    for (Schema branch : branches) {
      if (branch.getType() == Type.NULL) {
        renamedBranches.add(branch);
        continue;
      }
      final List<Integer> branchPath = append(writerPath, index++);
      final List<Integer> mapped = mapping.readerPathOf(branchPath);
      final boolean paired = readerBranches != null
          && isChildOf(mapped, readerPath)
          && mapped.get(readerPath.size()) < readerBranches.size();
      renamedBranches.add(
          paired
              ? renameAt(
                  branch,
                  branchPath,
                  readerBranches.get(mapped.get(readerPath.size())),
                  mapped)
              : keep(branch));
    }
    return Schema.createUnion(renamedBranches);
  }

  private Schema renameFixed(Schema writer, Schema reader) {
    final Schema fixed =
        Schema.createFixed(
            reader.getFullName(), writer.getDoc(), null, writer.getFixedSize());
    final LogicalType logicalType = LogicalTypes.fromSchemaIgnoreInvalid(writer);
    if (logicalType != null) {
      logicalType.addToSchema(fixed);
    }
    return withProps(writer, fixed);
  }

  /**
   * A named type rebuilt at every location rather than reused, so a location that needs a
   * different rewrite of the same type is caught by {@link #register} instead of hidden.
   */
  private Schema named(Supplier<Schema> build) {
    return register(build.get());
  }

  /**
   * A writer subtree provenance has no counterpart for, as written. It is either skipped or
   * matched by name as the resolver always has, so it is not held to the one-definition check.
   */
  private static Schema keep(Schema writer) {
    return writer;
  }

  private Schema register(Schema built) {
    final Schema existing = byName.get(built.getFullName());
    if (existing == null) {
      byName.put(built.getFullName(), built);
      return built;
    }
    if (existing != built && !existing.equals(built)) {
      throw new ProvenanceUnavailableException(
          "Provenance would give the Avro type "
              + built.getFullName()
              + " two different definitions at different locations, which one "
              + "schema cannot express.");
    }
    return existing;
  }

  // -------------------------------------------------------------------------------------------
  // The reader
  // -------------------------------------------------------------------------------------------

  /**

   * {@code reader} with the field aliases of every matched record removed.

   */
  private Schema withoutMatchedAliases(Schema reader, Map<Schema, Schema> copies) {
    final Schema copied = copies.get(reader);
    if (copied != null) {
      return copied;
    }
    switch (reader.getType()) {
      case RECORD:
        return recordWithoutMatchedAliases(reader, copies);
      case ARRAY:
        return withProps(
            reader,
            Schema.createArray(withoutMatchedAliases(reader.getElementType(), copies)));
      case MAP:
        return withProps(
            reader,
            Schema.createMap(withoutMatchedAliases(reader.getValueType(), copies)));
      case UNION:
        final List<Schema> branches = new ArrayList<>(reader.getTypes().size());
        for (Schema branch : reader.getTypes()) {
          branches.add(withoutMatchedAliases(branch, copies));
        }
        return Schema.createUnion(branches);
      default:
        // Enums, fixed and primitives carry no field aliases and are shared as they are.
        return reader;
    }
  }

  private Schema recordWithoutMatchedAliases(Schema reader, Map<Schema, Schema> copies) {
    final Schema record = Schema.createRecord(
        reader.getName(), reader.getDoc(), reader.getNamespace(), reader.isError());
    for (String alias : reader.getAliases()) {
      record.addAlias(alias);
    }
    copies.put(reader, record);
    final boolean matched = matchedReaderRecords.contains(reader);
    final List<Field> fields = new ArrayList<>(reader.getFields().size());
    for (Field field : reader.getFields()) {
      final Field copy = new Field(field.name(), withoutMatchedAliases(field.schema(), copies),
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
  // Helpers
  // -------------------------------------------------------------------------------------------

  /**

   * A union with one real branch, optionally beside null: Flink collapses it, with no step.

   */
  private static boolean collapses(Schema schema) {
    return schema.getType() == Type.UNION && nonNull(schema).size() == 1;
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

  private static Schema withProps(Schema from, Schema to) {
    from.getObjectProps().forEach(to::addProp);
    return to;
  }

  /**
   * True when {@code path} names a direct member of the container at {@code parent}.
   */
  private static boolean isChildOf(List<Integer> path, List<Integer> parent) {
    return path != null && path.size() == parent.size() + 1
        && path.subList(0, parent.size()).equals(parent);
  }

  private static List<Integer> append(List<Integer> path, int step) {
    final List<Integer> extended = new ArrayList<>(path.size() + 1);
    extended.addAll(path);
    extended.add(step);
    return extended;
  }
}
