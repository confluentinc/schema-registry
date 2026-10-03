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

package io.confluent.kafka.serializers.protobuf;

import com.google.protobuf.DescriptorProtos.DescriptorProto;
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto;
import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.DescriptorValidationException;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.Message;
import com.google.protobuf.UnknownFieldSet;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import io.confluent.kafka.serializers.provenance.ProvenanceMapping;
import io.confluent.kafka.serializers.provenance.ProvenanceUnavailableException;
import io.confluent.protobuf.MetaProto;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.kafka.common.errors.SerializationException;

/**
 * Carries provenance into protobuf's parser by renumbering the reader's descriptor.
 *
 * <p>The parser pairs a wire field with a descriptor field by number and decodes it as the
 * descriptor's type. A field's provenance identity is its number too, so the two agree except where
 * a number was dropped and later reused: the reader's field is then a new entity, and number
 * matching would hand it the old field's data. Each such reader field moves to a number no writer
 * uses, so the writer's field lands in the unknown fields and the reader's reads as unset. Names,
 * types, options, metadata and rules are untouched, so domain rules see the reader as it is.
 *
 * <p>Fields are found by the names the provenance response carries, which the converter recorded
 * as the descriptor's own route to each location.
 */
final class ProtoProvenanceRenumberer {

  // The largest field number protobuf allows; fresh numbers are taken downwards from here.
  private static final int MAX_FIELD_NUMBER = 536_870_911;
  // Reserved for the protobuf implementation; protoc rejects a field using one.
  private static final int FIRST_RESERVED_NUMBER = 19_000;
  private static final int LAST_RESERVED_NUMBER = 19_999;

  private final FileDescriptor file;
  private final int readerId;
  // For each message, by full name: which of its field numbers must move.
  private final Map<String, Map<Integer, Boolean>> moves = new HashMap<>();
  // The writer's messages, by full name, its imports' included: for their ranges.
  private final Map<String, DescriptorProto> writerMessages = new HashMap<>();
  // Every field number of every writer message: a fresh number takes none, since a location may
  // hold another message's data than its name says (a retype, or one inside a map value).
  private final Set<Integer> writerNumbers = new HashSet<>();
  // Every extension and reserved range of every writer message, for a renamed or retyped one.
  private final DescriptorProto.Builder writerRanges = DescriptorProto.newBuilder();
  // Reader messages a continuing field now types, where the writer's holds another message: the
  // data there is that message's, so their fresh numbers avoid every writer range too.
  private final Set<String> retypedInto = new HashSet<>();

  private ProtoProvenanceRenumberer(FileDescriptor file, int readerId) {
    this.file = file;
    this.readerId = readerId;
  }

  /**
   * {@code reader} with every field provenance gives no writer counterpart moved to an unused
   * number; {@code reader} itself when there is nothing to move. Fresh numbers also avoid those
   * {@code writer}, where given, writes under: data there would be parsed into the moved field,
   * and fail the parse where it is a message.
   *
   * @throws ProvenanceUnavailableException if a message used at several locations would need
   *     different numberings, a field needing a new number belongs to an imported file, or the
   *     record's message is a nested one no location reaches
   * @throws SerializationException if a location's names are missing or not in the reader
   */
  static Renumbered renumber(ProtobufSchema reader, ProtobufSchema writer,
      ProvenanceMapping mapping, boolean includeMultipleMessages) {
    // A location the walk cannot find would escape provenance without a trace.
    mapping.requireNames();
    Descriptor root = reader.toDescriptor();
    ProtoProvenanceRenumberer renumberer =
        new ProtoProvenanceRenumberer(root.getFile(), mapping.readerId());
    if (writer != null) {
      renumberer.collectWriter(writer.toDescriptor().getFile(), new HashSet<>());
    }
    Set<List<Integer>> moving = new HashSet<>();
    boolean reached = false;
    boolean nested = includeMultipleMessages && root.getContainingType() != null;
    // Whether the record's message is reached only under restarted uses, and which of its fields
    // continue a field of the writer's own message.
    boolean reachedMoving = false;
    Set<Integer> ofWriterRecord = new HashSet<>();
    for (List<Integer> path : mapping.readerPaths()) {
      List<String> names = mapping.readerNamesOf(path);
      if (includeMultipleMessages && !nested && !names.get(0).equals(root.getFullName())) {
        // Another top-level message: the record never parses it, so its locations decide nothing.
        continue;
      }
      if (renumberer.underMovingField(root, path, moving, mapping, includeMultipleMessages)) {
        // Nothing under a field that moves is ever read, so nothing under it needs a number.
        if (nested && !reachedMoving) {
          FieldDescriptor at = renumberer.fieldAt(root, names, includeMultipleMessages);
          reachedMoving = at != null && isOf(at, root);
        }
        continue;
      }
      boolean move = mapping.writerPathOf(path) == null;
      if (move) {
        moving.add(path);
      }
      FieldDescriptor field = renumberer.fieldAt(root, names, includeMultipleMessages);
      if (field != null && isOf(field, root)) {
        reached = true;
        if (nested && !move && writer != null) {
          FieldDescriptor was =
              writerFieldAt(writer, mapping.writerNamesOf(mapping.writerPathOf(path)),
                  includeMultipleMessages);
          if (was != null && isOf(was, root)) {
            ofWriterRecord.add(field.getNumber());
          }
        }
      }
      if (!move && writer != null && messageOf(field) != null) {
        Descriptor held = messageOf(
            writerFieldAt(writer, mapping.writerNamesOf(mapping.writerPathOf(path)),
                includeMultipleMessages));
        if (held != null && !held.getFullName().equals(messageOf(field).getFullName())) {
          renumberer.retypedInto.add(messageOf(field).getFullName());
        }
      }
      if (isOneof(path, names, field, mapping)) {
        // A oneof: no step of its own, so no field to number; its members are visited as fields.
        continue;
      }
      if (field != null && (!nested || renumberer.passesThrough(root, names))) {
        // Read as a nested record, only what lies under the record's message is ever parsed.
        renumberer.decide(field.getContainingType(), field, move);
      }
    }
    if (nested && !reached && reachedMoving) {
      // Every location reaching the record's message is under a restarted use: each of its
      // fields is new there, so none takes the writer's value.
      for (FieldDescriptor field : root.getFields()) {
        renumberer.decide(root, field, true);
      }
      reached = true;
    }
    if (nested && reached && writer != null) {
      // Read directly, the record's fields continue only fields of the writer's own message: one
      // continuing a field of another message at every use takes no value here.
      Map<Integer, Boolean> decided = renumberer.moves.get(root.getFullName());
      if (decided != null) {
        decided.replaceAll((number, move) -> move || !ofWriterRecord.contains(number));
      }
    }
    if (nested && !root.getFields().isEmpty() && !reached) {
      // Locations start at the file's top-level messages: a nested message reaches them only
      // through a field using it, and read directly it would escape provenance silently.
      throw new ProvenanceUnavailableException("The record's message " + root.getFullName()
          + " is nested, and no location of the subject's provenance reaches it");
    }
    FileDescriptor renumbered = renumberer.build();
    if (renumbered == root.getFile()) {
      return new Renumbered(reader, Collections.emptyMap());
    }
    Descriptor renamedRoot = messageNamed(renumbered, root.getFullName());
    ProtobufSchema schema = new ProtobufSchema(renamedRoot != null ? renamedRoot : root,
        reader.references());
    return new Renumbered((ProtobufSchema) schema.copy(reader.metadata(), reader.ruleSet()),
        renumberer.movedNumbers());
  }

  /**
   * Whether an ancestor of {@code path} is a moving field. A oneof moving is no field: its names
   * are its parent's, and its members still need numbers of their own. Nor is a top-level message:
   * restarted, it moves none of its members by itself.
   */
  private boolean underMovingField(Descriptor root, List<Integer> path,
      Set<List<Integer>> moving, ProvenanceMapping mapping, boolean multi) {
    for (int k = 1; k < path.size(); k++) {
      List<Integer> ancestor = path.subList(0, k);
      if (moving.contains(ancestor)) {
        List<String> names = mapping.readerNamesOf(ancestor);
        // A top-level message is no field: restarted, it moves none of its members by itself,
        // so each is decided on its own.
        FieldDescriptor field =
            names == null || names.isEmpty() ? null : fieldAt(root, names, multi);
        if (field != null && !isOneof(ancestor, names, field, mapping)) {
          return true;
        }
      }
    }
    return false;
  }

  /**
   * Whether the location at {@code path}, whose names reach {@code field}, is a oneof: its names
   * are its parent's, or it is a union whose names reach no field wrapping a union. A oneof has
   * no field of its own, so its names end at whatever field holds its message — a map value, a
   * list, a Flink wrapper's payload — however the collections around it nest.
   */
  private static boolean isOneof(List<Integer> path, List<String> names, FieldDescriptor field,
      ProvenanceMapping mapping) {
    if (names.equals(mapping.enclosingReaderNamesOf(path))) {
      return true;
    }
    return "UNION".equals(mapping.readerKindOf(path)) && (field == null || !wrapsUnion(field));
  }

  // A singular flink.wrapped field whose wrapper's payload is a oneof named value: the one field
  // a union location has, as the converter reads it.
  private static boolean wrapsUnion(FieldDescriptor field) {
    if (field.isRepeated() || field.getJavaType() != FieldDescriptor.JavaType.MESSAGE
        || !field.getOptions().hasExtension(MetaProto.fieldMeta)
        || !Boolean.parseBoolean(field.getOptions().getExtension(MetaProto.fieldMeta)
            .getParamsOrDefault("flink.wrapped", null))) {
      return false;
    }
    Descriptor wrapper = field.getMessageType();
    return wrapper.findFieldByName("value") == null && wrapper.getRealOneofs().stream()
        .anyMatch(o -> "value".equals(o.getName()));
  }

  /**
   * A renumbered reader, and the numbers moved in each message: a writer's data under one of them
   * now sits in that message's unknown fields, and is dropped from there.
   */
  static final class Renumbered {
    final ProtobufSchema schema;
    private final Map<String, Set<Integer>> moved;

    private Renumbered(ProtobufSchema schema, Map<String, Set<Integer>> moved) {
      this.schema = schema;
      this.moved = moved;
    }

    boolean movedAny() {
      return !moved.isEmpty();
    }

    /**
     * {@code message} without the unknown fields a moved number left behind, at any depth, so the
     * old value cannot come back if the message is written out again.
     */
    Message dropMoved(Message message) {
      Message.Builder builder = message.toBuilder();
      Set<Integer> numbers = moved.get(message.getDescriptorForType().getFullName());
      if (numbers != null) {
        UnknownFieldSet.Builder unknown = UnknownFieldSet.newBuilder(message.getUnknownFields());
        numbers.forEach(unknown::clearField);
        builder.setUnknownFields(unknown.build());
      }
      for (FieldDescriptor field : message.getDescriptorForType().getFields()) {
        if (field.getJavaType() != FieldDescriptor.JavaType.MESSAGE) {
          continue;
        }
        if (field.isRepeated()) {
          for (int i = 0; i < message.getRepeatedFieldCount(field); i++) {
            builder.setRepeatedField(field, i,
                dropMoved((Message) message.getRepeatedField(field, i)));
          }
        } else if (message.hasField(field)) {
          builder.setField(field, dropMoved((Message) message.getField(field)));
        }
      }
      return builder.build();
    }
  }

  private Map<String, Set<Integer>> movedNumbers() {
    Map<String, Set<Integer>> moved = new HashMap<>();
    moves.forEach((message, decided) -> decided.forEach((number, move) -> {
      if (move) {
        moved.computeIfAbsent(message, m -> new HashSet<>()).add(number);
      }
    }));
    return moved;
  }

  // Whether field belongs to the message named as message is.
  private static boolean isOf(FieldDescriptor field, Descriptor message) {
    return field.getContainingType().getFullName().equals(message.getFullName());
  }

  // The writer's field names end at, from its file's top-level messages, or from its record's
  // message in single-message mode; null where none is.
  private static FieldDescriptor writerFieldAt(ProtobufSchema writer, List<String> names,
      boolean multi) {
    if (names == null || names.isEmpty()) {
      return null;
    }
    Descriptor message = multi ? null : writer.toDescriptor();
    if (multi) {
      for (Descriptor top : writer.toDescriptor().getFile().getMessageTypes()) {
        if (top.getFullName().equals(names.get(0))) {
          message = top;
        }
      }
    }
    FieldDescriptor field = null;
    for (int i = multi ? 1 : 0; i < names.size() && message != null; i++) {
      field = names.get(i) != null ? message.findFieldByName(names.get(i)) : null;
      if (field == null) {
        return null;
      }
      message = messageOf(field);
    }
    return field;
  }

  // Whether names, from a top-level message, take a field of the record's message on the way.
  private boolean passesThrough(Descriptor root, List<String> names) {
    Descriptor message = topLevel(names.get(0));
    for (int i = 1; i < names.size() && message != null; i++) {
      FieldDescriptor field = names.get(i) != null ? message.findFieldByName(names.get(i)) : null;
      if (field == null) {
        return false;
      }
      if (isOf(field, root)) {
        return true;
      }
      message = messageOf(field);
    }
    return false;
  }

  /**
   * The field {@code names} end at; null for a message itself.
   */
  private FieldDescriptor fieldAt(Descriptor root, List<String> names, boolean multi) {
    int i = 0;
    Descriptor message = root;
    if (multi) {
      message = topLevel(names.get(0));
      i = 1;
    }
    // Every step is a field of the message we stand on: repeated elements and oneofs are no
    // steps, and a map entry's key and value are its fields.
    FieldDescriptor field = null;
    for (; i < names.size(); i++) {
      if (message == null || names.get(i) == null) {
        throw notInSchema(names);
      }
      field = message.findFieldByName(names.get(i));
      if (field == null) {
        throw notInSchema(names);
      }
      message = messageOf(field);
    }
    return field;
  }

  private void decide(Descriptor message, FieldDescriptor field, boolean move) {
    Boolean earlier = moves.computeIfAbsent(message.getFullName(), m -> new HashMap<>())
        .putIfAbsent(field.getNumber(), move);
    if (earlier != null && earlier != move) {
      throw new ProvenanceUnavailableException("Message " + message.getFullName()
          + " is used at several locations that provenance maps differently");
    }
    if (move && message.getFile() != file) {
      throw new ProvenanceUnavailableException("Field " + field.getFullName()
          + " needs a new number, but its message is defined in an imported file");
    }
  }

  /**
   * The message of {@code file} with {@code fullName}, nested or not; null if none.
   */
  private static Descriptor messageNamed(FileDescriptor file, String fullName) {
    for (Descriptor message : file.getMessageTypes()) {
      Descriptor found = messageNamed(message, fullName);
      if (found != null) {
        return found;
      }
    }
    return null;
  }

  private static Descriptor messageNamed(Descriptor message, String fullName) {
    if (message.getFullName().equals(fullName)) {
      return message;
    }
    for (Descriptor nested : message.getNestedTypes()) {
      Descriptor found = messageNamed(nested, fullName);
      if (found != null) {
        return found;
      }
    }
    return null;
  }

  private Descriptor topLevel(String fullName) {
    for (Descriptor message : file.getMessageTypes()) {
      if (message.getFullName().equals(fullName)) {
        return message;
      }
    }
    throw notInSchema(Collections.singletonList(fullName));
  }

  private SerializationException notInSchema(List<String> names) {
    return new SerializationException(
        "Location " + names + " of schema id " + readerId + " is not in the schema");
  }

  private static Descriptor messageOf(FieldDescriptor field) {
    return field != null && field.getJavaType() == FieldDescriptor.JavaType.MESSAGE
        ? field.getMessageType()
        : null;
  }

  private FileDescriptor build() {
    if (moves.values().stream().noneMatch(m -> m.containsValue(true))) {
      return file;
    }
    FileDescriptorProto.Builder proto = file.toProto().toBuilder();
    String prefix = file.getPackage().isEmpty() ? "" : file.getPackage() + ".";
    for (DescriptorProto.Builder message : proto.getMessageTypeBuilderList()) {
      renumberMessage(message, prefix + message.getName());
    }
    try {
      return FileDescriptor.buildFrom(
          proto.build(), file.getDependencies().toArray(new FileDescriptor[0]));
    } catch (DescriptorValidationException e) {
      throw new SerializationException(
          "Could not renumber " + file.getName() + " after provenance: " + e.getMessage(), e);
    }
  }

  // The writer's file and its imports: a reader message may hold data of a message imported
  // there.
  private void collectWriter(FileDescriptor writerFile, Set<String> seen) {
    if (!seen.add(writerFile.getName())) {
      return;
    }
    writerFile.getMessageTypes().forEach(this::collectWriter);
    writerFile.getDependencies().forEach(dependency -> collectWriter(dependency, seen));
  }

  private void collectWriter(Descriptor message) {
    DescriptorProto proto = message.toProto();
    writerMessages.put(message.getFullName(), proto);
    message.getFields().forEach(field -> writerNumbers.add(field.getNumber()));
    writerRanges.addAllExtensionRange(proto.getExtensionRangeList());
    writerRanges.addAllReservedRange(proto.getReservedRangeList());
    for (Descriptor nested : message.getNestedTypes()) {
      collectWriter(nested);
    }
  }

  private void renumberMessage(DescriptorProto.Builder message, String fullName) {
    Map<Integer, Boolean> decided = moves.get(fullName);
    if (decided != null && decided.containsValue(true)) {
      Set<Integer> taken = new HashSet<>();
      for (FieldDescriptorProto field : message.getFieldList()) {
        taken.add(field.getNumber());
      }
      DescriptorProto written =
          retypedInto.contains(fullName) ? null : writerMessages.get(fullName);
      // Whatever message's data a location now holds, it is under some writer number.
      taken.addAll(writerNumbers);
      if (written == null) {
        // A message renamed since the writer, or one a field was retyped to: its data may be under
        // any writer message's extension ranges.
        written = writerRanges.build();
      }
      int next = MAX_FIELD_NUMBER;
      for (FieldDescriptorProto.Builder field : message.getFieldBuilderList()) {
        if (Boolean.TRUE.equals(decided.get(field.getNumber()))) {
          next = freeNumber(message, written, taken, next);
          taken.add(next);
          field.setNumber(next--);
        }
      }
      // A reserved range may cover a fresh number; it has no bearing on parsing.
      message.clearReservedRange();
    }
    for (DescriptorProto.Builder nested : message.getNestedTypeBuilderList()) {
      renumberMessage(nested, fullName + "." + nested.getName());
    }
  }

  /**
   * The highest number at or below {@code from} that no field takes, no extension range covers —
   * nor the writer's extension ranges, which its data may still fill, nor, while any number is
   * left otherwise, its reserved ranges — and the implementation does not reserve. {@code written}
   * is the writer's message of the same name, or every writer message's ranges for one renamed
   * or retyped to since. A range is jumped over whole: one running to the maximum would otherwise
   * be stepped through number by number.
   */
  private static int freeNumber(DescriptorProto.Builder message, DescriptorProto written,
      Set<Integer> taken, int from) {
    int number = freeNumber(message, written, taken, from, true);
    // A writer reserving all that is left writes nothing there: its reserved ranges are a
    // preference, not a limit.
    return number > 0 ? number : freeNumber(message, written, taken, from, false);
  }

  private static int freeNumber(DescriptorProto.Builder message, DescriptorProto written,
      Set<Integer> taken, int from, boolean avoidReserved) {
    int number = from;
    while (number > 0) {
      int below = rangeStartHolding(message.getExtensionRangeList(), written, number,
          avoidReserved);
      if (below >= 0) {
        number = below - 1;
      } else if (number >= FIRST_RESERVED_NUMBER && number <= LAST_RESERVED_NUMBER) {
        number = FIRST_RESERVED_NUMBER - 1;
      } else if (taken.contains(number)) {
        number--;
      } else {
        return number;
      }
    }
    if (avoidReserved) {
      return 0;
    }
    throw new ProvenanceUnavailableException(
        "Message " + message.getName() + " has no field number left to move a field to");
  }

  /**
   * The start of an extension range of the reader's, or an extension or reserved range of the
   * writer's, holding {@code number}; -1 if none does.
   */
  private static int rangeStartHolding(List<DescriptorProto.ExtensionRange> extensions,
      DescriptorProto written, int number, boolean avoidReserved) {
    for (DescriptorProto.ExtensionRange range : extensions) {
      if (number >= range.getStart() && number < range.getEnd()) {
        return range.getStart();
      }
    }
    if (written != null) {
      for (DescriptorProto.ExtensionRange range : written.getExtensionRangeList()) {
        if (number >= range.getStart() && number < range.getEnd()) {
          return range.getStart();
        }
      }
      for (DescriptorProto.ReservedRange range : written.getReservedRangeList()) {
        if (avoidReserved && number >= range.getStart() && number < range.getEnd()) {
          return range.getStart();
        }
      }
    }
    return -1;
  }
}
