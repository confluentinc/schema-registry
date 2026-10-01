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
  // The writer's messages, by full name: a fresh number must be none the writer writes under.
  private final Map<String, DescriptorProto> writerMessages = new HashMap<>();
  // Every field number of every writer message, for a reader message the writer names otherwise.
  private final Set<Integer> writerNumbers = new HashSet<>();
  // Every extension and reserved range of every writer message, for the same.
  private final DescriptorProto.Builder writerRanges = DescriptorProto.newBuilder();

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
      for (Descriptor message : writer.toDescriptor().getFile().getMessageTypes()) {
        renumberer.collectWriter(message);
      }
    }
    Set<List<Integer>> moving = new HashSet<>();
    boolean reached = false;
    for (List<Integer> path : mapping.readerPaths()) {
      List<String> names = mapping.readerNamesOf(path);
      if (renumberer.underMovingField(root, path, moving, mapping, includeMultipleMessages)) {
        // Nothing under a field that moves is ever read, so nothing under it needs a number.
        continue;
      }
      boolean move = mapping.writerPathOf(path) == null;
      if (move) {
        moving.add(path);
      }
      FieldDescriptor field = renumberer.fieldAt(root, names, includeMultipleMessages);
      if (field != null && field.getContainingType().getFullName().equals(root.getFullName())) {
        reached = true;
      }
      if (isOneof(path, names, field, mapping)) {
        // A oneof: no step of its own, so no field to number; its members are visited as fields.
        continue;
      }
      if (field != null) {
        renumberer.decide(field.getContainingType(), field, move);
      }
    }
    boolean nested = includeMultipleMessages && root.getContainingType() != null;
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
   * Whether the location at {@code path}, ending at {@code field}, is a oneof: its names are its
   * parent's, or end at a map entry's {@code value}, which is no location of its own.
   */
  private static boolean isOneof(List<Integer> path, List<String> names, FieldDescriptor field,
      ProvenanceMapping mapping) {
    return names.equals(mapping.enclosingReaderNamesOf(path))
        || field != null && field.getContainingType().getOptions().getMapEntry()
            && field.getNumber() == 2;
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
      DescriptorProto written = writerMessages.get(fullName);
      if (written != null) {
        written.getFieldList().forEach(field -> taken.add(field.getNumber()));
      } else {
        // A message renamed since the writer: its data may be under any writer message's numbers,
        // or in any of their extension ranges.
        taken.addAll(writerNumbers);
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
   * since. A range is jumped over whole: one running to the maximum would otherwise be stepped
   * through number by number.
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
