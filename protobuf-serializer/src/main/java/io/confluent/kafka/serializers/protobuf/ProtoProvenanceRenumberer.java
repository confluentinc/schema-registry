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
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import io.confluent.kafka.serializers.provenance.ProvenanceMapping;
import io.confluent.kafka.serializers.provenance.ProvenanceUnavailableException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

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

  private final FileDescriptor file;
  // For each message, by full name: which of its field numbers must move.
  private final Map<String, Map<Integer, Boolean>> moves = new HashMap<>();

  private ProtoProvenanceRenumberer(FileDescriptor file) {
    this.file = file;
  }

  /**
   * {@code reader} with every field provenance gives no writer counterpart moved to an unused
   * number; {@code reader} itself when there is nothing to move.
   *
   * @throws ProvenanceUnavailableException if a message used at several locations would need
   *     different numberings, a field needing a new number belongs to an imported file, or a path
   *     cannot be located in the reader
   */
  static ProtobufSchema renumber(ProtobufSchema reader, ProvenanceMapping mapping,
      boolean includeMultipleMessages) {
    Descriptor root = reader.toDescriptor();
    ProtoProvenanceRenumberer renumberer = new ProtoProvenanceRenumberer(root.getFile());
    for (List<Integer> path : mapping.readerPaths()) {
      List<String> names = mapping.readerNamesOf(path);
      if (names == null) {
        throw new ProvenanceUnavailableException("The provenance response carries no names");
      }
      renumberer.visit(root, names, includeMultipleMessages, mapping.writerPathOf(path) == null);
    }
    FileDescriptor renumbered = renumberer.build();
    if (renumbered == root.getFile()) {
      return reader;
    }
    Descriptor renamedRoot = renumbered.findMessageTypeByName(root.getName());
    ProtobufSchema schema = new ProtobufSchema(renamedRoot != null ? renamedRoot : root,
        reader.references());
    return (ProtobufSchema) schema.copy(reader.metadata(), reader.ruleSet());
  }

  private void visit(Descriptor root, List<String> names, boolean multi, boolean move) {
    int i = 0;
    Descriptor message = root;
    if (multi) {
      message = topLevel(names.get(0));
      i = 1;
    }
    // Every step is a field of the message we stand on: repeated elements and oneofs are no
    // steps, and a map entry's key and value are its fields.
    Descriptor owner = null;
    FieldDescriptor field = null;
    for (; i < names.size(); i++) {
      if (message == null || names.get(i) == null) {
        throw new ProvenanceUnavailableException("Cannot locate " + names + " in the reader");
      }
      field = message.findFieldByName(names.get(i));
      if (field == null) {
        throw new ProvenanceUnavailableException("Cannot locate " + names + " in the reader");
      }
      owner = message;
      message = messageOf(field);
    }
    if (owner != null) {
      decide(owner, field, move);
    }
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

  private Descriptor topLevel(String fullName) {
    for (Descriptor message : file.getMessageTypes()) {
      if (message.getFullName().equals(fullName)) {
        return message;
      }
    }
    throw new ProvenanceUnavailableException("The reader declares no message " + fullName);
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
      throw new ProvenanceUnavailableException(
          "Could not renumber " + file.getName() + " after provenance: " + e.getMessage(), e);
    }
  }

  private void renumberMessage(DescriptorProto.Builder message, String fullName) {
    Map<Integer, Boolean> decided = moves.get(fullName);
    if (decided != null && decided.containsValue(true)) {
      Set<Integer> taken = new HashSet<>();
      for (FieldDescriptorProto field : message.getFieldList()) {
        taken.add(field.getNumber());
      }
      int next = MAX_FIELD_NUMBER;
      for (FieldDescriptorProto.Builder field : message.getFieldBuilderList()) {
        if (Boolean.TRUE.equals(decided.get(field.getNumber()))) {
          while (taken.contains(next) || inExtensionRange(message, next)) {
            next--;
          }
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

  private static boolean inExtensionRange(DescriptorProto.Builder message, int number) {
    for (DescriptorProto.ExtensionRange range : message.getExtensionRangeList()) {
      if (number >= range.getStart() && number < range.getEnd()) {
        return true;
      }
    }
    return false;
  }
}
