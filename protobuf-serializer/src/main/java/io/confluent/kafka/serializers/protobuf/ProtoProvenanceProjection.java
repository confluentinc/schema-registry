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
import com.google.protobuf.DescriptorProtos.OneofDescriptorProto;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.DescriptorValidationException;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.Descriptors.OneofDescriptor;
import com.google.protobuf.Message;
import com.google.protobuf.UnknownFieldSet;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import io.confluent.kafka.serializers.provenance.ProvenanceMapping;
import io.confluent.kafka.serializers.provenance.ProvenanceUnavailableException;
import io.confluent.protobuf.MetaProto;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;
import org.apache.kafka.common.errors.SerializationException;

/**
 * Carries provenance into protobuf's parser through the descriptor it parses with.
 *
 * <p>The parser pairs a wire field with a descriptor field by number and decodes it as the
 * descriptor's type. A field's provenance identity is its number too, so the two agree except where
 * a number was dropped and later reused: the reader's field is then a new entity, and number
 * matching would hand it the old field's data. Each such reader field is left out of the parse
 * descriptor, so whatever the record holds under its number, the writer's field or data no schema
 * declares, lands in the unknown fields and is dropped, and the reader's field reads as unset.
 * The record is then reparsed into the reader's own descriptor, so domain rules see the reader as
 * it is.
 *
 * <p>Fields are found by the names the provenance response carries, which the converter recorded
 * as the descriptor's own route to each location. A projection holds the numbers left out of each
 * message. A record is parsed with the reader's own descriptor and cleared of whatever lies under
 * them, which is the same message. It is parsed with the reader less them, built when first
 * needed, where that parse throws or leaves the message uninitialized, or where a oneof holding a
 * left-out member may have lost a kept member's data.
 */
final class ProtoProvenanceProjection {

  private final Map<String, Set<Integer>> moved;
  // The messages, by full name, holding a moved number or leading to one through their fields:
  // only these are visited for data under one.
  private final Set<String> leading;
  // The reader less its moved fields, for the records its own parse cannot serve: built once to
  // fail the pairing as before, then again only when a record needs it, and kept.
  private final Supplier<ProtobufSchema> prune;
  private volatile ProtobufSchema pruned;

  private ProtoProvenanceProjection(ProtobufSchema pruned, Supplier<ProtobufSchema> prune,
      Descriptor reader, Map<String, Set<Integer>> moved) {
    this.pruned = pruned;
    this.prune = prune;
    this.moved = moved;
    this.leading = moved.isEmpty() ? Collections.emptySet() : leading(reader, moved);
  }

  /**
   * The reader less every field provenance gives no writer counterpart; the reader itself when
   * there is none.
   */
  ProtobufSchema schema() {
    ProtobufSchema schema = pruned;
    if (schema == null) {
      // A race builds two equal schemas; either serves.
      schema = prune.get();
      pruned = schema;
    }
    return schema;
  }

  // For tests: whether the reader less its moved fields is held.
  boolean prunedBuilt() {
    return pruned != null;
  }

  private static Set<String> leading(Descriptor root, Map<String, Set<Integer>> moved) {
    Map<String, Descriptor> all = new HashMap<>();
    collect(root, all);
    Set<String> leads = new HashSet<>(moved.keySet());
    boolean changed = true;
    while (changed) {
      changed = false;
      for (Descriptor message : all.values()) {
        if (leads.contains(message.getFullName())) {
          continue;
        }
        for (FieldDescriptor field : message.getFields()) {
          if (field.getJavaType() == FieldDescriptor.JavaType.MESSAGE
              && leads.contains(field.getMessageType().getFullName())) {
            leads.add(message.getFullName());
            changed = true;
            break;
          }
        }
      }
    }
    return leads;
  }

  private static void collect(Descriptor message, Map<String, Descriptor> all) {
    if (all.putIfAbsent(message.getFullName(), message) != null) {
      return;
    }
    for (FieldDescriptor field : message.getFields()) {
      if (field.getJavaType() == FieldDescriptor.JavaType.MESSAGE) {
        collect(field.getMessageType(), all);
      }
    }
  }

  /**
   * Clears from {@code builder}, the reader's own parse, every moved field and the unknown data
   * under a moved number, at any depth. False, with the builder half cleared, where a oneof holding
   * a moved member may have lost a kept one's data: a moved member is set, its data may have
   * displaced a kept one's; a kept message member is set, a moved one may have split its halves.
   */
  boolean clearMoved(Message.Builder builder) {
    Descriptor type = builder.getDescriptorForType();
    if (!leading.contains(type.getFullName())) {
      return true;
    }
    Set<Integer> numbers = moved.get(type.getFullName());
    if (numbers != null) {
      for (OneofDescriptor oneof : type.getRealOneofs()) {
        // A kept message member split by a moved one is reset by the reader's parse, where the
        // projected parse merges its halves.
        FieldDescriptor set = builder.getOneofFieldDescriptor(oneof);
        if (set != null && set.getJavaType() == FieldDescriptor.JavaType.MESSAGE
            && !numbers.contains(set.getNumber())
            && !Collections.disjoint(numbers, numbersOf(oneof))) {
          return false;
        }
      }
      UnknownFieldSet.Builder unknown = null;
      for (int number : numbers) {
        FieldDescriptor field = type.findFieldByNumber(number);
        if (field != null && (field.isRepeated()
            ? builder.getRepeatedFieldCount(field) > 0 : builder.hasField(field))) {
          OneofDescriptor oneof = field.getRealContainingOneof();
          if (oneof != null && !numbers.containsAll(numbersOf(oneof))) {
            return false;
          }
          builder.clearField(field);
        }
        if (builder.getUnknownFields().hasField(number)) {
          unknown = unknown != null ? unknown : builder.getUnknownFields().toBuilder();
          unknown.clearField(number);
        }
      }
      if (unknown != null) {
        builder.setUnknownFields(unknown.build());
      }
    }
    for (FieldDescriptor field : type.getFields()) {
      if (field.getJavaType() != FieldDescriptor.JavaType.MESSAGE
          || !leading.contains(field.getMessageType().getFullName())) {
        continue;
      }
      if (field.isRepeated()) {
        for (int i = 0; i < builder.getRepeatedFieldCount(field); i++) {
          Message.Builder nested = ((Message) builder.getRepeatedField(field, i)).toBuilder();
          if (!clearMoved(nested)) {
            return false;
          }
          builder.setRepeatedField(field, i, nested.buildPartial());
        }
      } else if (builder.hasField(field)) {
        Message.Builder nested = ((Message) builder.getField(field)).toBuilder();
        if (!clearMoved(nested)) {
          return false;
        }
        builder.setField(field, nested.buildPartial());
      }
    }
    return true;
  }

  private static Set<Integer> numbersOf(OneofDescriptor oneof) {
    Set<Integer> numbers = new HashSet<>();
    for (FieldDescriptor member : oneof.getFields()) {
      numbers.add(member.getNumber());
    }
    return numbers;
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
      if (field.getJavaType() != FieldDescriptor.JavaType.MESSAGE
          || !leading.contains(field.getMessageType().getFullName())) {
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

  /**
   * The fields of {@code reader} provenance gives no writer counterpart, left out of the parse:
   * none when there is nothing to leave out. A field of an imported message is left out of a copy
   * of its file, which the reader's file is built against.
   *
   * @throws ProvenanceUnavailableException if a message used at several locations would need
   *     different fields left out, or the record's message is a nested one no location reaches
   * @throws SerializationException if a location's names are missing or not in the reader
   */
  static ProtoProvenanceProjection of(ProtobufSchema reader, ProtobufSchema writer,
      ProvenanceMapping mapping, boolean includeMultipleMessages) {
    // A location the walk cannot find, or a response without kinds, would escape provenance
    // without a trace.
    mapping.requireNames();
    mapping.requireKinds();
    Descriptor root = reader.toDescriptor();
    Walk walk = new Walk(root.getFile(), mapping.readerId());
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
      if (walk.underMovingField(root, path, moving, mapping, includeMultipleMessages)) {
        // Nothing under a field that moves is ever read, so nothing under it needs a number.
        if (nested && !reachedMoving) {
          FieldDescriptor at = walk.fieldAt(root, names, includeMultipleMessages);
          reachedMoving = at != null && isOf(at, root);
        }
        continue;
      }
      boolean move = mapping.writerPathOf(path) == null;
      if (move) {
        moving.add(path);
      }
      FieldDescriptor field = walk.fieldAt(root, names, includeMultipleMessages);
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
      if (isOneof(path, names, field, mapping)) {
        // A oneof: no step of its own, so no field to number; its members are visited as fields.
        continue;
      }
      if (field != null && (!nested || walk.passesThrough(root, names))) {
        // Read as a nested record, only what lies under the record's message is ever parsed.
        walk.decide(field.getContainingType(), field, move);
      }
    }
    if (nested && !reached && reachedMoving) {
      // Every location reaching the record's message is under a restarted use: each of its
      // fields is new there, so none takes the writer's value.
      for (FieldDescriptor field : root.getFields()) {
        walk.decide(root, field, true);
      }
      reached = true;
    }
    if (nested && reached && writer != null) {
      // Read directly, the record's fields continue only fields of the writer's own message: one
      // continuing a field of another message at every use takes no value here.
      Map<Integer, Boolean> decided = walk.moves.get(root.getFullName());
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
    // Built here so a pairing fails as before; only what moves is kept to build it again.
    ProtobufSchema pruned = pruned(walk.build(), root, reader);
    if (pruned == reader) {
      return new ProtoProvenanceProjection(reader, null, root, Collections.emptyMap());
    }
    walk.keepMovedOnly();
    return new ProtoProvenanceProjection(null, () -> pruned(walk.build(), root, reader), root,
        walk.movedNumbers());
  }

  private static ProtobufSchema pruned(FileDescriptor file, Descriptor root,
      ProtobufSchema reader) {
    if (file == root.getFile()) {
      return reader;
    }
    Descriptor prunedRoot = messageNamed(file, root.getFullName());
    ProtobufSchema schema = new ProtobufSchema(prunedRoot != null ? prunedRoot : root,
        reader.references());
    return (ProtobufSchema) schema.copy(reader.metadata(), reader.ruleSet());
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

  private static Descriptor messageOf(FieldDescriptor field) {
    return field != null && field.getJavaType() == FieldDescriptor.JavaType.MESSAGE
        ? field.getMessageType()
        : null;
  }

  /** The walk building a projection: its state lives only as long as the build. */
  private static final class Walk {
    private final FileDescriptor file;
    private final int readerId;
    // For each message, by full name: which of its field numbers move out of the parse.
    private final Map<String, Map<Integer, Boolean>> moves = new HashMap<>();

    private Walk(FileDescriptor file, int readerId) {
      this.file = file;
      this.readerId = readerId;
    }

    /**
     * Whether an ancestor of {@code path} is a moving field. A oneof moving is no field: its
     * names are its parent's, and its members still need numbers of their own. Nor is a top-level
     * message: restarted, it moves none of its members by itself.
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

    // A pairing then holds one entry a moved field, not one a field of every message reached.
    private void keepMovedOnly() {
      moves.values().forEach(decided -> decided.values().removeIf(move -> !move));
      moves.values().removeIf(Map::isEmpty);
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

    private FileDescriptor build() {
      if (moves.values().stream().noneMatch(m -> m.containsValue(true))) {
        return file;
      }
      return rebuild(file, new HashMap<>());
    }

    // The file to parse with, its imports first: an imported message's fields are left out of a
    // copy of its file under the same name, which the importing file is built against. Others pass
    // through.
    private FileDescriptor rebuild(FileDescriptor of, Map<String, FileDescriptor> rebuilt) {
      FileDescriptor done = rebuilt.get(of.getName());
      if (done != null) {
        return done;
      }
      List<FileDescriptor> dependencies = new ArrayList<>();
      boolean changed = false;
      for (FileDescriptor dependency : of.getDependencies()) {
        FileDescriptor copy = rebuild(dependency, rebuilt);
        changed |= copy != dependency;
        dependencies.add(copy);
      }
      if (!changed && of.getMessageTypes().stream().noneMatch(this::movesIn)) {
        rebuilt.put(of.getName(), of);
        return of;
      }
      FileDescriptorProto.Builder proto = of.toProto().toBuilder();
      String prefix = of.getPackage().isEmpty() ? "" : of.getPackage() + ".";
      for (DescriptorProto.Builder message : proto.getMessageTypeBuilderList()) {
        pruneMessage(message, prefix + message.getName());
      }
      try {
        FileDescriptor copy = FileDescriptor.buildFrom(
            proto.build(), dependencies.toArray(new FileDescriptor[0]));
        rebuilt.put(of.getName(), copy);
        return copy;
      } catch (DescriptorValidationException e) {
        throw new SerializationException("Could not build " + of.getName()
            + " to parse with after provenance: " + e.getMessage(), e);
      }
    }

    private boolean movesIn(Descriptor message) {
      Map<Integer, Boolean> decided = moves.get(message.getFullName());
      return decided != null && decided.containsValue(true)
          || message.getNestedTypes().stream().anyMatch(this::movesIn);
    }

    private void pruneMessage(DescriptorProto.Builder message, String fullName) {
      Map<Integer, Boolean> decided = moves.get(fullName);
      if (decided != null && decided.containsValue(true)) {
        List<FieldDescriptorProto> kept = new ArrayList<>();
        Set<Integer> usedOneofs = new HashSet<>();
        for (FieldDescriptorProto field : message.getFieldList()) {
          if (!Boolean.TRUE.equals(decided.get(field.getNumber()))) {
            kept.add(field);
            if (field.hasOneofIndex()) {
              usedOneofs.add(field.getOneofIndex());
            }
          }
        }
        // A oneof left with no member is no oneof; the rest keep their order.
        Map<Integer, Integer> oneofIndex = new HashMap<>();
        List<OneofDescriptorProto> oneofs = new ArrayList<>();
        for (int i = 0; i < message.getOneofDeclCount(); i++) {
          if (usedOneofs.contains(i)) {
            oneofIndex.put(i, oneofs.size());
            oneofs.add(message.getOneofDecl(i));
          }
        }
        message.clearField();
        for (FieldDescriptorProto field : kept) {
          message.addField(field.hasOneofIndex()
              ? field.toBuilder().setOneofIndex(oneofIndex.get(field.getOneofIndex())).build()
              : field);
        }
        message.clearOneofDecl();
        message.addAllOneofDecl(oneofs);
      }
      for (DescriptorProto.Builder nested : message.getNestedTypeBuilderList()) {
        pruneMessage(nested, fullName + "." + nested.getName());
      }
    }
  }
}
