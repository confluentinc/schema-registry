/*
 * Copyright 2020 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 *
 */

package io.confluent.kafka.schemaregistry.protobuf;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Range;
import com.google.protobuf.DescriptorProtos.DescriptorProto.ReservedRange;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.EnumDescriptor;
import com.google.protobuf.Descriptors.EnumValueDescriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.Descriptors.OneofDescriptor;
import com.google.protobuf.DynamicMessage;
import com.squareup.wire.schema.Field;
import com.squareup.wire.schema.Location;
import com.squareup.wire.schema.ProtoFile;
import com.squareup.wire.schema.ProtoType;
import com.squareup.wire.schema.internal.parser.EnumConstantElement;
import com.squareup.wire.schema.internal.parser.EnumElement;
import com.squareup.wire.schema.internal.parser.FieldElement;
import com.squareup.wire.schema.internal.parser.MessageElement;
import com.squareup.wire.schema.internal.parser.OneOfElement;
import com.squareup.wire.schema.internal.parser.OptionElement;
import com.squareup.wire.schema.internal.parser.ProtoFileElement;
import com.squareup.wire.schema.internal.parser.ProtoParser;
import com.squareup.wire.schema.internal.parser.ReservedElement;
import com.squareup.wire.schema.internal.parser.TypeElement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaReference;
import io.confluent.kafka.schemaregistry.protobuf.diff.Difference;
import io.confluent.kafka.schemaregistry.protobuf.diff.SchemaDiff;
import io.confluent.kafka.schemaregistry.protobuf.dynamic.DynamicSchema;
import io.confluent.kafka.schemaregistry.protobuf.dynamic.EnumDefinition;
import io.confluent.kafka.schemaregistry.protobuf.dynamic.MessageDefinition;

import static com.google.common.base.CaseFormat.LOWER_UNDERSCORE;
import static com.google.common.base.CaseFormat.UPPER_CAMEL;

public class ProtobufSchema implements ParsedSchema {

  private static final Logger log = LoggerFactory.getLogger(ProtobufSchema.class);

  public static final String TYPE = "PROTOBUF";

  public static final String DEFAULT_NAME = "default";
  public static final String MAP_ENTRY_SUFFIX = "Entry";  // Suffix used by protoc
  public static final String KEY_FIELD = "key";
  public static final String VALUE_FIELD = "value";

  public static final Location DEFAULT_LOCATION = Location.get(DEFAULT_NAME);

  private final ProtoFileElement schemaObj;

  private final Integer version;

  private final List<SchemaReference> references;

  private final Map<String, ProtoFileElement> dependencies;

  private transient DynamicSchema dynamicSchema;

  public ProtobufSchema(String schemaString) {
    this(schemaString, Collections.emptyList(), Collections.emptyMap(), null);
  }

  public ProtobufSchema(
      String schemaString,
      List<SchemaReference> references,
      Map<String, String> resolvedReferences,
      Integer version
  ) {
    try {
      this.schemaObj = ProtoParser.parse(DEFAULT_LOCATION, schemaString);
      this.version = version;
      this.references = references;
      this.dependencies = resolvedReferences.entrySet()
          .stream()
          .collect(Collectors.toMap(
              Map.Entry::getKey,
              e -> ProtoParser.parse(Location.get(e.getKey()), e.getValue())
          ));
    } catch (IllegalStateException e) {
      log.error("Cound not parse Protobuf schema " + schemaString, e);
      throw e;
    }
  }

  public ProtobufSchema(
      ProtoFileElement protoFileElement,
      List<SchemaReference> references,
      Map<String, ProtoFileElement> dependencies
  ) {
    this.schemaObj = protoFileElement;
    this.version = null;
    this.references = references;
    this.dependencies = dependencies;
  }

  public ProtobufSchema(Descriptor descriptor) {
    this(descriptor.getFile(), descriptor.getName());
  }

  public ProtobufSchema(
      FileDescriptor descriptor, String messageName
  ) {
    Map<String, ProtoFileElement> dependencies = new HashMap<>();
    this.schemaObj = toProtoFile(descriptor, messageName, dependencies);
    this.version = null;
    this.references = Collections.emptyList();
    this.dependencies = dependencies;
  }

  private ProtoFileElement toProtoFile(
      FileDescriptor file, String messageName, Map<String, ProtoFileElement> dependencies
  ) {
    final Location location = Location.get(file.getFullName());
    String packageName = file.getPackage();
    // Don't set empty package name
    if ("".equals(packageName)) {
      packageName = null;
    }

    ProtoFile.Syntax syntax = null;
    switch (file.getSyntax()) {
      case PROTO2:
        syntax = ProtoFile.Syntax.PROTO_2;
        break;
      case PROTO3:
        syntax = ProtoFile.Syntax.PROTO_3;
        break;
      case UNKNOWN:
      default:
        syntax = null;
        break;
    }
    ImmutableList.Builder<TypeElement> types = ImmutableList.builder();
    for (Descriptor md : reorderMessageTypes(file.getMessageTypes(), messageName)) {
      MessageElement message = toMessage(md);
      types.add(message);
    }
    for (EnumDescriptor ed : file.getEnumTypes()) {
      EnumElement enumer = toEnum(ed);
      types.add(enumer);
    }
    ImmutableList.Builder<String> imports = ImmutableList.builder();
    for (FileDescriptor dependency : file.getDependencies()) {
      String depName = dependency.getName();
      dependencies.put(depName, toProtoFile(dependency, depName, dependencies));
      imports.add(depName);
    }
    ImmutableList.Builder<String> publicImports = ImmutableList.builder();
    for (FileDescriptor dependency : file.getPublicDependencies()) {
      String depName = dependency.getName();
      dependencies.put(depName, toProtoFile(dependency, depName, dependencies));
      publicImports.add(depName);
    }
    // NOTE: skip services, extensions, options
    return new ProtoFileElement(location,
        packageName,
        syntax,
        imports.build(),
        publicImports.build(),
        types.build(),
        Collections.emptyList(),
        Collections.emptyList(),
        Collections.emptyList()
    );
  }

  private List<Descriptor> reorderMessageTypes(List<Descriptor> types, String messageName) {
    List<Descriptor> result = new ArrayList<>(types.size());
    for (Descriptor type : types) {
      if (type.getName().equals(messageName)) {
        result.add(type);
      }
    }
    for (Descriptor type : types) {
      if (!type.getName().equals(messageName)) {
        result.add(type);
      }
    }
    return result;
  }

  private MessageElement toMessage(Descriptor descriptor) {
    Location location = Location.get(descriptor.getFile().getFullName());
    String name = descriptor.getName();
    log.trace("*** msg name: " + name);
    ImmutableList.Builder<FieldElement> fields = ImmutableList.builder();
    ImmutableList.Builder<OneOfElement> oneofs = ImmutableList.builder();
    ImmutableList.Builder<TypeElement> nested = ImmutableList.builder();
    ImmutableList.Builder<ReservedElement> reserved = ImmutableList.builder();
    for (OneofDescriptor od : descriptor.getOneofs()) {
      OneOfElement oneof = toOneof(od);
      oneofs.add(oneof);
    }
    for (FieldDescriptor fd : descriptor.getFields()) {
      if (fd.getContainingOneof() == null) {
        FieldElement field = toField(fd, false);
        fields.add(field);
      }
    }
    for (Descriptor nestedDesc : descriptor.getNestedTypes()) {
      MessageElement nestedMessage = toMessage(nestedDesc);
      nested.add(nestedMessage);
    }
    for (EnumDescriptor nestedDesc : descriptor.getEnumTypes()) {
      EnumElement nestedEnum = toEnum(nestedDesc);
      nested.add(nestedEnum);
    }
    for (ReservedRange range : descriptor.toProto().getReservedRangeList()) {
      ReservedElement reservedElem = toReserved(location, range);
      reserved.add(reservedElem);
    }
    for (String reservedName : descriptor.toProto().getReservedNameList()) {
      ReservedElement reservedElem = new ReservedElement(
          location,
          "",
          Collections.singletonList(reservedName)
      );
      reserved.add(reservedElem);
    }
    ImmutableList.Builder<OptionElement> options = ImmutableList.builder();
    if (descriptor.getOptions().hasMapEntry()) {
      OptionElement.Kind kind = OptionElement.Kind.BOOLEAN;
      OptionElement option = new OptionElement(
          "map_entry",
          kind,
          descriptor.getOptions().getMapEntry(),
          false
      );
      options.add(option);
    }
    // NOTE: skip some options, extensions, groups
    return new MessageElement(location,
        name,
        "",
        nested.build(),
        options.build(),
        reserved.build(),
        fields.build(),
        oneofs.build(),
        Collections.emptyList(),
        Collections.emptyList()
    );
  }

  private ReservedElement toReserved(Location location, ReservedRange range) {
    List<Object> values = new ArrayList<>();
    int start = range.getStart();
    int end = range.getEnd();
    values.add(start == end ? start : Range.closed(start, end));
    return new ReservedElement(location, "", values);
  }

  private OneOfElement toOneof(OneofDescriptor od) {
    String name = od.getName();
    log.trace("*** oneof name: " + name);
    ImmutableList.Builder<FieldElement> fields = ImmutableList.builder();
    for (FieldDescriptor fd : od.getFields()) {
      FieldElement field = toField(fd, true);
      fields.add(field);
    }
    // NOTE: skip groups
    return new OneOfElement(name, "", fields.build(), Collections.emptyList());
  }

  private EnumElement toEnum(EnumDescriptor ed) {
    Location location = Location.get(ed.getFile().getFullName());
    String name = ed.getName();
    log.trace("*** enum name: " + name);
    ImmutableList.Builder<EnumConstantElement> constants = ImmutableList.builder();
    for (EnumValueDescriptor ev : ed.getValues()) {
      // NOTE: skip options
      constants.add(new EnumConstantElement(
          location,
          ev.getName(),
          ev.getNumber(),
          "",
          Collections.emptyList()
      ));
    }
    ImmutableList.Builder<OptionElement> options = ImmutableList.builder();
    if (ed.getOptions().hasAllowAlias()) {
      OptionElement.Kind kind = OptionElement.Kind.BOOLEAN;
      OptionElement option = new OptionElement(
          "allow_alis",
          kind,
          ed.getOptions().getAllowAlias(),
          false
      );
      options.add(option);
    }
    // NOTE: skip some options
    return new EnumElement(location, name, "", options.build(), constants.build());
  }

  private FieldElement toField(FieldDescriptor fd, boolean inOneof) {
    final Location location = Location.get(fd.getFile().getFullName());
    String name = fd.getName();
    log.trace("*** field name: " + name);
    ImmutableList.Builder<OptionElement> options = ImmutableList.builder();
    if (fd.getOptions().hasPacked()) {
      OptionElement.Kind kind = OptionElement.Kind.BOOLEAN;
      OptionElement option = new OptionElement("packed", kind, fd.getOptions().getPacked(), false);
      options.add(option);
    }
    if (fd.toProto().hasJsonName()) {
      OptionElement.Kind kind = OptionElement.Kind.STRING;
      OptionElement option = new OptionElement(
          "json_name",
          kind,
          fd.toProto().getJsonName(),
          false
      );
      options.add(option);
    }
    String defaultValue = fd.hasDefaultValue() && fd.getDefaultValue() != null
                          ? fd.getDefaultValue().toString()
                          : null;
    // NOTE: skip some options
    return new FieldElement(location,
        inOneof ? null : label(fd),
        dataType(fd),
        name,
        defaultValue,
        fd.getNumber(),
        "",
        options.build()
    );
  }

  private Field.Label label(FieldDescriptor fd) {
    FileDescriptor.Syntax syntax = fd.getFile().getSyntax();
    boolean isProto3 = syntax == FileDescriptor.Syntax.PROTO3;
    switch (fd.toProto().getLabel()) {
      case LABEL_REQUIRED:
        return isProto3 ? null : Field.Label.REQUIRED;
      case LABEL_OPTIONAL:
        return isProto3 ? null : Field.Label.OPTIONAL;
      case LABEL_REPEATED:
        return Field.Label.REPEATED;
      default:
        throw new IllegalArgumentException("Unsupported label");
    }
  }

  private String dataType(FieldDescriptor field) {
    FieldDescriptor.Type type = field.getType();
    switch (type) {
      case MESSAGE:
        return field.getMessageType().getFullName();
      case ENUM:
        return field.getEnumType().getFullName();
      default:
        return type.name().toLowerCase();
    }
  }

  public Descriptor toDescriptor() {
    return toDescriptor(name());
  }

  public Descriptor toDescriptor(String name) {
    return toDynamicSchema().getMessageDescriptor(name);
  }

  public DynamicMessage.Builder newMessageBuilder() {
    return newMessageBuilder(name());
  }

  public DynamicMessage.Builder newMessageBuilder(String name) {
    return toDynamicSchema().newMessageBuilder(name);
  }

  private MessageElement firstMessage() {
    for (TypeElement typeElement : schemaObj.getTypes()) {
      if (typeElement instanceof MessageElement) {
        return (MessageElement) typeElement;
      }
    }
    throw new IllegalArgumentException("Protobuf schema definition "
        + " contains no message type definitions");
  }

  @VisibleForTesting
  protected DynamicSchema toDynamicSchema() {
    if (dynamicSchema == null) {
      dynamicSchema = toDynamicSchema(DEFAULT_NAME, schemaObj, dependencies);
    }
    return dynamicSchema;
  }

  /*
   * DynamicSchema is used as a temporary helper class and should not be exposed in the API.
   */
  private static DynamicSchema toDynamicSchema(
      String name, ProtoFileElement rootElem, Map<String, ProtoFileElement> dependencies
  ) {
    log.trace("*** toDynamicSchema: " + rootElem.toSchema());
    DynamicSchema.Builder schema = DynamicSchema.newBuilder();
    try {
      if (rootElem.getPackageName() != null) {
        schema.setPackage(rootElem.getPackageName());
      }
      for (TypeElement typeElem : rootElem.getTypes()) {
        if (typeElem instanceof MessageElement) {
          MessageDefinition message = toDynamicMessage(schema, (MessageElement) typeElem);
          schema.addMessageDefinition(message);
        } else if (typeElem instanceof EnumElement) {
          EnumDefinition enumer = toDynamicEnum((EnumElement) typeElem);
          schema.addEnumDefinition(enumer);
        }
      }
      for (String ref : rootElem.getImports()) {
        ProtoFileElement dep = dependencies.get(ref);
        if (dep != null) {
          schema.addDependency(ref);
          schema.addSchema(toDynamicSchema(ref, dep, dependencies));
        }
      }
      for (String ref : rootElem.getPublicImports()) {
        ProtoFileElement dep = dependencies.get(ref);
        if (dep != null) {
          schema.addPublicDependency(ref);
          schema.addSchema(toDynamicSchema(ref, dep, dependencies));
        }
      }
      schema.setName(name);
      return schema.build();
    } catch (Descriptors.DescriptorValidationException e) {
      throw new IllegalStateException(e);
    }
  }

  private static MessageDefinition toDynamicMessage(
      DynamicSchema.Builder schema,
      MessageElement messageElem
  ) {
    log.trace("*** message: " + messageElem.getName());
    MessageDefinition.Builder message = MessageDefinition.newBuilder(messageElem.getName());
    Set<String> added = new HashSet<>();
    for (OneOfElement oneof : messageElem.getOneOfs()) {
      MessageDefinition.OneofBuilder oneofBuilder = message.addOneof(oneof.getName());
      for (FieldElement field : oneof.getFields()) {
        String defaultVal = field.getDefaultValue();
        String jsonName = findOption("json_name", field.getOptions()).map(o -> o.getValue()
            .toString()).orElse(null);
        oneofBuilder.addField(
            field.getType(),
            field.getName(),
            field.getTag(),
            defaultVal,
            jsonName
        );
        added.add(field.getName());
      }
    }
    for (FieldElement field : messageElem.getFields()) {
      if (added.contains(field.getName())) {
        continue;
      }
      String label = field.getLabel() != null ? field.getLabel().toString().toLowerCase() : null;
      String fieldType = field.getType();
      String defaultVal = field.getDefaultValue();
      String jsonName = findOption("json_name", field.getOptions()).map(o -> o.getValue()
          .toString()).orElse(null);
      Boolean isPacked = findOption(
          "packed",
          field.getOptions()
      ).map(o -> Boolean.valueOf(o.getValue().toString())).orElse(null);
      ProtoType protoType = ProtoType.get(fieldType);
      // Map fields are only permitted in messages
      if (protoType.isMap()) {
        label = "repeated";
        fieldType = toMapEntry(field.getName());
        MessageDefinition.Builder mapMessage = MessageDefinition.newBuilder(fieldType);
        mapMessage.setMapEntry(true);
        mapMessage.addField(null, protoType.keyType().simpleName(), KEY_FIELD, 1, null);
        mapMessage.addField(null, protoType.valueType().simpleName(), VALUE_FIELD, 2, null);
        schema.addMessageDefinition(mapMessage.build());
      }
      message.addField(
          label,
          fieldType,
          field.getName(),
          field.getTag(),
          defaultVal,
          jsonName,
          isPacked
      );
    }
    for (TypeElement type : messageElem.getNestedTypes()) {
      if (type instanceof MessageElement) {
        message.addMessageDefinition(toDynamicMessage(schema, (MessageElement) type));
      } else if (type instanceof EnumElement) {
        message.addEnumDefinition(toDynamicEnum((EnumElement) type));
      }
    }
    for (ReservedElement reserved : messageElem.getReserveds()) {
      for (Object elem : reserved.getValues()) {
        if (elem instanceof String) {
          message.addReservedName((String) elem);
        } else if (elem instanceof Integer) {
          int tag = (Integer) elem;
          message.addReservedRange(tag, tag);
        } else if (elem instanceof Range) {
          Range<Integer> range = (Range<Integer>) elem;
          message.addReservedRange(range.lowerEndpoint(), range.upperEndpoint());
        } else {
          throw new IllegalStateException("Unsupported reserved type: " + elem.getClass()
              .getName());
        }
      }
    }
    return message.build();
  }

  private static Optional<OptionElement> findOption(String name, List<OptionElement> options) {
    return options.stream().filter(o -> o.getName().equals(name)).findFirst();
  }

  private static EnumDefinition toDynamicEnum(EnumElement enumElem) {
    Boolean allowAlias = findOption("allow_alias", enumElem.getOptions()).map(o -> Boolean.valueOf(o
        .getValue()
        .toString())).orElse(null);
    EnumDefinition.Builder enumer = EnumDefinition.newBuilder(enumElem.getName(), allowAlias);
    for (EnumConstantElement constant : enumElem.getConstants()) {
      enumer.addValue(constant.getName(), constant.getTag());
    }
    return enumer.build();
  }

  public ProtoFileElement rawSchema() {
    return schemaObj;
  }

  @Override
  public String schemaType() {
    return TYPE;
  }

  @Override
  public String name() {
    return firstMessage().getName();
  }

  @Override
  public String canonicalString() {
    // Remove full-line comments, such as the location
    return schemaObj.toSchema().replaceAll("^//.*?\\n", "");
  }

  public Integer version() {
    return version;
  }

  @Override
  public List<SchemaReference> references() {
    return references;
  }

  public Map<String, String> resolvedReferences() {
    return dependencies.entrySet()
        .stream()
        .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().toSchema()));
  }

  public Map<String, ProtoFileElement> dependencies() {
    return dependencies;
  }

  @Override
  public boolean isBackwardCompatible(ParsedSchema previousSchema) {
    if (!schemaType().equals(previousSchema.schemaType())) {
      return false;
    }
    final List<Difference> differences = SchemaDiff.compare(
        ((ProtobufSchema) previousSchema).schemaObj,
        schemaObj
    );
    final List<Difference> incompatibleDiffs = differences.stream()
        .filter(diff -> !SchemaDiff.COMPATIBLE_CHANGES.contains(diff.getType()))
        .collect(Collectors.toList());
    boolean isCompatible = incompatibleDiffs.isEmpty();
    if (!isCompatible) {
      log.warn("Found incompatible change: {}", incompatibleDiffs.get(0));
    }
    return isCompatible;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ProtobufSchema that = (ProtobufSchema) o;
    return Objects.equals(schemaObj, that.schemaObj)
        && Objects.equals(references, that.references)
        && Objects.equals(version, that.version);
  }

  @Override
  public int hashCode() {
    return Objects.hash(schemaObj, references, version);
  }

  @Override
  public String toString() {
    return canonicalString();
  }

  public static String toMapEntry(String s) {
    if (s.contains("_")) {
      s = LOWER_UNDERSCORE.to(UPPER_CAMEL, s);
    }
    return s + MAP_ENTRY_SUFFIX;
  }

  public static String toMapField(String s) {
    if (s.endsWith(MAP_ENTRY_SUFFIX)) {
      s = s.substring(0, s.length() - MAP_ENTRY_SUFFIX.length());
      s = UPPER_CAMEL.to(LOWER_UNDERSCORE, s);
    }
    return s;
  }
}
