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
import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.DescriptorProtos.DescriptorProto;
import com.google.protobuf.DescriptorProtos.DescriptorProto.ReservedRange;
import com.google.protobuf.DescriptorProtos.EnumDescriptorProto;
import com.google.protobuf.DescriptorProtos.EnumValueDescriptorProto;
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto;
import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import com.google.protobuf.DescriptorProtos.OneofDescriptorProto;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Descriptors.FileDescriptor;
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
import kotlin.ranges.IntRange;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
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

  public static final String SERIALIZED_FORMAT = "serialized";

  public static final String PROTO2 = "proto2";
  public static final String PROTO3 = "proto3";

  public static final String DEFAULT_NAME = "default";
  public static final String MAP_ENTRY_SUFFIX = "Entry";  // Suffix used by protoc
  public static final String KEY_FIELD = "key";
  public static final String VALUE_FIELD = "value";

  public static final Location DEFAULT_LOCATION = Location.get("");

  private final ProtoFileElement schemaObj;

  private final Integer version;

  private final String name;

  private final List<SchemaReference> references;

  private final Map<String, ProtoFileElement> dependencies;

  private transient String canonicalString;

  private transient DynamicSchema dynamicSchema;

  private transient Descriptor descriptor;

  private transient int hashCode = NO_HASHCODE;

  private static final int NO_HASHCODE = Integer.MIN_VALUE;

  private static final Base64.Encoder base64Encoder = Base64.getEncoder();

  private static final Base64.Decoder base64Decoder = Base64.getDecoder();

  public ProtobufSchema(String schemaString) {
    this(schemaString, Collections.emptyList(), Collections.emptyMap(), null, null);
  }

  public ProtobufSchema(
      String schemaString,
      List<SchemaReference> references,
      Map<String, String> resolvedReferences,
      Integer version,
      String name
  ) {
    try {
      this.schemaObj = toProtoFile(schemaString);
      this.version = version;
      this.name = name;
      this.references = Collections.unmodifiableList(references);
      this.dependencies = Collections.unmodifiableMap(resolvedReferences.entrySet()
          .stream()
          .collect(Collectors.toMap(
              Map.Entry::getKey,
              e -> toProtoFile(e.getValue())
          )));
    } catch (IllegalStateException e) {
      log.error("Could not parse Protobuf schema " + schemaString
          + " with references " + references, e);
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
    this.name = null;
    this.references = Collections.unmodifiableList(references);
    this.dependencies = Collections.unmodifiableMap(dependencies);
  }

  public ProtobufSchema(Descriptor descriptor) {
    Map<String, ProtoFileElement> dependencies = new HashMap<>();
    this.schemaObj = toProtoFile(descriptor.getFile(), dependencies);
    this.version = null;
    this.name = descriptor.getFullName();
    this.references = Collections.emptyList();
    this.dependencies = dependencies;
    this.descriptor = descriptor;
  }

  private ProtobufSchema(
      ProtoFileElement schemaObj,
      Integer version,
      String name,
      List<SchemaReference> references,
      Map<String, ProtoFileElement> dependencies,
      String canonicalString,
      DynamicSchema dynamicSchema,
      Descriptor descriptor
  ) {
    this.schemaObj = schemaObj;
    this.version = version;
    this.name = name;
    this.references = references;
    this.dependencies = dependencies;
    this.canonicalString = canonicalString;
    this.dynamicSchema = dynamicSchema;
    this.descriptor = descriptor;
  }

  public ProtobufSchema copy() {
    return new ProtobufSchema(
        this.schemaObj,
        this.version,
        this.name,
        this.references,
        this.dependencies,
        this.canonicalString,
        this.dynamicSchema,
        this.descriptor
    );
  }

  public ProtobufSchema copy(Integer version) {
    return new ProtobufSchema(
        this.schemaObj,
        version,
        this.name,
        this.references,
        this.dependencies,
        this.canonicalString,
        this.dynamicSchema,
        this.descriptor
    );
  }

  public ProtobufSchema copy(String name) {
    return new ProtobufSchema(
        this.schemaObj,
        this.version,
        name,
        this.references,
        this.dependencies,
        this.canonicalString,
        this.dynamicSchema,
        this.descriptor
    );
  }

  public ProtobufSchema copy(List<SchemaReference> references) {
    return new ProtobufSchema(
        this.schemaObj,
        this.version,
        this.name,
        references,
        this.dependencies,
        this.canonicalString,
        this.dynamicSchema,
        this.descriptor
    );
  }

  private ProtoFileElement toProtoFile(String schema) {
    try {
      return ProtoParser.Companion.parse(DEFAULT_LOCATION, schema);
    } catch (Exception e) {
      try {
        // Attempt to parse binary FileDescriptorProto
        byte[] bytes = base64Decoder.decode(schema);
        return toProtoFile(FileDescriptorProto.parseFrom(bytes));
      } catch (Exception pe) {
        throw new IllegalArgumentException("Could not parse Protobuf", e);
      }
    }
  }

  private ProtoFileElement toProtoFile(
      FileDescriptor file, Map<String, ProtoFileElement> dependencies
  ) {
    for (FileDescriptor dependency : file.getDependencies()) {
      String depName = dependency.getName();
      dependencies.put(depName, toProtoFile(dependency, dependencies));
    }
    return toProtoFile(file.toProto());
  }

  private ProtoFileElement toProtoFile(FileDescriptorProto file) {
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
      default:
        break;
    }
    ImmutableList.Builder<TypeElement> types = ImmutableList.builder();
    for (DescriptorProto md : file.getMessageTypeList()) {
      MessageElement message = toMessage(file, md);
      types.add(message);
    }
    for (EnumDescriptorProto ed : file.getEnumTypeList()) {
      EnumElement enumer = toEnum(ed);
      types.add(enumer);
    }
    ImmutableList.Builder<String> imports = ImmutableList.builder();
    ImmutableList.Builder<String> publicImports = ImmutableList.builder();
    List<String> dependencyList = file.getDependencyList();
    Set<Integer> publicDependencyList = new HashSet<>(file.getPublicDependencyList());
    for (int i = 0; i < dependencyList.size(); i++) {
      String depName = dependencyList.get(i);
      if (publicDependencyList.contains(i)) {
        publicImports.add(depName);
      } else {
        imports.add(depName);
      }
    }
    ImmutableList.Builder<OptionElement> options = ImmutableList.builder();
    if (file.getOptions().hasJavaPackage()) {
      OptionElement.Kind kind = OptionElement.Kind.STRING;
      OptionElement option = new OptionElement(
          "java_package",
          kind,
          file.getOptions().getJavaPackage(),
          false
      );
      options.add(option);
    }
    if (file.getOptions().hasJavaOuterClassname()) {
      OptionElement.Kind kind = OptionElement.Kind.STRING;
      OptionElement option = new OptionElement(
          "java_outer_classname",
          kind,
          file.getOptions().getJavaOuterClassname(),
          false
      );
      options.add(option);
    }
    if (file.getOptions().hasJavaMultipleFiles()) {
      OptionElement.Kind kind = OptionElement.Kind.BOOLEAN;
      OptionElement option = new OptionElement(
          "java_multiple_files",
          kind,
          file.getOptions().getJavaMultipleFiles(),
          false
      );
      options.add(option);
    }
    // NOTE: skip services, extensions, some options
    return new ProtoFileElement(DEFAULT_LOCATION,
        packageName,
        syntax,
        imports.build(),
        publicImports.build(),
        types.build(),
        Collections.emptyList(),
        Collections.emptyList(),
        options.build()
    );
  }

  private MessageElement toMessage(FileDescriptorProto file, DescriptorProto descriptor) {
    String name = descriptor.getName();
    log.trace("*** msg name: {}", name);
    ImmutableList.Builder<FieldElement> fields = ImmutableList.builder();
    ImmutableList.Builder<TypeElement> nested = ImmutableList.builder();
    ImmutableList.Builder<ReservedElement> reserved = ImmutableList.builder();
    LinkedHashMap<String, ImmutableList.Builder<FieldElement>> oneofsMap = new LinkedHashMap<>();
    for (OneofDescriptorProto od : descriptor.getOneofDeclList()) {
      oneofsMap.put(od.getName(), ImmutableList.builder());
    }
    List<Map.Entry<String, ImmutableList.Builder<FieldElement>>> oneofs =
        new ArrayList<>(oneofsMap.entrySet());
    for (FieldDescriptorProto fd : descriptor.getFieldList()) {
      if (fd.hasOneofIndex()) {
        FieldElement field = toField(file, fd, true);
        oneofs.get(fd.getOneofIndex()).getValue().add(field);
      } else {
        FieldElement field = toField(file, fd, false);
        fields.add(field);
      }
    }
    for (DescriptorProto nestedDesc : descriptor.getNestedTypeList()) {
      MessageElement nestedMessage = toMessage(file, nestedDesc);
      nested.add(nestedMessage);
    }
    for (EnumDescriptorProto nestedDesc : descriptor.getEnumTypeList()) {
      EnumElement nestedEnum = toEnum(nestedDesc);
      nested.add(nestedEnum);
    }
    for (ReservedRange range : descriptor.getReservedRangeList()) {
      ReservedElement reservedElem = toReserved(range);
      reserved.add(reservedElem);
    }
    for (String reservedName : descriptor.getReservedNameList()) {
      ReservedElement reservedElem = new ReservedElement(
          DEFAULT_LOCATION,
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
    return new MessageElement(DEFAULT_LOCATION,
        name,
        "",
        nested.build(),
        options.build(),
        reserved.build(),
        fields.build(),
        oneofs.stream()
            .map(e -> toOneof(e.getKey(), e.getValue()))
            .collect(Collectors.toList()),
        Collections.emptyList(),
        Collections.emptyList()
    );
  }

  private ReservedElement toReserved(ReservedRange range) {
    List<Object> values = new ArrayList<>();
    int start = range.getStart();
    int end = range.getEnd();
    values.add(start == end ? start : new IntRange(start, end));
    return new ReservedElement(DEFAULT_LOCATION, "", values);
  }

  private OneOfElement toOneof(String name, ImmutableList.Builder<FieldElement> fields) {
    log.trace("*** oneof name: {}", name);
    // NOTE: skip groups
    return new OneOfElement(name, "", fields.build(), Collections.emptyList());
  }

  private EnumElement toEnum(EnumDescriptorProto ed) {
    String name = ed.getName();
    log.trace("*** enum name: {}", name);
    ImmutableList.Builder<EnumConstantElement> constants = ImmutableList.builder();
    for (EnumValueDescriptorProto ev : ed.getValueList()) {
      // NOTE: skip options
      constants.add(new EnumConstantElement(
          DEFAULT_LOCATION,
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
          "allow_alias",
          kind,
          ed.getOptions().getAllowAlias(),
          false
      );
      options.add(option);
    }
    // NOTE: skip some options
    return new EnumElement(DEFAULT_LOCATION, name, "", options.build(), constants.build());
  }

  private FieldElement toField(FileDescriptorProto file, FieldDescriptorProto fd, boolean inOneof) {
    String name = fd.getName();
    log.trace("*** field name: {}", name);
    ImmutableList.Builder<OptionElement> options = ImmutableList.builder();
    if (fd.getOptions().hasPacked()) {
      OptionElement.Kind kind = OptionElement.Kind.BOOLEAN;
      OptionElement option = new OptionElement("packed", kind, fd.getOptions().getPacked(), false);
      options.add(option);
    }
    if (fd.hasJsonName()) {
      OptionElement.Kind kind = OptionElement.Kind.STRING;
      OptionElement option = new OptionElement(
          "json_name",
          kind,
          fd.getJsonName(),
          false
      );
      options.add(option);
    }
    String defaultValue = fd.hasDefaultValue() && fd.getDefaultValue() != null
                          ? fd.getDefaultValue()
                          : null;
    // NOTE: skip some options
    return new FieldElement(DEFAULT_LOCATION,
        inOneof ? null : label(file, fd),
        dataType(fd),
        name,
        defaultValue,
        fd.getNumber(),
        "",
        options.build()
    );
  }

  private Field.Label label(FileDescriptorProto file, FieldDescriptorProto fd) {
    boolean isProto3 = file.getSyntax().equals(PROTO3);
    switch (fd.getLabel()) {
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

  private String dataType(FieldDescriptorProto field) {
    if (field.hasTypeName()) {
      return field.getTypeName();
    } else {
      FieldDescriptorProto.Type type = field.getType();
      return FieldDescriptor.Type.valueOf(type).name().toLowerCase();
    }
  }

  public Descriptor toDescriptor() {
    if (schemaObj == null) {
      return null;
    }
    if (descriptor == null) {
      descriptor = toDescriptor(name());
    }
    return descriptor;
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

  public Descriptors.EnumValueDescriptor getEnumValue(String enumTypeName, int enumNumber) {
    return toDynamicSchema().getEnumValue(enumTypeName, enumNumber);
  }

  private MessageElement firstMessage() {
    for (TypeElement typeElement : schemaObj.getTypes()) {
      if (typeElement instanceof MessageElement) {
        return (MessageElement) typeElement;
      }
    }
    throw new IllegalArgumentException("Protobuf schema definition"
        + " contains no message type definitions");
  }

  @VisibleForTesting
  protected DynamicSchema toDynamicSchema() {
    if (schemaObj == null) {
      return null;
    }
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
    log.trace("*** toDynamicSchema: {}", rootElem.toSchema());
    DynamicSchema.Builder schema = DynamicSchema.newBuilder();
    try {
      ProtoFile.Syntax syntax = rootElem.getSyntax();
      if (syntax != null) {
        schema.setSyntax(syntax.toString());
      }
      if (rootElem.getPackageName() != null) {
        schema.setPackage(rootElem.getPackageName());
      }
      for (TypeElement typeElem : rootElem.getTypes()) {
        if (typeElem instanceof MessageElement) {
          MessageDefinition message = toDynamicMessage((MessageElement) typeElem);
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
      String javaPackageName = findOption("java_package", rootElem.getOptions())
          .map(o -> o.getValue().toString()).orElse(null);
      if (javaPackageName != null) {
        schema.setJavaPackage(javaPackageName);
      }
      String javaOuterClassname = findOption("java_outer_classname", rootElem.getOptions())
          .map(o -> o.getValue().toString()).orElse(null);
      if (javaOuterClassname != null) {
        schema.setJavaOuterClassname(javaOuterClassname);
      }
      Boolean javaMultipleFiles = findOption("java_multiple_files", rootElem.getOptions())
          .map(o -> Boolean.valueOf(o.getValue().toString())).orElse(null);
      if (javaMultipleFiles != null) {
        schema.setJavaMultipleFiles(javaMultipleFiles);
      }
      schema.setName(name);
      return schema.build();
    } catch (Descriptors.DescriptorValidationException e) {
      throw new IllegalStateException(e);
    }
  }

  private static MessageDefinition toDynamicMessage(
      MessageElement messageElem
  ) {
    log.trace("*** message: {}", messageElem.getName());
    MessageDefinition.Builder message = MessageDefinition.newBuilder(messageElem.getName());
    for (TypeElement type : messageElem.getNestedTypes()) {
      if (type instanceof MessageElement) {
        message.addMessageDefinition(toDynamicMessage((MessageElement) type));
      } else if (type instanceof EnumElement) {
        message.addEnumDefinition(toDynamicEnum((EnumElement) type));
      }
    }
    Set<String> added = new HashSet<>();
    for (OneOfElement oneof : messageElem.getOneOfs()) {
      MessageDefinition.OneofBuilder oneofBuilder = message.addOneof(oneof.getName());
      for (FieldElement field : oneof.getFields()) {
        String defaultVal = field.getDefaultValue();
        String jsonName = findOption("json_name", field.getOptions())
            .map(o -> o.getValue().toString()).orElse(null);
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
    // Process fields after messages so that any newly created map entry messages are at the end
    for (FieldElement field : messageElem.getFields()) {
      if (added.contains(field.getName())) {
        continue;
      }
      Field.Label fieldLabel = field.getLabel();
      String label = fieldLabel != null ? fieldLabel.toString().toLowerCase() : null;
      String fieldType = field.getType();
      String defaultVal = field.getDefaultValue();
      String jsonName = findOption("json_name", field.getOptions())
          .map(o -> o.getValue().toString()).orElse(null);
      Boolean isPacked = findOption("packed", field.getOptions())
          .map(o -> Boolean.valueOf(o.getValue().toString())).orElse(null);
      ProtoType protoType = ProtoType.get(fieldType);
      ProtoType keyType = protoType.getKeyType();
      ProtoType valueType = protoType.getValueType();
      // Map fields are only permitted in messages
      if (protoType.isMap() && keyType != null && valueType != null) {
        label = "repeated";
        fieldType = toMapEntry(field.getName());
        MessageDefinition.Builder mapMessage = MessageDefinition.newBuilder(fieldType);
        mapMessage.setMapEntry(true);
        mapMessage.addField(null, keyType.getSimpleName(), KEY_FIELD, 1, null);
        mapMessage.addField(null, valueType.getSimpleName(), VALUE_FIELD, 2, null);
        message.addMessageDefinition(mapMessage.build());
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
    for (ReservedElement reserved : messageElem.getReserveds()) {
      for (Object elem : reserved.getValues()) {
        if (elem instanceof String) {
          message.addReservedName((String) elem);
        } else if (elem instanceof Integer) {
          int tag = (Integer) elem;
          message.addReservedRange(tag, tag);
        } else if (elem instanceof IntRange) {
          IntRange range = (IntRange) elem;
          message.addReservedRange(range.getStart(), range.getEndInclusive());
        } else {
          throw new IllegalStateException("Unsupported reserved type: " + elem.getClass()
              .getName());
        }
      }
    }
    Boolean isMapEntry = findOption("map_entry", messageElem.getOptions())
        .map(o -> Boolean.valueOf(o.getValue().toString())).orElse(null);
    if (isMapEntry != null) {
      message.setMapEntry(isMapEntry);
    }
    return message.build();
  }

  public static Optional<OptionElement> findOption(String name, List<OptionElement> options) {
    return options.stream().filter(o -> o.getName().equals(name)).findFirst();
  }

  private static EnumDefinition toDynamicEnum(EnumElement enumElem) {
    Boolean allowAlias = findOption("allow_alias", enumElem.getOptions())
        .map(o -> Boolean.valueOf(o.getValue().toString())).orElse(null);
    EnumDefinition.Builder enumer = EnumDefinition.newBuilder(enumElem.getName(), allowAlias);
    for (EnumConstantElement constant : enumElem.getConstants()) {
      enumer.addValue(constant.getName(), constant.getTag());
    }
    return enumer.build();
  }

  @Override
  public ProtoFileElement rawSchema() {
    return schemaObj;
  }

  @Override
  public String schemaType() {
    return TYPE;
  }

  @Override
  public String name() {
    return name != null ? name : firstMessage().getName();
  }

  @Override
  public String canonicalString() {
    if (schemaObj == null) {
      return null;
    }
    if (canonicalString == null) {
      // Remove comments, such as the location
      canonicalString = schemaObj.toSchema().replaceAll("//.*?\\n", "");
    }
    return canonicalString;
  }

  @Override
  public String formattedString(String format) {
    if (SERIALIZED_FORMAT.equals(format)) {
      FileDescriptorProto file = toDynamicSchema().getFileDescriptorProto();
      return base64Encoder.encodeToString(file.toByteArray());
    }
    throw new IllegalArgumentException("Unsupported format " + format);
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
  public void validate() {
    toDynamicSchema();
  }

  @Override
  public List<String> isBackwardCompatible(ParsedSchema previousSchema) {
    if (!schemaType().equals(previousSchema.schemaType())) {
      return Collections.singletonList("Incompatible because of different schema type");
    }
    final List<Difference> differences = SchemaDiff.compare(
        (ProtobufSchema) previousSchema, this
    );
    final List<Difference> incompatibleDiffs = differences.stream()
        .filter(diff -> !SchemaDiff.COMPATIBLE_CHANGES.contains(diff.getType()))
        .collect(Collectors.toList());
    boolean isCompatible = incompatibleDiffs.isEmpty();
    if (!isCompatible) {
      boolean first = true;
      List<String> errorMessages = new ArrayList<>();
      for (Difference incompatibleDiff : incompatibleDiffs) {
        if (first) {
          // Log first incompatible change as warning
          log.warn("Found incompatible change: {}", incompatibleDiff);
          errorMessages.add(String.format("Found incompatible change: {}", incompatibleDiff));
          first = false;
        } else {
          log.debug("Found incompatible change: {}", incompatibleDiff);
          errorMessages.add(String.format("Found incompatible change: {}", incompatibleDiff));
        }
      }
      return errorMessages;
    } else {
      return Collections.emptyList();
    }
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
    // Can't use schemaObj as locations may differ
    return Objects.equals(canonicalString(), that.canonicalString())
        && Objects.equals(references, that.references)
        && Objects.equals(version, that.version);
  }

  @Override
  public int hashCode() {
    if (hashCode == NO_HASHCODE) {
      // Can't use schemaObj as locations may differ
      hashCode = Objects.hash(canonicalString(), references, version);
    }
    return hashCode;
  }

  @Override
  public String toString() {
    return canonicalString();
  }

  public String fullName() {
    Descriptor descriptor = toDescriptor();
    FileDescriptor fd = descriptor.getFile();
    DescriptorProtos.FileOptions o = fd.getOptions();
    String p = o.hasJavaPackage() ? o.getJavaPackage() : fd.getPackage();
    String outer = "";
    if (!o.getJavaMultipleFiles()) {
      if (o.hasJavaOuterClassname()) {
        outer = o.getJavaOuterClassname();
      } else {
        // Can't determine full name without either java_outer_classname or java_multiple_files
        return null;
      }
    }
    StringBuilder inner = new StringBuilder();
    while (descriptor != null) {
      if (inner.length() == 0) {
        inner.insert(0, descriptor.getName());
      } else {
        inner.insert(0, descriptor.getName() + "$");
      }
      descriptor = descriptor.getContainingType();
    }
    String d1 = (!outer.isEmpty() || inner.length() != 0 ? "." : "");
    String d2 = (!outer.isEmpty() && inner.length() != 0 ? "$" : "");
    return p + d1 + outer + d2 + inner;
  }

  public MessageIndexes toMessageIndexes(String name) {
    List<Integer> indexes = new ArrayList<>();
    String[] parts = name.split("\\.");
    List<TypeElement> types = schemaObj.getTypes();
    for (String part : parts) {
      int i = 0;
      for (TypeElement type : types) {
        if (type instanceof MessageElement) {
          if (type.getName().equals(part)) {
            indexes.add(i);
            types = type.getNestedTypes();
            break;
          }
          i++;
        }
      }
    }
    return new MessageIndexes(indexes);
  }

  public String toMessageName(MessageIndexes indexes) {
    StringBuilder sb = new StringBuilder();
    List<TypeElement> types = schemaObj.getTypes();
    boolean first = true;
    for (Integer index : indexes.indexes()) {
      if (!first) {
        sb.append(".");
      } else {
        first = false;
      }
      MessageElement message = getMessageAtIndex(types, index);
      if (message == null) {
        throw new IllegalArgumentException("Invalid message indexes: " + indexes);
      }
      sb.append(message.getName());
      types = message.getNestedTypes();
    }
    return sb.toString();
  }

  private MessageElement getMessageAtIndex(List<TypeElement> types, int index) {
    int i = 0;
    for (TypeElement type : types) {
      if (type instanceof MessageElement) {
        if (index == i) {
          return (MessageElement) type;
        }
        i++;
      }
    }
    return null;
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
