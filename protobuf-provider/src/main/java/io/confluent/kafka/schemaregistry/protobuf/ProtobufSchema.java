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

import static com.google.common.base.CaseFormat.LOWER_UNDERSCORE;
import static com.google.common.base.CaseFormat.UPPER_CAMEL;
import static io.confluent.kafka.schemaregistry.protobuf.ProtobufSchemaUtils.findMatchingElement;
import static io.confluent.kafka.schemaregistry.protobuf.ProtobufSchemaUtils.findMatchingNode;
import static io.confluent.kafka.schemaregistry.protobuf.ProtobufSchemaUtils.jsonToFile;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.EnumHashBiMap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.protobuf.AnyProto;
import com.google.protobuf.ApiProto;
import com.google.protobuf.ByteString;
import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.DescriptorProtos.DescriptorProto;
import com.google.protobuf.DescriptorProtos.DescriptorProto.ExtensionRange;
import com.google.protobuf.DescriptorProtos.DescriptorProto.ReservedRange;
import com.google.protobuf.DescriptorProtos.EnumDescriptorProto;
import com.google.protobuf.DescriptorProtos.EnumDescriptorProto.EnumReservedRange;
import com.google.protobuf.DescriptorProtos.EnumValueDescriptorProto;
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto;
import com.google.protobuf.DescriptorProtos.FieldOptions.CType;
import com.google.protobuf.DescriptorProtos.FieldOptions.JSType;
import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import com.google.protobuf.DescriptorProtos.FileOptions.OptimizeMode;
import com.google.protobuf.DescriptorProtos.MethodDescriptorProto;
import com.google.protobuf.DescriptorProtos.MethodOptions.IdempotencyLevel;
import com.google.protobuf.DescriptorProtos.OneofDescriptorProto;
import com.google.protobuf.DescriptorProtos.ServiceDescriptorProto;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.EnumDescriptor;
import com.google.protobuf.Descriptors.EnumValueDescriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Descriptors.FieldDescriptor.Type;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.Descriptors.GenericDescriptor;
import com.google.protobuf.DurationProto;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.EmptyProto;
import com.google.protobuf.FieldMaskProto;
import com.google.protobuf.GeneratedMessageV3.ExtendableMessage;
import com.google.protobuf.Message;
import com.google.protobuf.SourceContextProto;
import com.google.protobuf.StructProto;
import com.google.protobuf.TimestampProto;
import com.google.protobuf.TypeProto;
import com.google.protobuf.WrappersProto;
import com.google.type.CalendarPeriodProto;
import com.google.type.ColorProto;
import com.google.type.DateProto;
import com.google.type.DateTimeProto;
import com.google.type.DayOfWeekProto;
import com.google.type.ExprProto;
import com.google.type.FractionProto;
import com.google.type.IntervalProto;
import com.google.type.LatLngProto;
import com.google.type.MoneyProto;
import com.google.type.MonthProto;
import com.google.type.PhoneNumberProto;
import com.google.type.PostalAddressProto;
import com.google.type.QuaternionProto;
import com.google.type.TimeOfDayProto;
import com.squareup.wire.Syntax;
import com.squareup.wire.schema.Field;
import com.squareup.wire.schema.Location;
import com.squareup.wire.schema.ProtoType;
import com.squareup.wire.schema.internal.parser.EnumConstantElement;
import com.squareup.wire.schema.internal.parser.EnumElement;
import com.squareup.wire.schema.internal.parser.ExtendElement;
import com.squareup.wire.schema.internal.parser.ExtensionsElement;
import com.squareup.wire.schema.internal.parser.FieldElement;
import com.squareup.wire.schema.internal.parser.MessageElement;
import com.squareup.wire.schema.internal.parser.OneOfElement;
import com.squareup.wire.schema.internal.parser.OptionElement;
import com.squareup.wire.schema.internal.parser.OptionElement.Kind;
import com.squareup.wire.schema.internal.parser.ProtoFileElement;
import com.squareup.wire.schema.internal.parser.ProtoParser;
import com.squareup.wire.schema.internal.parser.ReservedElement;
import com.squareup.wire.schema.internal.parser.RpcElement;
import com.squareup.wire.schema.internal.parser.ServiceElement;
import com.squareup.wire.schema.internal.parser.TypeElement;
import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.client.rest.entities.Metadata;
import io.confluent.kafka.schemaregistry.client.rest.entities.RuleKind;
import io.confluent.kafka.schemaregistry.client.rest.entities.RuleSet;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaEntity;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaReference;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchemaUtils.FormatContext;
import io.confluent.kafka.schemaregistry.protobuf.diff.Difference;
import io.confluent.kafka.schemaregistry.protobuf.diff.SchemaDiff;
import io.confluent.kafka.schemaregistry.protobuf.dynamic.DynamicSchema;
import io.confluent.kafka.schemaregistry.protobuf.dynamic.EnumDefinition;
import io.confluent.kafka.schemaregistry.protobuf.dynamic.MessageDefinition;
import io.confluent.kafka.schemaregistry.protobuf.dynamic.ServiceDefinition;
import io.confluent.kafka.schemaregistry.rules.FieldTransform;
import io.confluent.kafka.schemaregistry.rules.RuleConditionException;
import io.confluent.kafka.schemaregistry.rules.RuleContext;
import io.confluent.kafka.schemaregistry.rules.RuleContext.FieldContext;
import io.confluent.kafka.schemaregistry.rules.RuleException;
import io.confluent.kafka.schemaregistry.utils.JacksonMapper;
import io.confluent.protobuf.MetaProto;
import io.confluent.protobuf.MetaProto.Meta;
import io.confluent.protobuf.type.DecimalProto;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import kotlin.Pair;
import kotlin.ranges.IntRange;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProtobufSchema implements ParsedSchema {

  private static final Logger log = LoggerFactory.getLogger(ProtobufSchema.class);

  public static final String TYPE = "PROTOBUF";

  public static final String PROTO2 = "proto2";
  public static final String PROTO3 = "proto3";

  public static final String DOC_FIELD = "doc";
  public static final String PARAMS_FIELD = "params";
  public static final String TAGS_FIELD = "tags";
  public static final String PRECISION_KEY = "precision";
  public static final String SCALE_KEY = "scale";

  public static final String DEFAULT_NAME = "default";
  public static final String MAP_ENTRY_SUFFIX = "Entry";  // Suffix used by protoc
  public static final String KEY_FIELD = "key";
  public static final String VALUE_FIELD = "value";

  public static final String CONFLUENT_PREFIX = "confluent.";
  public static final String CONFLUENT_FILE_META = "confluent.file_meta";
  public static final String CONFLUENT_MESSAGE_META = "confluent.message_meta";
  public static final String CONFLUENT_FIELD_META = "confluent.field_meta";
  public static final String CONFLUENT_ENUM_META = "confluent.enum_meta";
  public static final String CONFLUENT_ENUM_VALUE_META = "confluent.enum_value_meta";

  private static final String JAVA_PACKAGE = "java_package";
  private static final String JAVA_OUTER_CLASSNAME = "java_outer_classname";
  private static final String JAVA_MULTIPLE_FILES = "java_multiple_files";
  private static final String JAVA_GENERATE_EQUALS_AND_HASH = "java_generate_equals_and_hash";
  private static final String JAVA_STRING_CHECK_UTF8 = "java_string_check_utf8";
  private static final String OPTIMIZE_FOR = "optimize_for";
  private static final String GO_PACKAGE = "go_package";
  private static final String CC_GENERIC_SERVICES = "cc_generic_services";
  private static final String JAVA_GENERIC_SERVICES = "java_generic_services";
  private static final String PY_GENERIC_SERVICES = "py_generic_services";
  private static final String PHP_GENERIC_SERVICES = "php_generic_services";
  private static final String DEPRECATED = "deprecated";
  private static final String CC_ENABLE_ARENAS = "cc_enable_arenas";
  private static final String OBJC_CLASS_PREFIX = "objc_class_prefix";
  private static final String CSHARP_NAMESPACE = "csharp_namespace";
  private static final String SWIFT_PREFIX = "swift_prefix";
  private static final String PHP_CLASS_PREFIX = "php_class_prefix";
  private static final String PHP_NAMESPACE = "php_namespace";
  private static final String PHP_METADATA_NAMESPACE = "php_metadata_namespace";
  private static final String RUBY_PACKAGE = "ruby_package";

  private static final String NO_STANDARD_DESCRIPTOR_ACCESSOR = "no_standard_descriptor_accessor";
  private static final String MAP_ENTRY = "map_entry";

  private static final String CTYPE = "ctype";
  private static final String PACKED = "packed";
  private static final String JSTYPE = "jstype";

  private static final String ALLOW_ALIAS = "allow_alias";

  private static final String IDEMPOTENCY_LEVEL = "idempotency_level";

  public static final Location DEFAULT_LOCATION = Location.get("");

  public static final String CFLT_META_LOCATION = "confluent/meta.proto";
  public static final String CFLT_DECIMAL_LOCATION = "confluent/type/decimal.proto";
  public static final String CALENDAR_PERIOD_LOCATION = "google/type/calendar_period.proto";
  public static final String COLOR_LOCATION = "google/type/color.proto";
  public static final String DATE_LOCATION = "google/type/date.proto";
  public static final String DATETIME_LOCATION = "google/type/datetime.proto";
  public static final String DAY_OF_WEEK_LOCATION = "google/type/dayofweek.proto";
  public static final String DECIMAL_LOCATION = "google/type/decimal.proto";
  public static final String EXPR_LOCATION = "google/type/expr.proto";
  public static final String FRACTION_LOCATION = "google/type/fraction.proto";
  public static final String INTERVAL_LOCATION = "google/type/interval.proto";
  public static final String LATLNG_LOCATION = "google/type/latlng.proto";
  public static final String MONEY_LOCATION = "google/type/money.proto";
  public static final String MONTH_LOCATION = "google/type/month.proto";
  public static final String PHONE_NUMBER_LOCATION = "google/type/phone_number.proto";
  public static final String POSTAL_ADDRESS_LOCATION = "google/type/postal_address.proto";
  public static final String QUATERNION_LOCATION = "google/type/quaternion.proto";
  public static final String TIME_OF_DAY_LOCATION = "google/type/timeofday.proto";
  public static final String ANY_LOCATION = "google/protobuf/any.proto";
  public static final String API_LOCATION = "google/protobuf/api.proto";
  public static final String DESCRIPTOR_LOCATION = "google/protobuf/descriptor.proto";
  public static final String DURATION_LOCATION = "google/protobuf/duration.proto";
  public static final String EMPTY_LOCATION = "google/protobuf/empty.proto";
  public static final String FIELD_MASK_LOCATION = "google/protobuf/field_mask.proto";
  public static final String SOURCE_CONTEXT_LOCATION = "google/protobuf/source_context.proto";
  public static final String STRUCT_LOCATION = "google/protobuf/struct.proto";
  public static final String TIMESTAMP_LOCATION = "google/protobuf/timestamp.proto";
  public static final String TYPE_LOCATION = "google/protobuf/type.proto";
  public static final String WRAPPER_LOCATION = "google/protobuf/wrappers.proto";

  private static final ProtoFileElement CFLT_META_SCHEMA =
      toProtoFile(MetaProto.getDescriptor().toProto()) ;
  private static final ProtoFileElement CFLT_DECIMAL_SCHEMA =
      toProtoFile(DecimalProto.getDescriptor().toProto()) ;
  private static final ProtoFileElement CALENDAR_PERIOD_SCHEMA =
      toProtoFile(CalendarPeriodProto.getDescriptor().toProto()) ;
  private static final ProtoFileElement COLOR_SCHEMA =
      toProtoFile(ColorProto.getDescriptor().toProto()) ;
  private static final ProtoFileElement DATE_SCHEMA =
      toProtoFile(DateProto.getDescriptor().toProto()) ;
  private static final ProtoFileElement DATETIME_SCHEMA =
      toProtoFile(DateTimeProto.getDescriptor().toProto()) ;
  private static final ProtoFileElement DAY_OF_WEEK_SCHEMA =
      toProtoFile(DayOfWeekProto.getDescriptor().toProto()) ;
  private static final ProtoFileElement DECIMAL_SCHEMA =
      toProtoFile(com.google.type.DecimalProto.getDescriptor().toProto()) ;
  private static final ProtoFileElement EXPR_SCHEMA =
      toProtoFile(ExprProto.getDescriptor().toProto()) ;
  private static final ProtoFileElement FRACTION_SCHEMA =
      toProtoFile(FractionProto.getDescriptor().toProto()) ;
  private static final ProtoFileElement INTERVAL_SCHEMA =
      toProtoFile(IntervalProto.getDescriptor().toProto()) ;
  private static final ProtoFileElement LATLNG_SCHEMA =
      toProtoFile(LatLngProto.getDescriptor().toProto()) ;
  private static final ProtoFileElement MONEY_SCHEMA =
      toProtoFile(MoneyProto.getDescriptor().toProto()) ;
  private static final ProtoFileElement MONTH_SCHEMA =
      toProtoFile(MonthProto.getDescriptor().toProto()) ;
  private static final ProtoFileElement PHONE_NUMBER_SCHEMA =
      toProtoFile(PhoneNumberProto.getDescriptor().toProto()) ;
  private static final ProtoFileElement POSTAL_ADDRESS_SCHEMA =
      toProtoFile(PostalAddressProto.getDescriptor().toProto()) ;
  private static final ProtoFileElement QUATERNION_SCHEMA =
      toProtoFile(QuaternionProto.getDescriptor().toProto()) ;
  private static final ProtoFileElement TIME_OF_DAY_SCHEMA =
      toProtoFile(TimeOfDayProto.getDescriptor().toProto()) ;
  private static final ProtoFileElement ANY_SCHEMA =
      toProtoFile(AnyProto.getDescriptor().toProto()) ;
  private static final ProtoFileElement API_SCHEMA =
      toProtoFile(ApiProto.getDescriptor().toProto()) ;
  private static final ProtoFileElement DESCRIPTOR_SCHEMA =
      toProtoFile(DescriptorProtos.getDescriptor().toProto()) ;
  private static final ProtoFileElement DURATION_SCHEMA =
      toProtoFile(DurationProto.getDescriptor().toProto()) ;
  private static final ProtoFileElement EMPTY_SCHEMA =
      toProtoFile(EmptyProto.getDescriptor().toProto()) ;
  private static final ProtoFileElement FIELD_MASK_SCHEMA =
      toProtoFile(FieldMaskProto.getDescriptor().toProto()) ;
  private static final ProtoFileElement SOURCE_CONTEXT_SCHEMA =
      toProtoFile(SourceContextProto.getDescriptor().toProto()) ;
  private static final ProtoFileElement STRUCT_SCHEMA =
      toProtoFile(StructProto.getDescriptor().toProto()) ;
  private static final ProtoFileElement TIMESTAMP_SCHEMA =
      toProtoFile(TimestampProto.getDescriptor().toProto()) ;
  private static final ProtoFileElement TYPE_SCHEMA =
      toProtoFile(TypeProto.getDescriptor().toProto()) ;
  private static final ProtoFileElement WRAPPER_SCHEMA =
      toProtoFile(WrappersProto.getDescriptor().toProto()) ;

  private static final HashMap<String, ProtoFileElement> KNOWN_DEPENDENCIES;

  static {
    KNOWN_DEPENDENCIES = new HashMap<>();
    KNOWN_DEPENDENCIES.put(CFLT_META_LOCATION, CFLT_META_SCHEMA);
    KNOWN_DEPENDENCIES.put(CFLT_DECIMAL_LOCATION, CFLT_DECIMAL_SCHEMA);
    KNOWN_DEPENDENCIES.put(CALENDAR_PERIOD_LOCATION, CALENDAR_PERIOD_SCHEMA);
    KNOWN_DEPENDENCIES.put(COLOR_LOCATION, COLOR_SCHEMA);
    KNOWN_DEPENDENCIES.put(DATE_LOCATION, DATE_SCHEMA);
    KNOWN_DEPENDENCIES.put(DATETIME_LOCATION, DATETIME_SCHEMA);
    KNOWN_DEPENDENCIES.put(DAY_OF_WEEK_LOCATION, DAY_OF_WEEK_SCHEMA);
    KNOWN_DEPENDENCIES.put(DECIMAL_LOCATION, DECIMAL_SCHEMA);
    KNOWN_DEPENDENCIES.put(EXPR_LOCATION, EXPR_SCHEMA);
    KNOWN_DEPENDENCIES.put(FRACTION_LOCATION, FRACTION_SCHEMA);
    KNOWN_DEPENDENCIES.put(INTERVAL_LOCATION, INTERVAL_SCHEMA);
    KNOWN_DEPENDENCIES.put(LATLNG_LOCATION, LATLNG_SCHEMA);
    KNOWN_DEPENDENCIES.put(MONEY_LOCATION, MONEY_SCHEMA);
    KNOWN_DEPENDENCIES.put(MONTH_LOCATION, MONTH_SCHEMA);
    KNOWN_DEPENDENCIES.put(PHONE_NUMBER_LOCATION, PHONE_NUMBER_SCHEMA);
    KNOWN_DEPENDENCIES.put(POSTAL_ADDRESS_LOCATION, POSTAL_ADDRESS_SCHEMA);
    KNOWN_DEPENDENCIES.put(QUATERNION_LOCATION, QUATERNION_SCHEMA);
    KNOWN_DEPENDENCIES.put(TIME_OF_DAY_LOCATION, TIME_OF_DAY_SCHEMA);
    KNOWN_DEPENDENCIES.put(ANY_LOCATION, ANY_SCHEMA);
    KNOWN_DEPENDENCIES.put(API_LOCATION, API_SCHEMA);
    KNOWN_DEPENDENCIES.put(DESCRIPTOR_LOCATION, DESCRIPTOR_SCHEMA);
    KNOWN_DEPENDENCIES.put(DURATION_LOCATION, DURATION_SCHEMA);
    KNOWN_DEPENDENCIES.put(EMPTY_LOCATION, EMPTY_SCHEMA);
    KNOWN_DEPENDENCIES.put(FIELD_MASK_LOCATION, FIELD_MASK_SCHEMA);
    KNOWN_DEPENDENCIES.put(SOURCE_CONTEXT_LOCATION, SOURCE_CONTEXT_SCHEMA);
    KNOWN_DEPENDENCIES.put(STRUCT_LOCATION, STRUCT_SCHEMA);
    KNOWN_DEPENDENCIES.put(TIMESTAMP_LOCATION, TIMESTAMP_SCHEMA);
    KNOWN_DEPENDENCIES.put(TYPE_LOCATION, TYPE_SCHEMA);
    KNOWN_DEPENDENCIES.put(WRAPPER_LOCATION, WRAPPER_SCHEMA);
  }

  public static Set<String> knownTypes() {
    return KNOWN_DEPENDENCIES.keySet();
  }

  private final ProtoFileElement schemaObj;

  private final Integer version;

  private final String name;

  private final List<SchemaReference> references;

  private final Map<String, ProtoFileElement> dependencies;

  private final Metadata metadata;

  private final RuleSet ruleSet;

  private transient String canonicalString;

  private transient DynamicSchema dynamicSchema;

  private transient Descriptor descriptor;

  private transient int hashCode = NO_HASHCODE;

  private static final int NO_HASHCODE = Integer.MIN_VALUE;

  private static final Base64.Encoder base64Encoder = Base64.getEncoder();

  private static final Base64.Decoder base64Decoder = Base64.getDecoder();

  private static final ObjectMapper jsonMapper = JacksonMapper.INSTANCE;

  private static volatile Method extensionFields;

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
    this(schemaString, references, resolvedReferences, null, null, version, name);
  }

  public ProtobufSchema(
      String schemaString,
      List<SchemaReference> references,
      Map<String, String> resolvedReferences,
      Metadata metadata,
      RuleSet ruleSet,
      Integer version,
      String name
  ) {
    try {
      this.schemaObj = schemaString != null ? toProtoFile(schemaString) : null;
      this.version = version;
      this.name = name;
      this.references = Collections.unmodifiableList(references);
      this.dependencies = Collections.unmodifiableMap(resolvedReferences.entrySet()
          .stream()
          .collect(Collectors.toMap(
              Map.Entry::getKey,
              e -> toProtoFile(e.getValue())
          )));
      this.metadata = metadata;
      this.ruleSet = ruleSet;
    } catch (IllegalStateException e) {
      log.error("Could not parse Protobuf schema {} with references {}", schemaString,
          references, e);
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
    this.metadata = null;
    this.ruleSet = null;
  }

  public ProtobufSchema(Descriptor descriptor) {
    this(descriptor, Collections.emptyList());
  }

  public ProtobufSchema(Descriptor descriptor, List<SchemaReference> references) {
    Map<String, ProtoFileElement> dependencies = new HashMap<>();
    this.schemaObj = toProtoFile(descriptor.getFile(), dependencies);
    this.version = null;
    this.name = descriptor.getFullName();
    this.references = Collections.unmodifiableList(references);
    this.dependencies = Collections.unmodifiableMap(dependencies);
    this.metadata = null;
    this.ruleSet = null;
    this.descriptor = descriptor;
  }

  public ProtobufSchema(EnumDescriptor enumDescriptor) {
    this(enumDescriptor, Collections.emptyList());
  }

  public ProtobufSchema(EnumDescriptor enumDescriptor, List<SchemaReference> references) {
    Map<String, ProtoFileElement> dependencies = new HashMap<>();
    this.schemaObj = toProtoFile(enumDescriptor.getFile(), dependencies);
    this.version = null;
    this.name = enumDescriptor.getFullName();
    this.references = Collections.unmodifiableList(references);
    this.dependencies = Collections.unmodifiableMap(dependencies);
    this.metadata = null;
    this.ruleSet = null;
    this.descriptor = null;
  }

  public ProtobufSchema(FileDescriptor fileDescriptor) {
    this(fileDescriptor, Collections.emptyList());
  }

  public ProtobufSchema(FileDescriptor fileDescriptor, List<SchemaReference> references) {
    Map<String, ProtoFileElement> dependencies = new HashMap<>();
    this.schemaObj = toProtoFile(fileDescriptor, dependencies);
    this.version = null;
    this.name = null;
    this.references = Collections.unmodifiableList(references);
    this.dependencies = Collections.unmodifiableMap(dependencies);
    this.metadata = null;
    this.ruleSet = null;
    this.descriptor = null;
  }

  private ProtobufSchema(
      ProtoFileElement schemaObj,
      Integer version,
      String name,
      List<SchemaReference> references,
      Map<String, ProtoFileElement> dependencies,
      Metadata metadata,
      RuleSet ruleSet,
      String canonicalString,
      DynamicSchema dynamicSchema,
      Descriptor descriptor
  ) {
    this.schemaObj = schemaObj;
    this.version = version;
    this.name = name;
    this.references = references;
    this.dependencies = dependencies;
    this.metadata = metadata;
    this.ruleSet = ruleSet;
    this.canonicalString = canonicalString;
    this.dynamicSchema = dynamicSchema;
    this.descriptor = descriptor;
  }

  @Override
  public ProtobufSchema copy() {
    return new ProtobufSchema(
        this.schemaObj,
        this.version,
        this.name,
        this.references,
        this.dependencies,
        this.metadata,
        this.ruleSet,
        this.canonicalString,
        this.dynamicSchema,
        this.descriptor
    );
  }

  @Override
  public ProtobufSchema copy(Integer version) {
    return new ProtobufSchema(
        this.schemaObj,
        version,
        this.name,
        this.references,
        this.dependencies,
        this.metadata,
        this.ruleSet,
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
        this.metadata,
        this.ruleSet,
        this.canonicalString,
        this.dynamicSchema,
        // reset descriptor if names not equal
        Objects.equals(this.name, name) ? this.descriptor : null
    );
  }

  public ProtobufSchema copy(List<SchemaReference> references) {
    return copy(references, this.dependencies);
  }

  public ProtobufSchema copy(
      List<SchemaReference> references, Map<String, ProtoFileElement> dependencies) {
    return new ProtobufSchema(
        this.schemaObj,
        this.version,
        this.name,
        references,
        dependencies,
        this.metadata,
        this.ruleSet,
        this.canonicalString,
        this.dynamicSchema,
        this.descriptor
    );
  }

  @Override
  public ProtobufSchema copy(Metadata metadata, RuleSet ruleSet) {
    return new ProtobufSchema(
        this.schemaObj,
        this.version,
        this.name,
        this.references,
        this.dependencies,
        metadata,
        ruleSet,
        this.canonicalString,
        this.dynamicSchema,
        this.descriptor
    );
  }

  @Override
  public ParsedSchema copy(Map<SchemaEntity, Set<String>> tagsToAdd,
                           Map<SchemaEntity, Set<String>> tagsToRemove) {
    ProtobufSchema schemaCopy = this.copy();
    JsonNode original = jsonMapper.valueToTree(schemaCopy.rawSchema());
    modifySchemaTags(schemaCopy.rawSchema(), original, tagsToAdd, tagsToRemove);
    try {
      ProtoFileElement newFileElement = jsonToFile(original);
      return new ProtobufSchema(newFileElement.toSchema(),
        schemaCopy.references(),
        schemaCopy.dependencies().entrySet().stream().collect(
            Collectors.toMap(
              Map.Entry::getKey,
              e -> e.getValue().toSchema())),
        schemaCopy.metadata(),
        schemaCopy.ruleSet(),
        schemaCopy.version(),
        schemaCopy.name()
      );
    } catch (JsonProcessingException e) {
      throw new IllegalStateException("Cannot deserialize json into ProtoFileElement", e);
    }
  }

  public ProtobufSchema copyWithSchema(String schema) {
    return new ProtobufSchema(
        toProtoFile(schema),
        this.version,
        this.name,
        references,
        this.dependencies,
        this.metadata,
        this.ruleSet,
        schema,
        null,
        null
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
        throw new IllegalArgumentException("Could not parse Protobuf - " + e.getMessage(), e);
      }
    }
  }

  private static ProtoFileElement toProtoFile(
      FileDescriptor file, Map<String, ProtoFileElement> dependencies
  ) {
    for (FileDescriptor dependency : file.getDependencies()) {
      String depName = dependency.getName();
      dependencies.put(depName, toProtoFile(dependency, dependencies));
    }
    return toProtoFile(file.toProto());
  }

  private static ProtoFileElement toProtoFile(FileDescriptorProto file) {
    String packageName = file.getPackage();
    // Don't set empty package name
    if (packageName.isEmpty()) {
      packageName = null;
    }
    Syntax syntax = null;
    switch (file.getSyntax()) {
      case PROTO2:
        syntax = Syntax.PROTO_2;
        break;
      case PROTO3:
        syntax = Syntax.PROTO_3;
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
    ImmutableList.Builder<ServiceElement> services = ImmutableList.builder();
    for (ServiceDescriptorProto sd : file.getServiceList()) {
      ServiceElement service = toService(sd);
      services.add(service);
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
      options.add(new OptionElement(
          JAVA_PACKAGE, Kind.STRING, file.getOptions().getJavaPackage(), false));
    }
    if (file.getOptions().hasJavaOuterClassname()) {
      options.add(new OptionElement(
          JAVA_OUTER_CLASSNAME, Kind.STRING, file.getOptions().getJavaOuterClassname(), false));
    }
    if (file.getOptions().hasJavaMultipleFiles()) {
      options.add(new OptionElement(
          JAVA_MULTIPLE_FILES, Kind.BOOLEAN, file.getOptions().getJavaMultipleFiles(), false));
    }
    if (file.getOptions().hasJavaGenerateEqualsAndHash()) {
      options.add(new OptionElement(
          JAVA_GENERATE_EQUALS_AND_HASH, Kind.BOOLEAN,
          file.getOptions().getJavaGenerateEqualsAndHash(), false));
    }
    if (file.getOptions().hasJavaStringCheckUtf8()) {
      options.add(new OptionElement(
          JAVA_STRING_CHECK_UTF8, Kind.BOOLEAN, file.getOptions().getJavaStringCheckUtf8(), false));
    }
    if (file.getOptions().hasOptimizeFor()) {
      options.add(new OptionElement(
          OPTIMIZE_FOR, Kind.ENUM, file.getOptions().getOptimizeFor(), false));
    }
    if (file.getOptions().hasGoPackage()) {
      options.add(new OptionElement(
          GO_PACKAGE, Kind.STRING, file.getOptions().getGoPackage(), false));
    }
    if (file.getOptions().hasCcGenericServices()) {
      options.add(new OptionElement(
          CC_GENERIC_SERVICES, Kind.BOOLEAN, file.getOptions().getCcGenericServices(), false));
    }
    if (file.getOptions().hasJavaGenericServices()) {
      options.add(new OptionElement(
          JAVA_GENERIC_SERVICES, Kind.BOOLEAN, file.getOptions().getJavaGenericServices(), false));
    }
    if (file.getOptions().hasPyGenericServices()) {
      options.add(new OptionElement(
          PY_GENERIC_SERVICES, Kind.BOOLEAN, file.getOptions().getPyGenericServices(), false));
    }
    if (file.getOptions().hasPhpGenericServices()) {
      options.add(new OptionElement(
          PHP_GENERIC_SERVICES, Kind.BOOLEAN, file.getOptions().getPhpGenericServices(), false));
    }
    if (file.getOptions().hasDeprecated()) {
      options.add(new OptionElement(
          DEPRECATED, Kind.BOOLEAN, file.getOptions().getDeprecated(), false));
    }
    if (file.getOptions().hasCcEnableArenas()) {
      options.add(new OptionElement(
          CC_ENABLE_ARENAS, Kind.BOOLEAN, file.getOptions().getCcEnableArenas(), false));
    }
    if (file.getOptions().hasObjcClassPrefix()) {
      options.add(new OptionElement(
          OBJC_CLASS_PREFIX, Kind.STRING, file.getOptions().getObjcClassPrefix(), false));
    }
    if (file.getOptions().hasCsharpNamespace()) {
      options.add(new OptionElement(
          CSHARP_NAMESPACE, Kind.STRING, file.getOptions().getCsharpNamespace(), false));
    }
    if (file.getOptions().hasSwiftPrefix()) {
      options.add(new OptionElement(
          SWIFT_PREFIX, Kind.STRING, file.getOptions().getSwiftPrefix(), false));
    }
    if (file.getOptions().hasPhpClassPrefix()) {
      options.add(new OptionElement(
          PHP_CLASS_PREFIX, Kind.STRING, file.getOptions().getPhpClassPrefix(), false));
    }
    if (file.getOptions().hasPhpNamespace()) {
      options.add(new OptionElement(
          PHP_NAMESPACE, Kind.STRING, file.getOptions().getPhpNamespace(), false));
    }
    if (file.getOptions().hasPhpMetadataNamespace()) {
      options.add(new OptionElement(
          PHP_METADATA_NAMESPACE, Kind.STRING, file.getOptions().getPhpMetadataNamespace(), false));
    }
    if (file.getOptions().hasRubyPackage()) {
      options.add(new OptionElement(
          RUBY_PACKAGE, Kind.STRING, file.getOptions().getRubyPackage(), false));
    }
    if (file.getOptions().hasExtension(MetaProto.fileMeta)) {
      Meta meta = file.getOptions().getExtension(MetaProto.fileMeta);
      OptionElement option = toOption(CONFLUENT_FILE_META, meta);
      if (option != null) {
        options.add(option);
      }
    }
    options.addAll(toCustomOptions(file.getOptions()));
    ImmutableList.Builder<ExtendElement> extendElements =
        toExtendElements(file, file.getExtensionList());
    return new ProtoFileElement(DEFAULT_LOCATION,
        packageName,
        syntax,
        imports.build(),
        publicImports.build(),
        types.build(),
        services.build(),
        extendElements.build(),
        options.build()
    );
  }

  private static ImmutableList.Builder<ExtendElement> toExtendElements(
      FileDescriptorProto file, List<FieldDescriptorProto> fields) {
    Map<String, ImmutableList.Builder<FieldElement>> extendFieldElements = new LinkedHashMap<>();
    for (FieldDescriptorProto fd : fields) {
      // Note that the extendee is a fully qualified name
      ImmutableList.Builder<FieldElement> extendFields = extendFieldElements.computeIfAbsent(
          fd.getExtendee(), k -> ImmutableList.builder());
      extendFields.add(toField(file, fd, false));
    }
    ImmutableList.Builder<ExtendElement> extendElements = ImmutableList.builder();
    for (Map.Entry<String, ImmutableList.Builder<FieldElement>> extendFieldElement :
        extendFieldElements.entrySet()) {
      extendElements.add(new ExtendElement(DEFAULT_LOCATION,
          extendFieldElement.getKey(), "", extendFieldElement.getValue().build()));
    }
    return extendElements;
  }

  private static List<OptionElement> toCustomOptions(ExtendableMessage<?> options) {
    // Uncomment this in case the getExtensionFields method is deprecated
    //return options.getAllFields().entrySet().stream()
    return getExtensionFields(options).entrySet().stream()
        .filter(e -> e.getKey().isExtension()
            && !e.getKey().getFullName().startsWith(CONFLUENT_PREFIX))
        .flatMap(e -> toOptionElements(e.getKey().getFullName(), e.getValue()))
        .collect(Collectors.toList());
  }

  @SuppressWarnings("unchecked")
  private static Map<Descriptors.FieldDescriptor, Object> getExtensionFields(
      ExtendableMessage<?> options) {
    // We use reflection to access getExtensionFields as an optimization over calling getAllFields
    try {
      if (extensionFields == null) {
        synchronized (ProtobufSchema.class) {
          if (extensionFields == null) {
            extensionFields = ExtendableMessage.class.getDeclaredMethod("getExtensionFields");
          }
        }
        extensionFields.setAccessible(true);
      }
      return (Map<Descriptors.FieldDescriptor, Object>) extensionFields.invoke(options);
    } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
      throw new RuntimeException(e);
    }
  }

  private static Stream<OptionElement> toOptionElements(String name, Object value) {
    if (value instanceof List) {
      return ((List<?>) value).stream().map(v -> toOptionElement(name, v));
    } else {
      return Stream.of(toOptionElement(name, value));
    }
  }

  private static OptionElement toOptionElement(String name, Object value) {
    return new OptionElement(name, toKind(value), toOptionValue(value, false), true);
  }

  private static Kind toKind(Object value) {
    if (value instanceof String) {
      return Kind.STRING;
    } else if (value instanceof Boolean) {
      return Kind.BOOLEAN;
    } else if (value instanceof Number) {
      return Kind.NUMBER;
    } else if (value instanceof Enum || value instanceof EnumValueDescriptor) {
      return Kind.ENUM;
    } else if (value instanceof List) {
      return Kind.LIST;
    } else if (value instanceof Message) {
      return Kind.MAP;
    } else {
      throw new IllegalArgumentException("Unsupported option type " + value.getClass().getName());
    }
  }

  private static Object toOptionValue(Object value, boolean isMapValue) {
    if (value instanceof List) {
      return ((List<?>) value).stream()
          .map(o -> toOptionValue(o, false))
          .collect(Collectors.toList());
    } else if (value instanceof Message) {
      return toOptionMap((Message) value);
    } else {
      if (isMapValue) {
        if (value instanceof Boolean) {
          return new OptionElement.OptionPrimitive(Kind.BOOLEAN, value);
        } else if (value instanceof Enum) {
          return new OptionElement.OptionPrimitive(Kind.ENUM, value);
        } else if (value instanceof Number) {
          return new OptionElement.OptionPrimitive(Kind.NUMBER, value);
        }
      }
      return value;
    }
  }

  private static Map<String, Object> toOptionMap(Message message) {
    return message.getAllFields().entrySet().stream()
        .map(e -> new Pair<>(toOptionMapKey(e.getKey()), toOptionValue(e.getValue(), true)))
        .filter(p -> p.getSecond() != null)
        .collect(Collectors.toMap(Pair::getFirst, Pair::getSecond,
            (e1, e2) -> e1, LinkedHashMap::new));
  }

  private static String toOptionMapKey(FieldDescriptor field) {
    return field.isExtension() ? "[" + field.getFullName() + "]" : field.getName();
  }

  private static MessageElement toMessage(FileDescriptorProto file, DescriptorProto descriptor) {
    String name = descriptor.getName();
    log.trace("*** msg name: {}", name);
    ImmutableList.Builder<FieldElement> fields = ImmutableList.builder();
    ImmutableList.Builder<TypeElement> nested = ImmutableList.builder();
    ImmutableList.Builder<ReservedElement> reserved = ImmutableList.builder();
    ImmutableList.Builder<ExtensionsElement> extensions = ImmutableList.builder();
    LinkedHashMap<String, ImmutableList.Builder<FieldElement>> oneofsMap = new LinkedHashMap<>();
    for (OneofDescriptorProto od : descriptor.getOneofDeclList()) {
      oneofsMap.put(od.getName(), ImmutableList.builder());
    }
    List<Map.Entry<String, ImmutableList.Builder<FieldElement>>> oneofs =
        new ArrayList<>(oneofsMap.entrySet());
    for (FieldDescriptorProto fd : descriptor.getFieldList()) {
      if (fd.hasOneofIndex() && !fd.getProto3Optional()) {
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
    for (ExtensionRange extensionRange : descriptor.getExtensionRangeList()) {
      ExtensionsElement extension = toExtension(extensionRange);
      extensions.add(extension);
    }
    ImmutableList.Builder<OptionElement> options = ImmutableList.builder();
    if (descriptor.getOptions().hasNoStandardDescriptorAccessor()) {
      OptionElement option = new OptionElement(
          NO_STANDARD_DESCRIPTOR_ACCESSOR, Kind.BOOLEAN,
          descriptor.getOptions().getNoStandardDescriptorAccessor(), false
      );
      options.add(option);
    }
    if (descriptor.getOptions().hasDeprecated()) {
      OptionElement option = new OptionElement(
          DEPRECATED, Kind.BOOLEAN,
          descriptor.getOptions().getDeprecated(), false
      );
      options.add(option);
    }
    if (descriptor.getOptions().hasMapEntry()) {
      OptionElement option = new OptionElement(
          MAP_ENTRY, Kind.BOOLEAN,
          descriptor.getOptions().getMapEntry(), false
      );
      options.add(option);
    }
    if (descriptor.getOptions().hasExtension(MetaProto.messageMeta)) {
      Meta meta = descriptor.getOptions().getExtension(MetaProto.messageMeta);
      OptionElement option = toOption(CONFLUENT_MESSAGE_META, meta);
      if (option != null) {
        options.add(option);
      }
    }
    options.addAll(toCustomOptions(descriptor.getOptions()));
    ImmutableList.Builder<ExtendElement> extendElements =
        toExtendElements(file, descriptor.getExtensionList());
    // NOTE: skip groups
    return new MessageElement(DEFAULT_LOCATION,
        name,
        "",
        nested.build(),
        options.build(),
        reserved.build(),
        fields.build(),
        oneofs.stream()
            .map(e -> toOneof(e.getKey(), e.getValue()))
            .filter(e -> !e.getFields().isEmpty())
            .collect(Collectors.toList()),
        extensions.build(),
        Collections.emptyList(),
        extendElements.build()
    );
  }

  private static OptionElement toOption(String name, Meta meta) {
    Map<String, Object> map = new LinkedHashMap<>();
    String doc = meta.getDoc();
    if (!doc.isEmpty()) {
      map.put(DOC_FIELD, doc);
    }
    Map<String, String> params = meta.getParamsMap();
    if (!params.isEmpty()) {
      List<Map<String, String>> keyValues = new ArrayList<>();
      for (Map.Entry<String, String> entry : params.entrySet()) {
        Map<String, String> keyValue = new LinkedHashMap<>();
        String key = entry.getKey();
        if (PRECISION_KEY.equals(key) || SCALE_KEY.equals(key)) {
          // For backward compatibility, we emit the value first
          keyValue.put(VALUE_FIELD, entry.getValue());
          keyValue.put(KEY_FIELD, key);
        } else {
          keyValue.put(KEY_FIELD, key);
          keyValue.put(VALUE_FIELD, entry.getValue());
        }
        keyValues.add(keyValue);
      }
      map.put(PARAMS_FIELD, keyValues);
    }
    List<String> tags = meta.getTagsList();
    if (!tags.isEmpty()) {
      map.put(TAGS_FIELD, tags);
    }
    return map.isEmpty() ? null : new OptionElement(name, Kind.MAP, map, true);
  }

  private static OneOfElement toOneof(String name, ImmutableList.Builder<FieldElement> fields) {
    log.trace("*** oneof name: {}", name);
    // NOTE: skip groups
    return new OneOfElement(name, "", fields.build(),
        Collections.emptyList(), Collections.emptyList(), DEFAULT_LOCATION);
  }

  private static EnumElement toEnum(EnumDescriptorProto ed) {
    String name = ed.getName();
    log.trace("*** enum name: {}", name);
    ImmutableList.Builder<EnumConstantElement> constants = ImmutableList.builder();
    for (EnumValueDescriptorProto ev : ed.getValueList()) {
      ImmutableList.Builder<OptionElement> options = ImmutableList.builder();
      if (ev.getOptions().hasDeprecated()) {
        OptionElement option = new OptionElement(
            DEPRECATED, Kind.BOOLEAN,
            ev.getOptions().getDeprecated(), false
        );
        options.add(option);
      }
      if (ev.getOptions().hasExtension(MetaProto.enumValueMeta)) {
        Meta meta = ev.getOptions().getExtension(MetaProto.enumValueMeta);
        OptionElement option = toOption(CONFLUENT_ENUM_VALUE_META, meta);
        if (option != null) {
          options.add(option);
        }
      }
      options.addAll(toCustomOptions(ev.getOptions()));
      constants.add(new EnumConstantElement(
          DEFAULT_LOCATION,
          ev.getName(),
          ev.getNumber(),
          "",
          options.build()
      ));
    }
    ImmutableList.Builder<ReservedElement> reserved = ImmutableList.builder();
    for (EnumReservedRange range : ed.getReservedRangeList()) {
      ReservedElement reservedElem = toReserved(range);
      reserved.add(reservedElem);
    }
    for (String reservedName : ed.getReservedNameList()) {
      ReservedElement reservedElem = new ReservedElement(
          DEFAULT_LOCATION,
          "",
          Collections.singletonList(reservedName)
      );
      reserved.add(reservedElem);
    }
    ImmutableList.Builder<OptionElement> options = ImmutableList.builder();
    if (ed.getOptions().hasAllowAlias()) {
      OptionElement option = new OptionElement(
          ALLOW_ALIAS, Kind.BOOLEAN,
          ed.getOptions().getAllowAlias(), false
      );
      options.add(option);
    }
    if (ed.getOptions().hasDeprecated()) {
      OptionElement option = new OptionElement(
          DEPRECATED, Kind.BOOLEAN,
          ed.getOptions().getDeprecated(), false
      );
      options.add(option);
    }
    if (ed.getOptions().hasExtension(MetaProto.enumMeta)) {
      Meta meta = ed.getOptions().getExtension(MetaProto.enumMeta);
      OptionElement option = toOption(CONFLUENT_ENUM_META, meta);
      if (option != null) {
        options.add(option);
      }
    }
    options.addAll(toCustomOptions(ed.getOptions()));
    return new EnumElement(DEFAULT_LOCATION, name, "",
        options.build(), constants.build(), reserved.build());
  }

  private static ReservedElement toReserved(ReservedRange range) {
    List<Object> values = new ArrayList<>();
    int start = range.getStart();
    int end = range.getEnd();
    // inclusive, exclusive
    values.add(start == end - 1 ? start : new IntRange(start, end - 1));
    return new ReservedElement(DEFAULT_LOCATION, "", values);
  }

  private static ReservedElement toReserved(EnumReservedRange range) {
    List<Object> values = new ArrayList<>();
    int start = range.getStart();
    int end = range.getEnd();
    // inclusive, inclusive
    values.add(start == end ? start : new IntRange(start, end));
    return new ReservedElement(DEFAULT_LOCATION, "", values);
  }

  private static ExtensionsElement toExtension(ExtensionRange range) {
    List<Object> values = new ArrayList<>();
    int start = range.getStart();
    int end = range.getEnd();
    // inclusive, exclusive
    values.add(start == end - 1 ? start : new IntRange(start, end - 1));
    return new ExtensionsElement(DEFAULT_LOCATION, "", values);
  }

  private static ServiceElement toService(ServiceDescriptorProto sd) {
    String name = sd.getName();
    log.trace("*** service name: {}", name);
    ImmutableList.Builder<RpcElement> methods = ImmutableList.builder();
    for (MethodDescriptorProto method : sd.getMethodList()) {
      ImmutableList.Builder<OptionElement> options = ImmutableList.builder();
      if (method.getOptions().hasDeprecated()) {
        OptionElement option = new OptionElement(
            DEPRECATED, Kind.BOOLEAN,
            method.getOptions().getDeprecated(), false
        );
        options.add(option);
      }
      if (method.getOptions().hasIdempotencyLevel()) {
        OptionElement option = new OptionElement(
            IDEMPOTENCY_LEVEL, Kind.ENUM,
            method.getOptions().getIdempotencyLevel(), false
        );
        options.add(option);
      }
      options.addAll(toCustomOptions(method.getOptions()));
      methods.add(new RpcElement(
          DEFAULT_LOCATION,
          method.getName(),
          "",
          method.getInputType(),
          method.getOutputType(),
          method.getClientStreaming(),
          method.getServerStreaming(),
          options.build()
      ));
    }
    ImmutableList.Builder<OptionElement> options = ImmutableList.builder();
    if (sd.getOptions().hasDeprecated()) {
      OptionElement option = new OptionElement(
          DEPRECATED, Kind.BOOLEAN,
          sd.getOptions().getDeprecated(), false
      );
      options.add(option);
    }
    options.addAll(toCustomOptions(sd.getOptions()));
    return new ServiceElement(DEFAULT_LOCATION, name, "", methods.build(), options.build());
  }

  private static FieldElement toField(
      FileDescriptorProto file, FieldDescriptorProto fd, boolean inOneof) {
    String name = fd.getName();
    log.trace("*** field name: {}", name);
    ImmutableList.Builder<OptionElement> options = ImmutableList.builder();
    if (fd.getOptions().hasCtype()) {
      OptionElement option = new OptionElement(CTYPE, Kind.ENUM, fd.getOptions().getCtype(), false);
      options.add(option);
    }
    if (fd.getOptions().hasPacked()) {
      OptionElement option =
          new OptionElement(PACKED, Kind.BOOLEAN, fd.getOptions().getPacked(), false);
      options.add(option);
    }
    if (fd.getOptions().hasJstype()) {
      OptionElement option =
          new OptionElement(JSTYPE, Kind.ENUM, fd.getOptions().getJstype(), false);
      options.add(option);
    }
    if (fd.getOptions().hasDeprecated()) {
      OptionElement option =
          new OptionElement(DEPRECATED, Kind.BOOLEAN, fd.getOptions().getDeprecated(), false);
      options.add(option);
    }
    if (fd.getOptions().hasExtension(MetaProto.fieldMeta)) {
      Meta meta = fd.getOptions().getExtension(MetaProto.fieldMeta);
      OptionElement option = toOption(CONFLUENT_FIELD_META, meta);
      if (option != null) {
        options.add(option);
      }
    }
    options.addAll(toCustomOptions(fd.getOptions()));
    String jsonName = fd.hasJsonName() ? fd.getJsonName() : null;
    String defaultValue = !PROTO3.equals(file.getSyntax()) && fd.hasDefaultValue()
                          ? fd.getDefaultValue()
                          : null;
    return new FieldElement(DEFAULT_LOCATION,
        inOneof ? null : label(file, fd),
        dataType(fd),
        name,
        defaultValue,
        jsonName,
        fd.getNumber(),
        "",
        options.build()
    );
  }

  private static Field.Label label(FileDescriptorProto file, FieldDescriptorProto fd) {
    if (fd.getProto3Optional()) {
      return Field.Label.OPTIONAL;
    }
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

  private static String dataType(FieldDescriptorProto field) {
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

  public EnumDescriptor getEnumDescriptor(String enumTypeName) {
    return toDynamicSchema().getEnumDescriptor(enumTypeName);
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
    return null;
  }

  private EnumElement firstEnum() {
    for (TypeElement typeElement : schemaObj.getTypes()) {
      if (typeElement instanceof EnumElement) {
        return (EnumElement) typeElement;
      }
    }
    return null;
  }

  public DynamicSchema toDynamicSchema() {
    return toDynamicSchema(DEFAULT_NAME);
  }

  public DynamicSchema toDynamicSchema(String name) {
    if (schemaObj == null) {
      return null;
    }
    if (dynamicSchema == null) {
      Map<String, DynamicSchema> cache = new HashMap<>();
      dynamicSchema = toDynamicSchema(name, schemaObj, dependenciesWithLogicalTypes(), cache);
    }
    return dynamicSchema;
  }

  private static DynamicSchema toDynamicSchema(
      String name, ProtoFileElement rootElem, Map<String, ProtoFileElement> dependencies, 
      Map<String, DynamicSchema> cache
  ) {

    if (cache.containsKey(name)) {
      return cache.get(name);
    }

    if (log.isTraceEnabled()) {
      log.trace("*** toDynamicSchema: {}", ProtobufSchemaUtils.toString(rootElem));
    }
    DynamicSchema.Builder schema = DynamicSchema.newBuilder();
    try {
      Syntax syntax = rootElem.getSyntax();
      if (syntax != null) {
        schema.setSyntax(syntax.toString());
      }
      if (rootElem.getPackageName() != null) {
        schema.setPackage(rootElem.getPackageName());
      }
      for (TypeElement typeElem : rootElem.getTypes()) {
        if (typeElem instanceof MessageElement) {
          MessageDefinition message = toDynamicMessage(syntax, (MessageElement) typeElem);
          schema.addMessageDefinition(message);
        } else if (typeElem instanceof EnumElement) {
          EnumDefinition enumer = toDynamicEnum((EnumElement) typeElem);
          schema.addEnumDefinition(enumer);
        }
      }
      for (ServiceElement serviceElement : rootElem.getServices()) {
        ServiceDefinition service = toDynamicService(serviceElement);
        schema.addServiceDefinition(service);
      }
      for (ExtendElement extendElement : rootElem.getExtendDeclarations()) {
        for (FieldElement field : extendElement.getFields()) {
          Field.Label fieldLabel = field.getLabel();
          String label = fieldLabel != null ? fieldLabel.toString().toLowerCase() : null;
          String fieldType = field.getType();
          String defaultVal = field.getDefaultValue();
          String jsonName = field.getJsonName();
          Map<String, OptionElement> options = mergeOptions(field.getOptions());
          CType ctype = findOption(CTYPE, options)
              .map(o -> CType.valueOf(o.getValue().toString())).orElse(null);
          Boolean isPacked = findOption(PACKED, options)
              .map(o -> Boolean.valueOf(o.getValue().toString())).orElse(null);
          JSType jstype = findOption(JSTYPE, options)
              .map(o -> JSType.valueOf(o.getValue().toString())).orElse(null);
          Boolean isDeprecated = findOption(DEPRECATED, options)
              .map(o -> Boolean.valueOf(o.getValue().toString())).orElse(null);
          ProtobufMeta metadata = findMeta(CONFLUENT_FIELD_META, options);
          schema.addExtendDefinition(
              extendElement.getName(),
              label,
              fieldType,
              field.getName(),
              field.getTag(),
              defaultVal,
              jsonName,
              metadata,
              ctype,
              isPacked,
              jstype,
              isDeprecated
          );
        }
      }
      for (String ref : rootElem.getImports()) {
        ProtoFileElement dep = dependencies.get(ref);
        if (dep != null) {
          schema.addDependency(ref);
          schema.addSchema(toDynamicSchema(ref, dep, dependencies, cache));
        }
      }
      for (String ref : rootElem.getPublicImports()) {
        ProtoFileElement dep = dependencies.get(ref);
        if (dep != null) {
          schema.addPublicDependency(ref);
          schema.addSchema(toDynamicSchema(ref, dep, dependencies, cache));
        }
      }
      Map<String, OptionElement> options = mergeOptions(rootElem.getOptions());
      OptionElement javaPackageName = options.get(JAVA_PACKAGE);
      if (javaPackageName != null) {
        schema.setJavaPackage(javaPackageName.getValue().toString());
      }
      OptionElement javaOuterClassname = options.get(JAVA_OUTER_CLASSNAME);
      if (javaOuterClassname != null) {
        schema.setJavaOuterClassname(javaOuterClassname.getValue().toString());
      }
      OptionElement javaMultipleFiles = options.get(JAVA_MULTIPLE_FILES);
      if (javaMultipleFiles != null) {
        schema.setJavaMultipleFiles(Boolean.parseBoolean(javaMultipleFiles.getValue().toString()));
      }
      OptionElement javaGenerateEqualsAndHash = options.get(JAVA_GENERATE_EQUALS_AND_HASH);
      if (javaGenerateEqualsAndHash != null) {
        schema.setJavaGenerateEqualsAndHash(
            Boolean.parseBoolean(javaGenerateEqualsAndHash.getValue().toString()));
      }
      OptionElement javaStringCheckUtf8 = options.get(JAVA_STRING_CHECK_UTF8);
      if (javaStringCheckUtf8 != null) {
        schema.setJavaStringCheckUtf8(
            Boolean.parseBoolean(javaStringCheckUtf8.getValue().toString()));
      }
      OptionElement optimizeFor = options.get(OPTIMIZE_FOR);
      if (optimizeFor != null) {
        schema.setOptimizeFor(OptimizeMode.valueOf(optimizeFor.getValue().toString()));
      }
      OptionElement goPackage = options.get(GO_PACKAGE);
      if (goPackage != null) {
        schema.setGoPackage(goPackage.getValue().toString());
      }
      OptionElement ccGenericServices = options.get(CC_GENERIC_SERVICES);
      if (ccGenericServices != null) {
        schema.setCcGenericServices(Boolean.parseBoolean(ccGenericServices.getValue().toString()));
      }
      OptionElement javaGenericServices = options.get(JAVA_GENERIC_SERVICES);
      if (javaGenericServices != null) {
        schema.setJavaGenericServices(
            Boolean.parseBoolean(javaGenericServices.getValue().toString()));
      }
      OptionElement pyGenericServices = options.get(PY_GENERIC_SERVICES);
      if (pyGenericServices != null) {
        schema.setPyGenericServices(Boolean.parseBoolean(pyGenericServices.getValue().toString()));
      }
      OptionElement phpGenericServices = options.get(PHP_GENERIC_SERVICES);
      if (phpGenericServices != null) {
        schema.setPhpGenericServices(
            Boolean.parseBoolean(phpGenericServices.getValue().toString()));
      }
      OptionElement isDeprecated = options.get(DEPRECATED);
      if (isDeprecated != null) {
        schema.setDeprecated(Boolean.parseBoolean(isDeprecated.getValue().toString()));
      }
      OptionElement ccEnableArenas = options.get(CC_ENABLE_ARENAS);
      if (ccEnableArenas != null) {
        schema.setCcEnableArenas(Boolean.parseBoolean(ccEnableArenas.getValue().toString()));
      }
      OptionElement objcClassPrefix = options.get(OBJC_CLASS_PREFIX);
      if (objcClassPrefix != null) {
        schema.setObjcClassPrefix(objcClassPrefix.getValue().toString());
      }
      OptionElement csharpNamespace = options.get(CSHARP_NAMESPACE);
      if (csharpNamespace != null) {
        schema.setCsharpNamespace(csharpNamespace.getValue().toString());
      }
      OptionElement swiftPrefix = options.get(SWIFT_PREFIX);
      if (swiftPrefix != null) {
        schema.setSwiftPrefix(swiftPrefix.getValue().toString());
      }
      OptionElement phpClassPrefix = options.get(PHP_CLASS_PREFIX);
      if (phpClassPrefix != null) {
        schema.setPhpClassPrefix(phpClassPrefix.getValue().toString());
      }
      OptionElement phpNamespace = options.get(PHP_NAMESPACE);
      if (phpNamespace != null) {
        schema.setPhpNamespace(phpNamespace.getValue().toString());
      }
      OptionElement phpMetadataNamespace = options.get(PHP_METADATA_NAMESPACE);
      if (phpMetadataNamespace != null) {
        schema.setPhpMetadataNamespace(phpMetadataNamespace.getValue().toString());
      }
      OptionElement rubyPackage = options.get(RUBY_PACKAGE);
      if (rubyPackage != null) {
        schema.setRubyPackage(rubyPackage.getValue().toString());
      }
      ProtobufMeta meta = findMeta(CONFLUENT_FILE_META, options);
      schema.setMeta(meta);
      schema.setName(name);
      DynamicSchema dynamicSchema = schema.build();
      cache.put(name, dynamicSchema);
      return dynamicSchema;
    } catch (Descriptors.DescriptorValidationException e) {
      throw new IllegalStateException(e);
    }
  }

  private static Map<String, OptionElement> mergeOptions(List<OptionElement> options) {
    // This method is mainly used to merge Confluent meta options
    // which may not be using the alternative aggregate syntax.
    return options.stream()
        .collect(Collectors.toMap(
            o -> o.getName().startsWith(".") ? o.getName().substring(1) : o.getName(),
            ProtobufSchema::transform,
            ProtobufSchema::merge));
  }

  @SuppressWarnings("unchecked")
  protected static OptionElement merge(OptionElement existing, OptionElement replacement) {
    replacement = transform(replacement);
    if (existing.getKind() == Kind.MAP && replacement.getKind() == Kind.MAP) {
      Map<String, ?> existingMap = (Map<String, ?>) existing.getValue();
      Map<String, ?> replacementMap = (Map<String, ?>) replacement.getValue();
      Map<String, Object> mergedMap = new LinkedHashMap<>(existingMap);
      for (Map.Entry<String, ?> entry : replacementMap.entrySet()) {
        // Merging should only be needed for repeated fields
        mergedMap.merge(entry.getKey(), entry.getValue(), (v1, v2) -> {
          Set<Object> set = new LinkedHashSet<>();
          if (v1 instanceof List) {
            set.addAll((List<Object>) v1);
          } else {
            set.add(v1);
          }
          if (v2 instanceof List) {
            set.addAll((List<Object>) v2);
          } else {
            set.add(v2);
          }
          return new ArrayList<>(set);
        });
      }
      return new OptionElement(
          replacement.getName(), Kind.MAP, mergedMap, replacement.isParenthesized());
    } else {
      // Discard existing option
      // This should only happen with custom options that are ignored
      return replacement;
    }
  }

  protected static OptionElement transform(OptionElement option) {
    if (option.getKind() == Kind.OPTION) {
      Map<String, ?> map = transformOptionMap(option);
      return new OptionElement(option.getName(), Kind.MAP, map, option.isParenthesized());
    } else {
      return option;
    }
  }

  private static Map<String, ?> transformOptionMap(OptionElement option) {
    if (option.getKind() != Kind.OPTION) {
      throw new IllegalArgumentException("Expected option of kind OPTION");
    }
    OptionElement value = (OptionElement) option.getValue();
    Object mapValue = value.getValue();
    Kind kind = value.getKind();
    if (kind == Kind.BOOLEAN || kind == Kind.ENUM || kind == Kind.NUMBER) {
      // Wire only creates OptionPrimitive for the above kinds
      mapValue = new OptionElement.OptionPrimitive(kind, mapValue);
    } else if (kind == Kind.OPTION) {
      // Recursively convert options of kind OPTION to maps
      mapValue = transformOptionMap(value);
    }
    return Collections.singletonMap(value.getName(), mapValue);
  }

  private static MessageDefinition toDynamicMessage(
      Syntax syntax,
      MessageElement messageElem
  ) {
    log.trace("*** message: {}", messageElem.getName());
    MessageDefinition.Builder message = MessageDefinition.newBuilder(messageElem.getName());
    for (TypeElement type : messageElem.getNestedTypes()) {
      if (type instanceof MessageElement) {
        message.addMessageDefinition(toDynamicMessage(syntax, (MessageElement) type));
      } else if (type instanceof EnumElement) {
        message.addEnumDefinition(toDynamicEnum((EnumElement) type));
      }
    }
    Set<String> added = new HashSet<>();
    for (OneOfElement oneof : messageElem.getOneOfs()) {
      MessageDefinition.OneofBuilder oneofBuilder = message.addOneof(oneof.getName());
      for (FieldElement field : oneof.getFields()) {
        String defaultVal = field.getDefaultValue();
        String jsonName = field.getJsonName();
        Map<String, OptionElement> options = mergeOptions(field.getOptions());
        CType ctype = findOption(CTYPE, options)
            .map(o -> CType.valueOf(o.getValue().toString())).orElse(null);
        JSType jstype = findOption(JSTYPE, options)
            .map(o -> JSType.valueOf(o.getValue().toString())).orElse(null);
        Boolean isDeprecated = findOption(DEPRECATED, options)
            .map(o -> Boolean.valueOf(o.getValue().toString())).orElse(null);
        ProtobufMeta meta = findMeta(CONFLUENT_FIELD_META, options);
        oneofBuilder.addField(
            false,
            field.getType(),
            field.getName(),
            field.getTag(),
            defaultVal,
            jsonName,
            meta,
            ctype,
            jstype,
            isDeprecated
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
      boolean isProto3Optional = "optional".equals(label) && syntax == Syntax.PROTO_3;
      String fieldType = field.getType();
      String defaultVal = field.getDefaultValue();
      String jsonName = field.getJsonName();
      Map<String, OptionElement> options = mergeOptions(field.getOptions());
      CType ctype = findOption(CTYPE, options)
          .map(o -> CType.valueOf(o.getValue().toString())).orElse(null);
      Boolean isPacked = findOption(PACKED, options)
          .map(o -> Boolean.valueOf(o.getValue().toString())).orElse(null);
      JSType jstype = findOption(JSTYPE, options)
          .map(o -> JSType.valueOf(o.getValue().toString())).orElse(null);
      Boolean isDeprecated = findOption(DEPRECATED, options)
          .map(o -> Boolean.valueOf(o.getValue().toString())).orElse(null);
      ProtobufMeta meta = findMeta(CONFLUENT_FIELD_META, options);
      ProtoType protoType = ProtoType.get(fieldType);
      ProtoType keyType = protoType.getKeyType();
      ProtoType valueType = protoType.getValueType();
      // Map fields are only permitted in messages
      if (protoType.isMap() && keyType != null && valueType != null) {
        label = "repeated";
        fieldType = toMapEntry(field.getName());
        MessageDefinition.Builder mapMessage = MessageDefinition.newBuilder(fieldType);
        mapMessage.setMapEntry(true);
        mapMessage.addField(null, keyType.toString(), KEY_FIELD, 1, null, null);
        mapMessage.addField(null, valueType.toString(), VALUE_FIELD, 2, null, null);
        message.addMessageDefinition(mapMessage.build());
      }
      if (isProto3Optional) {
        // Add synthetic oneof after real oneofs
        MessageDefinition.OneofBuilder oneofBuilder = message.addOneof("_" + field.getName());
        oneofBuilder.addField(
            true,
            fieldType,
            field.getName(),
            field.getTag(),
            defaultVal,
            jsonName,
            meta,
            ctype,
            jstype,
            isDeprecated
        );
      } else {
        message.addField(
            label,
            fieldType,
            field.getName(),
            field.getTag(),
            defaultVal,
            jsonName,
            meta,
            ctype,
            isPacked,
            jstype,
            isDeprecated
        );
      }
    }
    for (ReservedElement reserved : messageElem.getReserveds()) {
      for (Object elem : reserved.getValues()) {
        if (elem instanceof String) {
          message.addReservedName((String) elem);
        } else if (elem instanceof Integer) {
          int tag = (Integer) elem;
          message.addReservedRange(tag, tag + 1);
        } else if (elem instanceof IntRange) {
          IntRange range = (IntRange) elem;
          message.addReservedRange(range.getStart(), range.getEndInclusive() + 1);
        } else {
          throw new IllegalStateException("Unsupported reserved type: " + elem.getClass()
              .getName());
        }
      }
    }
    for (ExtensionsElement extension : messageElem.getExtensions()) {
      for (Object elem : extension.getValues()) {
        if (elem instanceof Integer) {
          int tag = (Integer) elem;
          message.addExtensionRange(tag, tag + 1);
        } else if (elem instanceof IntRange) {
          IntRange range = (IntRange) elem;
          message.addExtensionRange(range.getStart(), range.getEndInclusive() + 1);
        } else {
          throw new IllegalStateException("Unsupported extensions type: " + elem.getClass()
              .getName());
        }
      }
    }
    for (ExtendElement extendElement : messageElem.getExtendDeclarations()) {
      for (FieldElement field : extendElement.getFields()) {
        Field.Label fieldLabel = field.getLabel();
        String label = fieldLabel != null ? fieldLabel.toString().toLowerCase() : null;
        String fieldType = field.getType();
        String defaultVal = field.getDefaultValue();
        String jsonName = field.getJsonName();
        Map<String, OptionElement> options = mergeOptions(field.getOptions());
        CType ctype = findOption(CTYPE, options)
            .map(o -> CType.valueOf(o.getValue().toString())).orElse(null);
        Boolean isPacked = findOption(PACKED, options)
            .map(o -> Boolean.valueOf(o.getValue().toString())).orElse(null);
        JSType jstype = findOption(JSTYPE, options)
            .map(o -> JSType.valueOf(o.getValue().toString())).orElse(null);
        Boolean isDeprecated = findOption(DEPRECATED, options)
            .map(o -> Boolean.valueOf(o.getValue().toString())).orElse(null);
        ProtobufMeta metadata = findMeta(CONFLUENT_FIELD_META, options);
        message.addExtendDefinition(
            extendElement.getName(),
            label,
            fieldType,
            field.getName(),
            field.getTag(),
            defaultVal,
            jsonName,
            metadata,
            ctype,
            isPacked,
            jstype,
            isDeprecated
        );
      }
    }
    Map<String, OptionElement> options = mergeOptions(messageElem.getOptions());
    findOption(NO_STANDARD_DESCRIPTOR_ACCESSOR, options)
            .map(o -> Boolean.valueOf(o.getValue().toString()))
            .ifPresent(message::setNoStandardDescriptorAccessor);
    findOption(DEPRECATED, options)
            .map(o -> Boolean.valueOf(o.getValue().toString()))
            .ifPresent(message::setDeprecated);
    findOption(MAP_ENTRY, options)
            .map(o -> Boolean.valueOf(o.getValue().toString()))
            .ifPresent(message::setMapEntry);
    ProtobufMeta meta = findMeta(CONFLUENT_MESSAGE_META, options);
    message.setMeta(meta);
    return message.build();
  }

  public static Optional<OptionElement> findOption(
      String name, List<OptionElement> options) {
    return findOption(name, mergeOptions(options));
  }

  public static Optional<OptionElement> findOption(
      String name, Map<String, OptionElement> options) {
    return Optional.ofNullable(options.get(name));
  }

  public static ProtobufMeta findMeta(String name, List<OptionElement> options) {
    return findMeta(name, mergeOptions(options));
  }

  public static ProtobufMeta findMeta(String name, Map<String, OptionElement> options) {
    Optional<OptionElement> meta = findOption(name, options);
    if (!meta.isPresent()) {
      return null;
    }
    return new ProtobufMeta(findDoc(meta), findParams(meta), findTags(meta));
  }

  public static String findDoc(Optional<OptionElement> meta) {
    return (String) findMetaField(meta, DOC_FIELD);
  }

  @SuppressWarnings("unchecked")
  public static Map<String, String> findParams(Optional<OptionElement> meta) {
    Object result = findMetaField(meta, PARAMS_FIELD);
    if (result == null) {
      return null;
    } else if (result instanceof Map) {
      return (Map<String, String>) result;
    } else if (result instanceof List) {
      List<Map<String, String>> keyValues = (List<Map<String, String>>) result;
      Map<String, String> params = new LinkedHashMap<>();
      for (Map<String, String> keyValue : keyValues) {
        String key = keyValue.get(KEY_FIELD);
        String value = keyValue.get(VALUE_FIELD);
        params.put(key, value);
      }
      return params;
    } else {
      throw new IllegalStateException("Unrecognized params type " + result.getClass().getName());
    }
  }

  public static List<String> findTags(Optional<OptionElement> meta) {
    Object result = findMetaField(meta, TAGS_FIELD);
    if (result instanceof List) {
      return (List<String>) result;
    } else {
      return result != null ? Collections.singletonList(result.toString()) : null;
    }
  }

  @SuppressWarnings("unchecked")
  private static Object findMetaField(Optional<OptionElement> meta, String name) {
    OptionElement options = meta.get();
    switch (options.getKind()) {
      case OPTION:
        OptionElement option = (OptionElement) options.getValue();
        if (option.getName().equals(name)) {
          return option.getValue();
        } else {
          return null;
        }
      case MAP:
        Map<String, ?> map = (Map<String, ?>) options.getValue();
        return map.get(name);
      default:
        throw new IllegalStateException("Unexpected custom option kind " + options.getKind());
    }
  }

  private static EnumDefinition toDynamicEnum(EnumElement enumElem) {
    Map<String, OptionElement> enumOptions = mergeOptions(enumElem.getOptions());
    Boolean allowAlias = findOption(ALLOW_ALIAS, enumOptions)
        .map(o -> Boolean.valueOf(o.getValue().toString())).orElse(null);
    Boolean isDeprecated = findOption(DEPRECATED, enumOptions)
        .map(o -> Boolean.valueOf(o.getValue().toString())).orElse(null);
    EnumDefinition.Builder enumer =
        EnumDefinition.newBuilder(enumElem.getName(), allowAlias, isDeprecated);
    for (ReservedElement reserved : enumElem.getReserveds()) {
      for (Object elem : reserved.getValues()) {
        if (elem instanceof String) {
          enumer.addReservedName((String) elem);
        } else if (elem instanceof Integer) {
          int tag = (Integer) elem;
          enumer.addReservedRange(tag, tag);
        } else if (elem instanceof IntRange) {
          IntRange range = (IntRange) elem;
          enumer.addReservedRange(range.getStart(), range.getEndInclusive());
        } else {
          throw new IllegalStateException("Unsupported reserved type: " + elem.getClass()
              .getName());
        }
      }
    }
    for (EnumConstantElement constant : enumElem.getConstants()) {
      Map<String, OptionElement> constantOptions = mergeOptions(constant.getOptions());
      Boolean isConstDeprecated = findOption(DEPRECATED, constantOptions)
          .map(o -> Boolean.valueOf(o.getValue().toString())).orElse(null);
      ProtobufMeta meta = findMeta(CONFLUENT_ENUM_VALUE_META, constantOptions);
      enumer.addValue(constant.getName(), constant.getTag(), meta, isConstDeprecated);
    }
    ProtobufMeta meta = findMeta(CONFLUENT_ENUM_META, enumOptions);
    enumer.setMeta(meta);
    return enumer.build();
  }

  private static ServiceDefinition toDynamicService(ServiceElement serviceElement) {
    ServiceDefinition.Builder service =
        ServiceDefinition.newBuilder(serviceElement.getName());
    Map<String, OptionElement> serviceOptions = mergeOptions(serviceElement.getOptions());
    findOption(DEPRECATED, serviceOptions)
            .map(o -> Boolean.valueOf(o.getValue().toString()))
            .ifPresent(service::setDeprecated);
    for (RpcElement method : serviceElement.getRpcs()) {
      Map<String, OptionElement> methodOptions = mergeOptions(method.getOptions());
      Boolean isMethodDeprecated = findOption(DEPRECATED, methodOptions)
          .map(o -> Boolean.valueOf(o.getValue().toString())).orElse(null);
      IdempotencyLevel idempotencyLevel = findOption(IDEMPOTENCY_LEVEL, methodOptions)
          .map(o -> IdempotencyLevel.valueOf(o.getValue().toString())).orElse(null);
      service.addMethod(method.getName(), method.getRequestType(), method.getResponseType(),
          method.getRequestStreaming(), method.getResponseStreaming(),
          isMethodDeprecated, idempotencyLevel);
    }
    return service.build();
  }

  @Override
  public ProtoFileElement rawSchema() {
    return schemaObj;
  }

  @Override
  public boolean hasTopLevelField(String field) {
    return schemaObj != null && schemaObj.getTypes()
            .stream()
            .map(ProtobufSchema::getFieldNames)
            .flatMap(Collection::stream)
            .anyMatch(property -> property.equals(field));
  }

  @Override
  public String schemaType() {
    return TYPE;
  }

  @Override
  public String name() {
    if (name != null) {
      return name;
    }
    TypeElement typeElement = firstMessage();
    if (typeElement == null) {
      typeElement = firstEnum();
    }
    if (typeElement == null) {
      throw new IllegalArgumentException("Protobuf schema definition contains no type definitions");
    }
    String typeName = typeElement.getName();
    String packageName = schemaObj.getPackageName();
    return packageName != null && !packageName.isEmpty()
        ? packageName + '.' + typeName
        : typeName;
  }

  @Override
  public String canonicalString() {
    if (schemaObj == null) {
      return null;
    }
    if (canonicalString == null) {
      canonicalString = ProtobufSchemaUtils.toString(schemaObj);
    }
    return canonicalString;
  }

  @Override
  public String formattedString(String format) {
    if (format == null || format.trim().isEmpty()) {
      return canonicalString();
    }
    Format formatEnum = Format.get(format);
    switch (formatEnum) {
      case DEFAULT:
        return canonicalString();
      case IGNORE_EXTENSIONS:
        FormatContext ctx = new FormatContext(true, false);
        return ProtobufSchemaUtils.toFormattedString(ctx, this);
      case SERIALIZED:
        FileDescriptorProto file = toDynamicSchema().getFileDescriptorProto();
        return base64Encoder.encodeToString(file.toByteArray());
      default:
        // Don't throw an exception for forward compatibility of formats
        log.warn("Unsupported format {}", format);
        return canonicalString();
    }
  }

  @Override
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
        .collect(Collectors.toMap(
                Map.Entry::getKey, e -> ProtobufSchemaUtils.toString(e.getValue())));
  }

  public Map<String, ProtoFileElement> dependencies() {
    return dependencies;
  }

  public Map<String, ProtoFileElement> dependenciesWithLogicalTypes() {
    Map<String, ProtoFileElement> deps = new HashMap<>(dependencies);
    for (Map.Entry<String, ProtoFileElement> entry : KNOWN_DEPENDENCIES.entrySet()) {
      if (!deps.containsKey(entry.getKey())) {
        deps.put(entry.getKey(), entry.getValue());
      }
    }
    return deps;
  }

  @Override
  public Metadata metadata() {
    return metadata;
  }

  @Override
  public RuleSet ruleSet() {
    return ruleSet;
  }

  @Override
  public ProtobufSchema normalize() {
    String normalized = ProtobufSchemaUtils.toNormalizedString(this);
    return new ProtobufSchema(
        toProtoFile(normalized),
        this.version,
        this.name,
        this.references.stream().sorted().distinct().collect(Collectors.toList()),
        this.dependencies,
        this.metadata,
        this.ruleSet,
        normalized,
        null,
        null
    );
  }

  @Override
  public void validate(boolean strict) {
    // Normalization will try to resolve types
    normalize();
  }

  @Override
  public List<String> isBackwardCompatible(ParsedSchema previousSchema) {
    if (!schemaType().equals(previousSchema.schemaType())) {
      return Lists.newArrayList("Incompatible because of different schema type");
    }
    final List<Difference> differences = SchemaDiff.compare(
        (ProtobufSchema) previousSchema, this
    );
    final List<Difference> incompatibleDiffs = differences.stream()
        .filter(diff -> !SchemaDiff.COMPATIBLE_CHANGES.contains(diff.getType()))
        .collect(Collectors.toList());
    boolean isCompatible = incompatibleDiffs.isEmpty();
    if (!isCompatible) {
      List<String> errorMessages = new ArrayList<>();
      for (Difference incompatibleDiff : incompatibleDiffs) {
        errorMessages.add(incompatibleDiff.toString());
      }
      return errorMessages;
    } else {
      return new ArrayList<>();
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
    return Objects.equals(version, that.version)
        && Objects.equals(references, that.references)
        && Objects.equals(canonicalString(), that.canonicalString())
        && Objects.equals(metadata, that.metadata)
        && Objects.equals(ruleSet, that.ruleSet);
  }

  @Override
  public int hashCode() {
    if (hashCode == NO_HASHCODE) {
      // Can't use schemaObj as locations may differ
      hashCode = Objects.hash(canonicalString(), references, version, metadata, ruleSet);
    }
    return hashCode;
  }

  @Override
  public String toString() {
    return canonicalString();
  }

  public GenericDescriptor toSpecificDescriptor(String originalPath) {
    String clsName = fullName(originalPath);
    if (clsName == null) {
      return null;
    }
    try {
      Class<?> cls = Class.forName(clsName);
      Method parseMethod = cls.getDeclaredMethod("getDescriptor");
      return (GenericDescriptor) parseMethod.invoke(null);
    } catch (ClassNotFoundException e) {
      throw new IllegalArgumentException("Class " + clsName + " could not be found.");
    } catch (NoSuchMethodException e) {
      throw new IllegalArgumentException("Class " + clsName
          + " is not a valid protobuf message class", e);
    } catch (IllegalAccessException | InvocationTargetException e) {
      throw new IllegalArgumentException("Not a valid protobuf builder");
    }
  }

  public String fullName() {
    return fullName(null);
  }

  public String fullName(String originalPath) {
    Map<String, OptionElement> options = mergeOptions(schemaObj.getOptions());
    OptionElement javaPackageName = options.get(JAVA_PACKAGE);
    OptionElement javaOuterClassname = options.get(JAVA_OUTER_CLASSNAME);
    OptionElement javaMultipleFiles = options.get(JAVA_MULTIPLE_FILES);

    String outer = "";
    if (javaMultipleFiles == null
        || !Boolean.parseBoolean(javaMultipleFiles.getValue().toString())) {
      if (javaOuterClassname != null) {
        outer = javaOuterClassname.getValue().toString();
      } else if (originalPath != null) {
        Path path = Paths.get(originalPath).getFileName();
        if (path != null) {
          String fileName = path.toString();
          if (fileName.endsWith(".proto")) {
            fileName = fileName.substring(0, fileName.length() - ".proto".length());
          }
          outer = underscoresToCamelCase(fileName, true);
          if (hasConflictingClassName(outer)) {
            outer += "OuterClass";
          }
        }
      } else {
        // Can't determine full name w/o either java_outer_classname or java_multiple_files=true
        return null;
      }
    }
    String p = javaPackageName != null
        ? javaPackageName.getValue().toString()
        : schemaObj.getPackageName();
    StringBuilder inner = new StringBuilder();
    String typeName = name;
    if (typeName == null && !schemaObj.getTypes().isEmpty()) {
      typeName = name();
    }
    List<TypeElement> typeElems = toTypeElements(typeName);
    for (TypeElement typeElem : typeElems) {
      if (inner.length() > 0) {
        inner.append("$");
      }
      inner.append(typeElem.getName());
    }
    String d1 = (!outer.isEmpty() || inner.length() != 0 ? "." : "");
    String d2 = (!outer.isEmpty() && inner.length() != 0 ? "$" : "");
    return p + d1 + outer + d2 + inner;
  }

  private boolean hasConflictingClassName(String className) {
    for (TypeElement type : schemaObj.getTypes()) {
      if (type.getName().equals(className)) {
        return true;
      }
      if (type instanceof MessageElement) {
        if (messageHasConflictingClassName(className, ((MessageElement) type))) {
          return true;
        }
      }
    }
    for (ServiceElement service : schemaObj.getServices()) {
      if (service.getName().equals(className)) {
        return true;
      }
    }
    return false;
  }

  private boolean messageHasConflictingClassName(String className, MessageElement message) {
    for (TypeElement type : message.getNestedTypes()) {
      if (type.getName().equals(className)) {
        return true;
      }
      if (type instanceof MessageElement) {
        if (messageHasConflictingClassName(className, ((MessageElement) type))) {
          return true;
        }
      }
    }
    return false;
  }

  public MessageIndexes toMessageIndexes(String name) {
    return toMessageIndexes(name, false);
  }

  public MessageIndexes toMessageIndexes(String name, boolean normalize) {
    List<Integer> indexes = new ArrayList<>();
    String[] parts = name.split("\\.");
    List<TypeElement> types = schemaObj.getTypes();
    for (String part : parts) {
      int i = 0;
      for (TypeElement type : types) {
        if (type instanceof MessageElement) {
          if (normalize) {
            boolean isMapEntry = findOption(MAP_ENTRY, type.getOptions())
                .map(o -> Boolean.valueOf(o.getValue().toString())).orElse(false);
            if (isMapEntry) {
              // Skip map entries if normalizing
              continue;
            }
          }
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
    String messageName = sb.toString();
    String packageName = schemaObj.getPackageName();
    return packageName != null && !packageName.isEmpty()
        ? packageName + '.' + messageName
        : messageName;
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

  private List<TypeElement> toTypeElements(String name) {
    List<TypeElement> typeElems = new ArrayList<>();
    if (name == null) {
      return Collections.emptyList();
    }
    String[] parts = name.split("\\.");
    List<TypeElement> types = schemaObj.getTypes();
    for (String part : parts) {
      for (TypeElement type : types) {
        if (type.getName().equals(part)) {
          typeElems.add(type);
          types = type.getNestedTypes();
          break;
        }
      }
    }
    return typeElems;
  }

  public static String toMapEntry(String s) {
    if (s.contains("_")) {
      s = LOWER_UNDERSCORE.to(UPPER_CAMEL, s);
    }
    return s + MAP_ENTRY_SUFFIX;
  }

  public static String toMapField(String s) {
    String[] parts = s.split("\\.");
    String lastPart = parts[parts.length - 1];
    if (lastPart.endsWith(MAP_ENTRY_SUFFIX)) {
      lastPart = lastPart.substring(0, lastPart.length() - MAP_ENTRY_SUFFIX.length());
      lastPart = UPPER_CAMEL.to(LOWER_UNDERSCORE, lastPart);
      parts[parts.length - 1] = lastPart;
    }
    return String.join(".", parts);
  }

  @Override
  public Object fromJson(JsonNode json) throws IOException {
    return ProtobufSchemaUtils.toObject(json, this);
  }

  @Override
  public JsonNode toJson(Object message) throws IOException {
    if (message instanceof JsonNode) {
      return (JsonNode) message;
    }
    return JacksonMapper.INSTANCE.readTree(ProtobufSchemaUtils.toJson((Message) message));
  }

  @Override
  public Object copyMessage(Object message) {
    // Protobuf messages are already immutable
    return message;
  }

  @Override
  public Object transformMessage(RuleContext ctx, FieldTransform transform, Object message)
      throws RuleException {
    try {
      Message msg = (Message) message;
      // Pass the schema-based descriptor which has the tags
      Descriptor desc = toDescriptor(msg.getDescriptorForType().getFullName());
      return toTransformedMessage(ctx, desc, msg, transform);
    } catch (RuntimeException e) {
      if (e.getCause() instanceof RuleException) {
        throw (RuleException) e.getCause();
      } else {
        throw e;
      }
    }
  }

  private Object toTransformedMessage(
      RuleContext ctx, Descriptor desc, Object message, FieldTransform transform) {
    FieldContext fieldCtx = ctx.currentField();
    if (desc == null) {
      return message;
    }
    if (message instanceof List) {
      return ((List<?>) message).stream()
          .map(it -> toTransformedMessage(ctx, desc, it, transform))
          .collect(Collectors.toList());
    } else if (message instanceof Map) {
      return message;
    } else if (message instanceof Message) {
      Message.Builder copy = ((Message) message).toBuilder();
      for (FieldDescriptor fd : copy.getDescriptorForType().getFields()) {
        FieldDescriptor schemaFd = desc.findFieldByName(fd.getName());
        try (FieldContext fc = ctx.enterField(
            message, fd.getFullName(), fd.getName(), getType(fd),
            getInlineTags(schemaFd)) // use schema-based fd which has the tags
        ) {
          if (fc != null) {
            Object value = copy.getField(fd); // we can't use the schema-based fd
            Descriptor d = desc;
            if (schemaFd.getType() == Type.MESSAGE) {
              // Pass the schema-based descriptor which has the tags
              d = schemaFd.getMessageType();
            }
            Object newValue = toTransformedMessage(ctx, d, value, transform);
            if (ctx.rule().getKind() == RuleKind.CONDITION) {
              if (Boolean.FALSE.equals(newValue)) {
                throw new RuntimeException(new RuleConditionException(ctx.rule()));
              }
            } else {
              copy.setField(fd, newValue);
            }
          }
        }
      }
      return copy.build();
    } else {
      if (fieldCtx != null) {
        try {
          Set<String> ruleTags = ctx.rule().getTags();
          if (ruleTags.isEmpty()) {
            return fieldTransform(ctx, message, transform, fieldCtx);
          } else {
            if (!RuleContext.disjoint(fieldCtx.getTags(), ruleTags)) {
              return fieldTransform(ctx, message, transform, fieldCtx);
            }
          }
        } catch (RuleException e) {
          throw new RuntimeException(e);
        }
      }
      return message;
    }
  }

  private static Object fieldTransform(RuleContext ctx, Object message, FieldTransform transform,
      FieldContext fieldCtx) throws RuleException {
    if (message instanceof ByteString) {
      message = ((ByteString)message).toByteArray();
    }
    Object result = transform.transform(ctx, fieldCtx, message);
    if (result instanceof byte[]) {
      result = ByteString.copyFrom((byte[]) result);
    }
    return result;
  }

  private RuleContext.Type getType(FieldDescriptor field) {
    if (field.isMapField()) {
      return RuleContext.Type.MAP;
    }
    switch (field.getType()) {
      case MESSAGE:
        return RuleContext.Type.RECORD;
      case ENUM:
        return RuleContext.Type.ENUM;
      case STRING:
        return RuleContext.Type.STRING;
      case BYTES:
        return RuleContext.Type.BYTES;
      case INT32:
      case UINT32:
      case FIXED32:
      case SFIXED32:
        return RuleContext.Type.INT;
      case INT64:
      case UINT64:
      case FIXED64:
      case SFIXED64:
        return RuleContext.Type.LONG;
      case FLOAT:
        return RuleContext.Type.FLOAT;
      case DOUBLE:
        return RuleContext.Type.DOUBLE;
      case BOOL:
        return RuleContext.Type.BOOLEAN;
      default:
        return RuleContext.Type.NULL;
    }
  }

  @Override
  public Set<String> inlineTags() {
    Set<String> tags = new LinkedHashSet<>();
    if (schemaObj == null) {
      return tags;
    }
    ProtobufMeta meta = findMeta(CONFLUENT_FILE_META, schemaObj.getOptions());
    if (meta != null && meta.getTags() != null) {
      tags.addAll(meta.getTags());
    }
    for (TypeElement type : schemaObj.getTypes()) {
      if (type instanceof MessageElement) {
        getInlineTagsRecursively(tags, (MessageElement) type);
      } else if (type instanceof EnumElement) {
        getInlineTagsRecursively(tags, (EnumElement) type);
      }
    }
    return tags;
  }

  private void getInlineTagsRecursively(Set<String> tags, MessageElement messageElem) {
    ProtobufMeta meta = findMeta(CONFLUENT_MESSAGE_META, messageElem.getOptions());
    if (meta != null && meta.getTags() != null) {
      tags.addAll(meta.getTags());
    }
    for (FieldElement field : messageElem.getFields()) {
      ProtobufMeta fieldMeta = findMeta(CONFLUENT_FIELD_META, field.getOptions());
      if (fieldMeta != null && fieldMeta.getTags() != null) {
        tags.addAll(fieldMeta.getTags());
      }
    }
    for (TypeElement type : messageElem.getNestedTypes()) {
      if (type instanceof MessageElement) {
        getInlineTagsRecursively(tags, (MessageElement) type);
      } else if (type instanceof EnumElement) {
        getInlineTagsRecursively(tags, (EnumElement) type);
      }
    }
    for (OneOfElement oneOfElement: messageElem.getOneOfs()) {
      for (FieldElement oneOfField : oneOfElement.getFields()) {
        ProtobufMeta fieldMeta = findMeta(CONFLUENT_FIELD_META, oneOfField.getOptions());
        if (fieldMeta != null && fieldMeta.getTags() != null) {
          tags.addAll(fieldMeta.getTags());
        }
      }
    }
  }

  private void getInlineTagsRecursively(Set<String> tags, EnumElement enumElem) {
    ProtobufMeta meta = findMeta(CONFLUENT_ENUM_META, enumElem.getOptions());
    if (meta != null && meta.getTags() != null) {
      tags.addAll(meta.getTags());
    }
    for (EnumConstantElement constant : enumElem.getConstants()) {
      ProtobufMeta constantMeta = findMeta(CONFLUENT_ENUM_VALUE_META, constant.getOptions());
      if (constantMeta != null && constantMeta.getTags() != null) {
        tags.addAll(constantMeta.getTags());
      }
    }
  }

  private Set<String> getInlineTags(FieldDescriptor fd) {
    if (fd.getOptions().hasExtension(MetaProto.fieldMeta)) {
      Meta meta = fd.getOptions().getExtension(MetaProto.fieldMeta);
      return new LinkedHashSet<>(meta.getTagsList());
    }
    return Collections.emptySet();
  }

  private void modifySchemaTags(ProtoFileElement original, JsonNode node,
                                Map<SchemaEntity, Set<String>> tagsToAddMap,
                                Map<SchemaEntity, Set<String>> tagsToRemoveMap) {
    Set<SchemaEntity> entityToModify = new LinkedHashSet<>(tagsToAddMap.keySet());
    entityToModify.addAll(tagsToRemoveMap.keySet());
    Map<Object, OptionElement> optionCache = new HashMap<>();

    for (SchemaEntity entity : entityToModify) {
      String[] identifiers = entity.getNormalizedPath().split("\\.");
      List<OptionElement> allOptions = new LinkedList<>();
      Map<String, OptionElement> mergedOptions;
      String metaName;
      JsonNode entityNode;
      Object matchingElement;

      if (SchemaEntity.EntityType.SR_RECORD == entity.getEntityType()) {
        metaName = CONFLUENT_MESSAGE_META;
        matchingElement = findMatchingElement(original, identifiers, false);
        MessageElement messageElement = (MessageElement) matchingElement;
        entityNode = findMatchingNode(node, identifiers, false);
        OptionElement cachedOptionElement = optionCache.get(matchingElement);
        if (cachedOptionElement != null) {
          allOptions.add(cachedOptionElement);
        } else {
          allOptions.addAll(messageElement.getOptions());
        }
      } else {
        metaName = CONFLUENT_FIELD_META;
        matchingElement = findMatchingElement(original, identifiers, true);
        FieldElement fieldElement = (FieldElement) matchingElement;
        entityNode = findMatchingNode(node, identifiers, true);
        OptionElement cachedOptionElement = optionCache.get(matchingElement);
        if (cachedOptionElement != null) {
          allOptions.add(cachedOptionElement);
        } else {
          allOptions.addAll(fieldElement.getOptions());
        }
      }

      Set<String> tagsToAdd = tagsToAddMap.get(entity);
      if (tagsToAdd != null && !tagsToAdd.isEmpty()) {
        OptionElement newOption = new OptionElement(metaName, Kind.OPTION,
            new OptionElement(TAGS_FIELD, Kind.LIST, new ArrayList<>(tagsToAdd), false), true);
        allOptions.add(newOption);
      }
      mergedOptions = mergeOptions(allOptions);

      Set<String> tagsToRemove = tagsToRemoveMap.get(entity);
      if (tagsToRemove != null && !tagsToRemove.isEmpty()) {
        OptionElement metaOptionElement = mergedOptions.get(metaName);
        if (metaOptionElement != null) {
          LinkedHashMap<String, Object> metaMap =
              new LinkedHashMap<>((Map<String, Object>) metaOptionElement.getValue());
          Object tagsObj = metaMap.get(TAGS_FIELD);
          if (tagsObj != null) {
            List<Object> allTags;
            if (tagsObj instanceof List) {
              allTags = (List<Object>) tagsObj;
            } else {
              allTags = Collections.singletonList(tagsObj);
            }
            List<Object> remainingTags = allTags.stream()
                .filter(tag -> !tagsToRemove.contains(tag))
                .collect(Collectors.toCollection(ArrayList::new));

            if (remainingTags.isEmpty()) {
              metaMap.remove(TAGS_FIELD);
            } else {
              metaMap.put(TAGS_FIELD, remainingTags);
            }

            if (metaMap.isEmpty()) {
              mergedOptions.remove(metaName);
            } else {
              mergedOptions.put(metaName, new OptionElement(metaName, Kind.MAP, metaMap, true));
            }
          }
        }
      }
      optionCache.put(matchingElement, mergedOptions.get(metaName));
      ((ObjectNode) entityNode).replace("options", jsonMapper.valueToTree(mergedOptions.values()));
    }
  }

  // Adapted from java_helpers.cc in protobuf
  public static String underscoresToCamelCase(String input, boolean capitalizeNextLetter) {
    StringBuilder result = new StringBuilder();
    for (int i = 0; i < input.length(); i++) {
      char c = input.charAt(i);
      if ('a' <= c && c <= 'z') {
        if (capitalizeNextLetter) {
          result.append(Character.toUpperCase(c));
        } else {
          result.append(c);
        }
        capitalizeNextLetter = false;
      } else if ('A' <= c && c <= 'Z') {
        if (i == 0 && !capitalizeNextLetter) {
          // Force first letter to lower-case unless explicitly told to
          // capitalize it.
          result.append(Character.toLowerCase(c));
        } else {
          // Capital letters after the first are left as-is.
          result.append(c);
        }
        capitalizeNextLetter = false;
      } else if ('0' <= c && c <= '9') {
        result.append(c);
        capitalizeNextLetter = true;
      } else {
        capitalizeNextLetter = true;
      }
    }
    return result.toString();
  }

  private static List<String> getFieldNames(TypeElement typeElement) {
    if (typeElement instanceof MessageElement) {
      return ((MessageElement) typeElement).getFields()
              .stream()
              .map(FieldElement::getName)
              .collect(Collectors.toList());
    }
    return Collections.emptyList();
  }

  public enum Format {
    DEFAULT("default"),
    IGNORE_EXTENSIONS("ignore_extensions"),
    SERIALIZED("serialized");

    private static final EnumHashBiMap<Format, String> lookup =
        EnumHashBiMap.create(Format.class);

    static {
      for (Format type : Format.values()) {
        lookup.put(type, type.symbol());
      }
    }

    private final String symbol;

    Format(String symbol) {
      this.symbol = symbol;
    }

    public String symbol() {
      return symbol;
    }

    public static Format get(String symbol) {
      return lookup.inverse().get(symbol);
    }

    public static Set<String> symbols() {
      return lookup.inverse().keySet();
    }

    @Override
    public String toString() {
      return symbol();
    }
  }

  public static class ProtobufMeta {
    private final String doc;
    private final Map<String, String> params;
    private final List<String> tags;

    public ProtobufMeta(String doc, Map<String, String> params, List<String> tags) {
      this.doc = doc;
      this.params = params;
      this.tags = tags;
    }

    public String getDoc() {
      return doc;
    }

    public Map<String, String> getParams() {
      return params;
    }

    public List<String> getTags() {
      return tags;
    }

    public boolean isEmpty() {
      return doc == null
          && (params == null || params.isEmpty())
          && (tags == null || tags.isEmpty());
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      ProtobufMeta metadata = (ProtobufMeta) o;
      return Objects.equals(doc, metadata.doc)
          && Objects.equals(params, metadata.params)
          && Objects.equals(tags, metadata.tags);
    }

    @Override
    public int hashCode() {
      return Objects.hash(doc, params, tags);
    }
  }
}
