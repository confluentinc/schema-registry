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
 */

package io.confluent.kafka.schemaregistry.protobuf.rest;

import com.acme.glup.CommonProto.Consent;
import com.acme.glup.ExampleProtoAcme;
import com.acme.glup.MetadataProto;
import com.acme.glup.MetadataProto.DataSet;
import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.EnumDescriptor;
import com.google.protobuf.Descriptors.EnumValueDescriptor;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.Descriptors.GenericDescriptor;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Timestamp;
import io.confluent.connect.protobuf.test.DescriptorRef;
import io.confluent.kafka.schemaregistry.client.SchemaMetadata;
import io.confluent.kafka.schemaregistry.client.rest.RestService;
import io.confluent.kafka.schemaregistry.client.rest.entities.Metadata;
import io.confluent.kafka.schemaregistry.client.rest.entities.Schema;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaReference;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaString;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.ConfigUpdateRequest;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.RegisterSchemaRequest;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.protobuf.AbstractKafkaProtobufSerializer;
import io.confluent.kafka.serializers.subject.DefaultReferenceSubjectNameStrategy;
import io.confluent.kafka.serializers.subject.strategy.ReferenceSubjectNameStrategy;
import io.confluent.kafka.serializers.subject.strategy.SubjectNameStrategy;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Properties;

import io.confluent.kafka.schemaregistry.ClusterTestHarness;
import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchemaProvider;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchemaUtils;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufDeserializer;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufDeserializerConfig;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufSerializer;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufSerializerConfig;
import io.confluent.kafka.serializers.protobuf.test.DependencyTestProto.DependencyMessage;
import io.confluent.kafka.serializers.protobuf.test.EnumReferenceOuter.EnumReference;
import io.confluent.kafka.serializers.protobuf.test.EnumRootOuter.EnumRoot;
import io.confluent.kafka.serializers.protobuf.test.NestedTestProto.ComplexType;
import io.confluent.kafka.serializers.protobuf.test.NestedTestProto.NestedMessage;
import io.confluent.kafka.serializers.protobuf.test.NestedTestProto.Status;
import io.confluent.kafka.serializers.protobuf.test.NestedTestProto.UserId;
import io.confluent.kafka.serializers.protobuf.test.TestMessageProtos.TestMessage;

import static io.confluent.kafka.serializers.protobuf.KafkaProtobufSerializerTest.getField;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class RestApiSerializerTest extends ClusterTestHarness {

  private final String topic = "test";

  private static final String TEST_MSG_STRING = "Hello World";
  private static final TestMessage HELLO_WORLD_MESSAGE = TestMessage.newBuilder()
      .setTestString(TEST_MSG_STRING)
      .setTestInt32(123)
      .build();
  private static final UserId USER_ID = UserId.newBuilder().setKafkaUserId("user1").build();
  private static final ComplexType COMPLEX_TYPE = ComplexType.newBuilder()
      .setOneId("complex")
      .setIsActive(true)
      .build();
  private static final Timestamp TS = Timestamp.newBuilder()
      .setSeconds(1000)
      .setNanos(2000)
      .build();
  private static final NestedMessage NESTED_MESSAGE = NestedMessage.newBuilder()
      .setUserId(USER_ID)
      .setIsActive(true)
      .addExperimentsActive("first")
      .addExperimentsActive("second")
      .setUpdatedAt(TS)
      .setStatus(Status.ACTIVE)
      .setComplexType(COMPLEX_TYPE)
      .putMapType("key1", "value1")
      .putMapType("key2", "value2")
      .build();
  private static final DependencyMessage DEPENDENCY_MESSAGE = DependencyMessage.newBuilder()
      .setNestedMessage(NESTED_MESSAGE)
      .setIsActive(true)
      .setTestMesssage(HELLO_WORLD_MESSAGE)
      .build();

  private static final MetadataProto.Origin ORIGIN_MESSAGE = MetadataProto.Origin.newBuilder()
      .setDatacenter(MetadataProto.DataCenter.AM5)
      .setIp4(1)
      .setHostname("myhost")
      .setContainerTask("mytask")
      .setContainerApp("myapp")
      .build();
  private static final MetadataProto.Partition PARTITION_MESSAGE =
      MetadataProto.Partition.newBuilder()
      .setTimestampSeconds(1L)
      .setHostPlatform(MetadataProto.Platform.US)
      .setEventType(MetadataProto.EventType.Basket)
      .build();
  private static final MetadataProto.ControlMessage.Watermark WATERMARK_MESSAGE =
      MetadataProto.ControlMessage.Watermark
      .newBuilder()
      .setType("mytype")
      .setHostname("myhost")
      .setKafkaTopic("mytopic")
      .setPartition(1)
      .setPartitionCount(1)
      .setProcessUuid(ByteString.EMPTY)
      .setRegion("myregion")
      .setTimestampSeconds(1)
      .setCluster("mycluster")
      .setEnvironment("myenv")
      .build();
  private static final ExampleProtoAcme.ClickCas CLICK_CAS_MESSAGE =
      ExampleProtoAcme.ClickCas.newBuilder()
      .setGlupOrigin(ORIGIN_MESSAGE)
      .setPartition(PARTITION_MESSAGE)
      .setUid("myuid")
      .addControlMessage(WATERMARK_MESSAGE)
      .build();
  private static final Consent CONSENT_MESSAGE =
      Consent.newBuilder()
          .setIdentificationForbidden(true)
          .build();
  private static final DataSet DATA_SET_MESSAGE =
      DataSet.newBuilder()
          .setId("1")
          .build();
  private static final EnumReference ENUM_REF =
      EnumReference.newBuilder().setEnumRoot(EnumRoot.GOODBYE).build();


  public RestApiSerializerTest() {
    super(1, true);
  }

  @Override
  protected Properties getSchemaRegistryProperties() {
    Properties props = new Properties();
    props.setProperty("schema.providers", ProtobufSchemaProvider.class.getName());
    return props;
  }

  @Test
  public void testDependency() throws Exception {
    Properties serializerConfig = new Properties();
    serializerConfig.put(KafkaProtobufSerializerConfig.AUTO_REGISTER_SCHEMAS, true);
    serializerConfig.put(KafkaProtobufSerializerConfig.NORMALIZE_SCHEMAS, true);
    serializerConfig.put(KafkaProtobufSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "bogus");
    serializerConfig.put(KafkaProtobufSerializerConfig.SKIP_KNOWN_TYPES_CONFIG, false);
    SchemaRegistryClient schemaRegistry = new CachedSchemaRegistryClient(restApp.restClient,
        10,
        Collections.singletonList(new ProtobufSchemaProvider()),
        Collections.emptyMap(),
        Collections.emptyMap()
    );
    KafkaProtobufSerializer protobufSerializer = new KafkaProtobufSerializer(schemaRegistry,
        new HashMap(serializerConfig)
    );

    KafkaProtobufDeserializer protobufDeserializer = new KafkaProtobufDeserializer(schemaRegistry);

    Properties dependencyMessageDeserializerConfig = new Properties();
    dependencyMessageDeserializerConfig.put(
        KafkaProtobufDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG,
        "bogus"
    );
    KafkaProtobufDeserializer dependencyMessageDeserializer = new KafkaProtobufDeserializer(
        schemaRegistry,
        new HashMap(dependencyMessageDeserializerConfig),
        DependencyMessage.class
    );

    byte[] bytes;

    // specific -> specific
    bytes = protobufSerializer.serialize(topic, DEPENDENCY_MESSAGE);
    assertEquals(DEPENDENCY_MESSAGE, dependencyMessageDeserializer.deserialize(topic, bytes));

    // specific -> dynamic
    bytes = protobufSerializer.serialize(topic, DEPENDENCY_MESSAGE);
    DynamicMessage message = (DynamicMessage) protobufDeserializer.deserialize(topic, bytes);
    assertEquals(DEPENDENCY_MESSAGE.getNestedMessage().getUserId().getKafkaUserId(),
        getField((DynamicMessage) getField((DynamicMessage) getField(message, "nested_message"),
            "user_id"
        ), "kafka_user_id")
    );

    ParsedSchema schema = schemaRegistry.getSchemaBySubjectAndId("test-value", 6);
    assertEquals(ProtobufSchemaUtils.getSchema(DEPENDENCY_MESSAGE).canonicalString(),
        schema.canonicalString()
    );

    checkNormalization(schemaRegistry, "DependencyTestProto.proto");
  }

  @Test
  public void testDependencyPreregister() throws Exception {
    Properties serializerConfig = new Properties();
    serializerConfig.put(KafkaProtobufSerializerConfig.AUTO_REGISTER_SCHEMAS, true);
    serializerConfig.put(KafkaProtobufSerializerConfig.NORMALIZE_SCHEMAS, true);
    serializerConfig.put(KafkaProtobufSerializerConfig.REFERENCE_LOOKUP_ONLY_CONFIG, true);
    serializerConfig.put(KafkaProtobufSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "bogus");
    serializerConfig.put(KafkaProtobufSerializerConfig.SKIP_KNOWN_TYPES_CONFIG, true);
    SchemaRegistryClient schemaRegistry = new CachedSchemaRegistryClient(restApp.restClient,
        10,
        Collections.singletonList(new ProtobufSchemaProvider()),
        Collections.emptyMap(),
        Collections.emptyMap()
    );
    KafkaProtobufSerializer protobufSerializer = new KafkaProtobufSerializer(schemaRegistry,
        new HashMap(serializerConfig)
    );
    serializerConfig.put(AbstractKafkaSchemaSerDeConfig.VALUE_SUBJECT_NAME_STRATEGY, TestReferenceNameStrategy.class);
    KafkaProtobufSerializer referenceSerializer = new KafkaProtobufSerializer(schemaRegistry,
        new HashMap(serializerConfig)
    );

    Properties deserializerConfig = new Properties();
    deserializerConfig.put(
        KafkaProtobufDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG,
        "bogus"
    );
    KafkaProtobufDeserializer dataSetMessageDeserializer = new KafkaProtobufDeserializer(
        schemaRegistry,
        new HashMap(deserializerConfig),
        DataSet.class
    );
    KafkaProtobufDeserializer consentMessageDeserializer = new KafkaProtobufDeserializer(
        schemaRegistry,
        new HashMap(deserializerConfig),
        Consent.class
    );

    byte[] bytes;

    // first manually register the reference
    bytes = referenceSerializer.serialize(topic, DATA_SET_MESSAGE);
    assertEquals(DATA_SET_MESSAGE, dataSetMessageDeserializer.deserialize(topic, bytes));

    // specific -> specific
    bytes = protobufSerializer.serialize(topic, CONSENT_MESSAGE);
    assertEquals(CONSENT_MESSAGE, consentMessageDeserializer.deserialize(topic, bytes));

    checkNormalization(schemaRegistry, "common_proto.proto");
  }

  public static class TestReferenceNameStrategy implements SubjectNameStrategy {
    public void configure(Map<String, ?> configs) {
    }

    public String subjectName(String topic, boolean isKey, ParsedSchema schema) {
      if (schema.name().endsWith("DataSet")) {
        return "metadata_proto.proto";
      } else {
        throw new IllegalArgumentException();
      }
    }
  }

  @Test
  public void testDependency2() throws Exception {
    Properties serializerConfig = new Properties();
    serializerConfig.put(KafkaProtobufSerializerConfig.AUTO_REGISTER_SCHEMAS, true);
    serializerConfig.put(KafkaProtobufSerializerConfig.NORMALIZE_SCHEMAS, true);
    serializerConfig.put(KafkaProtobufSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "bogus");
    serializerConfig.put(KafkaProtobufSerializerConfig.SKIP_KNOWN_TYPES_CONFIG, false);
    SchemaRegistryClient schemaRegistry = new CachedSchemaRegistryClient(restApp.restClient,
        10,
        Collections.singletonList(new ProtobufSchemaProvider()),
        Collections.emptyMap(),
        Collections.emptyMap()
    );
    KafkaProtobufSerializer protobufSerializer = new KafkaProtobufSerializer(schemaRegistry,
        new HashMap(serializerConfig)
    );

    KafkaProtobufDeserializer protobufDeserializer = new KafkaProtobufDeserializer(schemaRegistry);

    Properties clickCasDeserializerConfig = new Properties();
    clickCasDeserializerConfig.put(
        KafkaProtobufDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG,
        "bogus"
    );
    KafkaProtobufDeserializer clickCasDeserializer = new KafkaProtobufDeserializer(
        schemaRegistry,
        new HashMap(clickCasDeserializerConfig),
        ExampleProtoAcme.ClickCas.class
    );

    byte[] bytes;

    // specific -> specific
    bytes = protobufSerializer.serialize(topic, CLICK_CAS_MESSAGE);
    assertEquals(CLICK_CAS_MESSAGE, clickCasDeserializer.deserialize(topic, bytes));

    // specific -> dynamic
    bytes = protobufSerializer.serialize(topic, CLICK_CAS_MESSAGE);
    DynamicMessage message = (DynamicMessage) protobufDeserializer.deserialize(topic, bytes);
    assertEquals(CLICK_CAS_MESSAGE.getGlupOrigin().getHostname(),
        getField((DynamicMessage) getField(message, "glup_origin"), "hostname")
    );

    ParsedSchema schema = schemaRegistry.getSchemaBySubjectAndId("test-value", 4);
    assertEquals(ProtobufSchemaUtils.getSchema(CLICK_CAS_MESSAGE).normalize().canonicalString(),
        schema.normalize().canonicalString()
    );

    checkNormalization(schemaRegistry, "exampleProtoAcme.proto");
  }

  @Test
  public void testWellKnownType() throws Exception {
    String schemaString = getSchemaWithWellKnownType();
    String subject = "wellknown";
    registerAndVerifySchema(
        restApp.restClient, schemaString, Collections.emptyList(), 1, subject, true);

    DescriptorRef.DescriptorMessage descMessage =
        DescriptorRef.DescriptorMessage.newBuilder()
            .setKey(123)
            .setValue(DescriptorRef.DescriptorMessage.getDescriptor().toProto())
            .build();

    Properties serializerConfig = new Properties();
    serializerConfig.put(KafkaProtobufSerializerConfig.AUTO_REGISTER_SCHEMAS, true);
    serializerConfig.put(KafkaProtobufSerializerConfig.NORMALIZE_SCHEMAS, true);
    serializerConfig.put(KafkaProtobufSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "bogus");
    SchemaRegistryClient schemaRegistry = new CachedSchemaRegistryClient(restApp.restClient,
        10,
        Collections.singletonList(new ProtobufSchemaProvider()),
        Collections.emptyMap(),
        Collections.emptyMap()
    );
    KafkaProtobufSerializer protobufSerializer = new KafkaProtobufSerializer(schemaRegistry,
        new HashMap(serializerConfig)
    );

    KafkaProtobufDeserializer protobufDeserializer = new KafkaProtobufDeserializer(schemaRegistry);

    Properties deserializerConfig = new Properties();
    deserializerConfig.put(
        KafkaProtobufDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG,
        "bogus"
    );
    KafkaProtobufDeserializer descMessageDeserializer = new KafkaProtobufDeserializer(
        schemaRegistry,
        new HashMap(deserializerConfig),
        DescriptorRef.DescriptorMessage.class
    );

    byte[] bytes;

    // specific -> specific
    bytes = protobufSerializer.serialize(topic, descMessage);
    assertEquals(descMessage, descMessageDeserializer.deserialize(topic, bytes));

    // specific -> dynamic
    bytes = protobufSerializer.serialize(topic, descMessage);
    DynamicMessage message = (DynamicMessage) protobufDeserializer.deserialize(topic, bytes);
    assertEquals(descMessage.getKey(), getField(message, "key"));

    ParsedSchema schema = schemaRegistry.getSchemaBySubjectAndId(subject, 1);
    assertEquals(ProtobufSchemaUtils.getSchema(descMessage).normalize().canonicalString(),
        schema.normalize().canonicalString()
    );

    // additional resolve dependencies check
    ReferenceSubjectNameStrategy strategy = new DefaultReferenceSubjectNameStrategy();
    ProtobufSchema resolvedSchema = ProtobufSchemaUtils.getSchema(descMessage);
    resolvedSchema = KafkaProtobufSerializer.resolveDependencies(
        schemaRegistry, false, false, true, null, true, strategy, subject, false, resolvedSchema);
    assertEquals(schema.normalize(), resolvedSchema.normalize());

    checkNormalization(schemaRegistry, "DescriptorRef.proto");
  }

  @Test
  public void testSchemaReferencesConfigMetadata() throws Exception {
    Map<String, String> properties = new HashMap<>();
    properties.put("configKey", "configValue");
    Metadata metadata = new Metadata(null, properties, null);
    ConfigUpdateRequest config = new ConfigUpdateRequest();
    config.setDefaultMetadata(metadata);
    // add config metadata
    assertEquals("Adding config with initial metadata should succeed",
        config,
        restApp.restClient.updateConfig(config, null));

    SchemaRegistryClient schemaRegistry = new CachedSchemaRegistryClient(restApp.restClient,
        10,
        Collections.singletonList(new ProtobufSchemaProvider()),
        Collections.emptyMap(),
        Collections.emptyMap());
    ProtobufSchema schema = ProtobufSchemaUtils.getSchema(DEPENDENCY_MESSAGE);
    schema = AbstractKafkaProtobufSerializer.resolveDependencies(
        schemaRegistry, true, false, false, null,
        new DefaultReferenceSubjectNameStrategy(), "referrer", false, schema);

    RegisterSchemaRequest request = new RegisterSchemaRequest(schema);
    int registeredId = restApp.restClient.registerSchema(request, "referrer", false).getId();
    assertEquals("Registering a new schema should succeed", 4, registeredId);

    SchemaString schemaString = restApp.restClient.getId(4);
    // the newly registered schema should be immediately readable on the leader
    assertNotNull("Registered schema should be found", schemaString);

    assertEquals("Schema dependencies should be found",
        2,
        schemaString.getReferences().size()
    );
  }

  @Test(expected = RestClientException.class)
  public void testInvalidSchema() throws Exception {
    String schemaString = getInvalidSchema();
    String subject = "invalid";
    registerAndVerifySchema(
        restApp.restClient, schemaString, Collections.emptyList(), 1, subject);
  }

  private static String getInvalidSchema() {
    String schema = "syntax = \"proto3\";\n"
        + "\n"
        + "option java_outer_classname = \"InvalidSchema\";\n"
        + "option java_package = \"io.confluent.connect.protobuf.test\";\n"
        + "\n"
        + "message MyMessage {\n"
        + "  int32 key = 1;\n"
        + "  .org.unknown.BadMessage value = 2;\n"
        + "}";
    return schema;
  }

  public static void registerAndVerifySchema(
      RestService restService,
      String schemaString,
      List<SchemaReference> references,
      int expectedId,
      String subject
  ) throws IOException, RestClientException {
    registerAndVerifySchema(restService, schemaString, references, expectedId, subject, false);
  }

  public static void registerAndVerifySchema(
      RestService restService,
      String schemaString,
      List<SchemaReference> references,
      int expectedId,
      String subject,
      boolean normalize
  ) throws IOException, RestClientException {
    int registeredId = restService.registerSchema(schemaString,
        ProtobufSchema.TYPE,
        references,
        subject,
        normalize
    ).getId();
    assertEquals(
        "Registering a new schema should succeed",
        (long) expectedId,
        (long) registeredId
    );
    assertEquals(
        "Registered schema should be found",
        schemaString.trim(),
        restService.getId(expectedId).getSchemaString().trim()
    );
  }

  private static String getSchemaWithWellKnownType() {
    String schema = "syntax = \"proto3\";\n"
        + "\n"
        + "import \"google/protobuf/descriptor.proto\";\n"
        + "\n"
        + "option java_outer_classname = \"DescriptorRef\";\n"
        + "option java_package = \"io.confluent.connect.protobuf.test\";\n"
        + "\n"
        + "message DescriptorMessage {\n"
        + "  int32 key = 1;\n"
        + "  .google.protobuf.DescriptorProto value = 2;\n"
        + "}";
    return schema;
  }

  @Test
  public void testEnumRoot() throws Exception {
    Properties serializerConfig = new Properties();
    serializerConfig.put(KafkaProtobufSerializerConfig.AUTO_REGISTER_SCHEMAS, true);
    serializerConfig.put(KafkaProtobufSerializerConfig.NORMALIZE_SCHEMAS, true);
    serializerConfig.put(KafkaProtobufSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "bogus");
    SchemaRegistryClient schemaRegistry = new CachedSchemaRegistryClient(restApp.restClient,
        10,
        Collections.singletonList(new ProtobufSchemaProvider()),
        Collections.emptyMap(),
        Collections.emptyMap()
    );
    KafkaProtobufSerializer protobufSerializer = new KafkaProtobufSerializer(schemaRegistry,
        new HashMap(serializerConfig)
    );

    KafkaProtobufDeserializer protobufDeserializer = new KafkaProtobufDeserializer(schemaRegistry);

    Properties enumRefDeserializerConfig = new Properties();
    enumRefDeserializerConfig.put(
        KafkaProtobufDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG,
        "bogus"
    );
    KafkaProtobufDeserializer enumRefDeserializer = new KafkaProtobufDeserializer(
        schemaRegistry,
        new HashMap(enumRefDeserializerConfig),
        EnumReference.class
    );

    byte[] bytes;

    // specific -> specific
    bytes = protobufSerializer.serialize(topic, ENUM_REF);
    assertEquals(ENUM_REF, enumRefDeserializer.deserialize(topic, bytes));

    // specific -> dynamic
    bytes = protobufSerializer.serialize(topic, ENUM_REF);
    DynamicMessage message = (DynamicMessage) protobufDeserializer.deserialize(topic, bytes);
    assertEquals(ENUM_REF.getEnumRoot().name(), ((EnumValueDescriptor) getField(message, "enum_root")).getName());

    ParsedSchema schema = schemaRegistry.getSchemaBySubjectAndId("test-value", 1);
    assertEquals(new ProtobufSchema("syntax = \"proto3\";\n"
            + "\n"
            + "option java_outer_classname = \"EnumRootOuter\";\n"
            + "option java_package = \"io.confluent.kafka.serializers.protobuf.test\";\n"
            + "\n"
            + "enum EnumRoot {\n"
            + "  HELLO = 0;\n"
            + "  GOODBYE = 1;\n"
            + "}").canonicalString(),
        schema.canonicalString()
    );
    assertEquals("EnumRoot", schema.name());

    schema = schemaRegistry.getSchemaBySubjectAndId("test-value", 2);
    assertEquals(ProtobufSchemaUtils.getSchema(ENUM_REF).normalize().canonicalString(),
        schema.normalize().canonicalString()
    );

    checkNormalization(schemaRegistry, "EnumReference.proto");
  }

  private static void checkNormalization(SchemaRegistryClient schemaRegistry, String fileName)
      throws Exception {
    Collection<String> subjects = schemaRegistry.getAllSubjects();
    for (String subject : subjects) {
      SchemaMetadata metadata = schemaRegistry.getLatestSchemaMetadata(subject);
      Optional<ParsedSchema> schema = schemaRegistry.parseSchema(new Schema(subject, metadata));
      ProtobufSchema proto = (ProtobufSchema) schema.get();
      GenericDescriptor d = proto.toSpecificDescriptor(
          subject.endsWith(".proto") ? subject : fileName);
      ProtobufSchema proto2;
      if (d instanceof EnumDescriptor) {
        proto2 = new ProtobufSchema((EnumDescriptor) d);
      } else if (d instanceof Descriptor){
        proto2 = new ProtobufSchema((Descriptor) d);
      } else if (d instanceof FileDescriptor) {
        proto2 = new ProtobufSchema((FileDescriptor) d);
      } else {
        throw new IllegalArgumentException();
      }
      proto2 = proto2.normalize();
      assertEquals(metadata.getSchema(), proto2.canonicalString());
    }
  }
}

