/*
 * Copyright 2020 Confluent Inc.
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
import org.junit.jupiter.api.Test;

import static io.confluent.kafka.serializers.protobuf.KafkaProtobufSerializerTest.getField;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

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
  public Properties getSchemaRegistryProperties() {
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
    assertEquals(
        config,
        restApp.restClient.updateConfig(config, null),
        "Adding config with initial metadata should succeed"
    );

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
    assertEquals(4, registeredId, "Registering a new schema should succeed");

    SchemaString schemaString = restApp.restClient.getId(4);
    // the newly registered schema should be immediately readable on the leader
    assertNotNull(schemaString, "Registered schema should be found");

    assertEquals(
        2,
        schemaString.getReferences().size(),
        "Schema dependencies should be found"
    );
  }

  @Test
  public void testInvalidSchema() throws Exception {
    assertThrows(RestClientException.class, () -> {
      String schemaString = getInvalidSchema();
      String subject = "invalid";
      registerAndVerifySchema(
          restApp.restClient, schemaString, Collections.emptyList(), 1, subject);
    });
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
        (long) expectedId,
        (long) registeredId,
        "Registering a new schema should succeed"
    );
    assertEquals(
        schemaString.trim(),
        restService.getId(expectedId).getSchemaString().trim(),
        "Registered schema should be found"
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

  /**
   * A schema that consists of nothing but {@code import public "..."} should:
   *   1. register successfully against the SR server,
   *   2. be usable through {@code KafkaProtobufSerializer} /
   *      {@code KafkaProtobufDeserializer} (the resolved Descriptor flows
   *      through the publicly-imported file in both directions), and
   *   3. pass backward-compat checks against an identical public-import file.
   *
   * <p>Pattern mirrors {@code testEnumRoot}: pre-register the imported file
   * and the public-import wrapper, then drive serialize/deserialize through
   * Kafka serializers with {@code USE_LATEST_VERSION} so the serializer picks
   * up the pre-registered wrapper schema for the topic's subject.
   */
  @Test
  public void testImportPublic() throws Exception {
    // 1. Pre-register the imported schema (`com.Foo`) at "leaf-subject".
    String leafSchemaText = "syntax = \"proto3\";\n"
        + "package com;\n"
        + "message Foo {\n"
        + "  string id = 1;\n"
        + "}\n";
    int leafId = restApp.restClient.registerSchema(leafSchemaText,
        ProtobufSchema.TYPE,
        Collections.emptyList(),
        "leaf-subject",
        false).getId();
    assertNotNull(restApp.restClient.getId(leafId));

    // 2. Pre-register an empty public-import wrapper at "test-value" (the
    // topic's subject) that pulls in the imported schema via `import public`.
    // No local types — just package + public import. This is the schema the
    // serializer will use.
    String wrapperSchemaText = "syntax = \"proto3\";\n"
        + "package com;\n"
        + "import public \"leaf.proto\";\n";
    List<SchemaReference> refs = Collections.singletonList(
        new SchemaReference("leaf.proto", "leaf-subject", 1));
    int wrapperId = restApp.restClient.registerSchema(wrapperSchemaText,
        ProtobufSchema.TYPE,
        refs,
        topic + "-value",
        false).getId();
    assertNotNull(restApp.restClient.getId(wrapperId));

    // 3. Configure a serializer that USES the pre-registered wrapper
    // (USE_LATEST_VERSION + AUTO_REGISTER off). Without this, the serializer
    // would try to auto-register the message's own schema and bypass the
    // wrapper entirely.
    Properties serializerConfig = new Properties();
    serializerConfig.put(KafkaProtobufSerializerConfig.AUTO_REGISTER_SCHEMAS, false);
    serializerConfig.put(KafkaProtobufSerializerConfig.USE_LATEST_VERSION, true);
    // The strict-latest check structurally diffs the message's schema against
    // the registered latest. For an empty public-import wrapper, that diff
    // sees a gap (message has Foo, registered file has no types) and refuses.
    // Disabling the strict check is the supported way to use a public-import
    // wrapper as the registered latest.
    serializerConfig.put(KafkaProtobufSerializerConfig.LATEST_COMPATIBILITY_STRICT, false);
    serializerConfig.put(KafkaProtobufSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "bogus");
    SchemaRegistryClient schemaRegistry = new CachedSchemaRegistryClient(restApp.restClient,
        10,
        Collections.singletonList(new ProtobufSchemaProvider()),
        Collections.emptyMap(),
        Collections.emptyMap()
    );
    KafkaProtobufSerializer protobufSerializer = new KafkaProtobufSerializer(schemaRegistry,
        new HashMap(serializerConfig));

    KafkaProtobufDeserializer protobufDeserializer = new KafkaProtobufDeserializer(schemaRegistry);

    // 4. Build a `com.Foo` DynamicMessage via the wrapper's resolved
    // Descriptor (a generated Java class isn't available since `Foo` is
    // declared inline in the test).
    ProtobufSchema wrapper =
        (ProtobufSchema) schemaRegistry.getSchemaBySubjectAndId(topic + "-value", wrapperId);
    Descriptor desc = wrapper.toDescriptor();
    assertEquals("com.Foo", desc.getFullName());
    DynamicMessage.Builder builder = DynamicMessage.newBuilder(desc);
    builder.setField(desc.findFieldByName("id"), "test-id");
    DynamicMessage fooMessage = builder.build();

    // 5. Round-trip through the serializer/deserializer. Bytes carry the
    // wrapper's schema ID; on read, the deserializer fetches the wrapper,
    // resolves its Descriptor through the public import, and parses bytes
    // against `com.Foo`.
    byte[] bytes = protobufSerializer.serialize(topic, fooMessage);
    DynamicMessage roundTripped =
        (DynamicMessage) protobufDeserializer.deserialize(topic, bytes);
    assertEquals("test-id", getField(roundTripped, "id"));

    // 6. Backward compat: re-registering an identical wrapper must succeed
    // (no diff at the file level since both sides have empty local types).
    List<String> compatErrors = restApp.restClient.testCompatibility(
        wrapperSchemaText,
        ProtobufSchema.TYPE,
        refs,
        topic + "-value",
        "latest",
        false);
    assertTrue(compatErrors.isEmpty(),
        "re-registering an identical public-import wrapper should be compatible, got: "
            + compatErrors);
  }

  /**
   * Same shape as {@link #testImportPublic}, but the leaf has two messages and
   * the test serializes the *second* one. Confirms the wrapper's
   * {@code toDescriptor(name)} can find non-first leaf types and that the
   * serializer/deserializer index round-trip lands on the right message.
   */
  @Test
  public void testImportPublicNonFirstMessage() throws Exception {
    String leafSchemaText = "syntax = \"proto3\";\n"
        + "package com;\n"
        + "message Foo {\n"
        + "  string id = 1;\n"
        + "}\n"
        + "message Bar {\n"
        + "  int32 n = 1;\n"
        + "}\n";
    int leafId = restApp.restClient.registerSchema(leafSchemaText,
        ProtobufSchema.TYPE,
        Collections.emptyList(),
        "leaf-subject",
        false).getId();
    assertNotNull(restApp.restClient.getId(leafId));

    String reExportSchemaText = "syntax = \"proto3\";\n"
        + "package com;\n"
        + "import public \"leaf.proto\";\n";
    List<SchemaReference> refs = Collections.singletonList(
        new SchemaReference("leaf.proto", "leaf-subject", 1));
    int reExportId = restApp.restClient.registerSchema(reExportSchemaText,
        ProtobufSchema.TYPE,
        refs,
        topic + "-value",
        false).getId();
    assertNotNull(restApp.restClient.getId(reExportId));

    Properties serializerConfig = new Properties();
    serializerConfig.put(KafkaProtobufSerializerConfig.AUTO_REGISTER_SCHEMAS, false);
    serializerConfig.put(KafkaProtobufSerializerConfig.USE_LATEST_VERSION, true);
    serializerConfig.put(KafkaProtobufSerializerConfig.LATEST_COMPATIBILITY_STRICT, false);
    serializerConfig.put(KafkaProtobufSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "bogus");
    SchemaRegistryClient schemaRegistry = new CachedSchemaRegistryClient(restApp.restClient,
        10,
        Collections.singletonList(new ProtobufSchemaProvider()),
        Collections.emptyMap(),
        Collections.emptyMap()
    );
    KafkaProtobufSerializer protobufSerializer = new KafkaProtobufSerializer(schemaRegistry,
        new HashMap(serializerConfig));
    KafkaProtobufDeserializer protobufDeserializer = new KafkaProtobufDeserializer(schemaRegistry);

    // Build a com.Bar (the *second* message in the leaf). toDescriptor(name)
    // must find Bar through the public import — name() would only return Foo.
    ProtobufSchema reExport =
        (ProtobufSchema) schemaRegistry.getSchemaBySubjectAndId(topic + "-value", reExportId);
    Descriptor barDesc = reExport.toDescriptor("com.Bar");
    assertNotNull(barDesc,
        "expected toDescriptor(\"com.Bar\") to resolve via public import");
    assertEquals("com.Bar", barDesc.getFullName());
    DynamicMessage.Builder builder = DynamicMessage.newBuilder(barDesc);
    builder.setField(barDesc.findFieldByName("n"), 42);
    DynamicMessage barMessage = builder.build();

    // Round-trip. The serializer encodes index [1] (Bar's position in the
    // leaf); the deserializer decodes it back to com.Bar and parses the bytes
    // against the right descriptor.
    byte[] bytes = protobufSerializer.serialize(topic, barMessage);
    DynamicMessage roundTripped =
        (DynamicMessage) protobufDeserializer.deserialize(topic, bytes);
    assertEquals("com.Bar", roundTripped.getDescriptorForType().getFullName());
    assertEquals(42, getField(roundTripped, "n"));
  }

  private static void checkNormalization(SchemaRegistryClient schemaRegistry, String fileName)
      throws Exception {
    Collection<String> subjects = schemaRegistry.getAllSubjects();
    for (String subject : subjects) {
      SchemaMetadata metadata = schemaRegistry.getLatestSchemaMetadata(subject);
      assertEquals(metadata.getSubject(), subject);
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

