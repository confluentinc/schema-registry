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

package io.confluent.kafka.serializers.protobuf;

import com.google.protobuf.Message;
import com.squareup.wire.schema.internal.parser.ProtoFileElement;
import io.confluent.kafka.schemaregistry.ParsedSchema;
import java.io.InterruptedIOException;
import java.util.HashMap;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.errors.InvalidConfigurationException;
import org.apache.kafka.common.errors.SerializationException;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.entities.Schema;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaReference;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.schemaregistry.protobuf.MessageIndexes;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchemaProvider;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDe;
import io.confluent.kafka.serializers.subject.strategy.ReferenceSubjectNameStrategy;
import org.apache.kafka.common.errors.TimeoutException;

public abstract class AbstractKafkaProtobufSerializer<T extends Message>
    extends AbstractKafkaSchemaSerDe {

  protected boolean normalizeSchema;
  protected boolean autoRegisterSchema;
  protected boolean onlyLookupReferencesBySchema;
  protected int useSchemaId = -1;
  protected boolean idCompatStrict;
  protected boolean useLatestVersion;
  protected boolean latestCompatStrict;
  protected String schemaFormat;
  protected boolean skipKnownTypes;
  protected ReferenceSubjectNameStrategy referenceSubjectNameStrategy;

  protected void configure(KafkaProtobufSerializerConfig config) {
    configureClientProperties(config, new ProtobufSchemaProvider());
    this.normalizeSchema = config.normalizeSchema();
    this.autoRegisterSchema = config.autoRegisterSchema();
    this.onlyLookupReferencesBySchema = config.onlyLookupReferencesBySchema();
    this.useSchemaId = config.useSchemaId();
    this.idCompatStrict = config.getIdCompatibilityStrict();
    this.useLatestVersion = config.useLatestVersion();
    this.latestCompatStrict = config.getLatestCompatibilityStrict();
    this.schemaFormat = config.getSchemaFormat();
    this.skipKnownTypes = config.skipKnownTypes();
    this.referenceSubjectNameStrategy = config.referenceSubjectNameStrategyInstance();
  }

  protected KafkaProtobufSerializerConfig serializerConfig(Map<String, ?> props) {
    try {
      return new KafkaProtobufSerializerConfig(props);
    } catch (ConfigException e) {
      throw new ConfigException(e.getMessage());
    }
  }

  protected byte[] serializeImpl(
      String subject, String topic, boolean isKey, T object, ProtobufSchema schema
  ) throws SerializationException, InvalidConfigurationException {
    if (schemaRegistry == null) {
      throw new InvalidConfigurationException(
          "SchemaRegistryClient not found. You need to configure the serializer "
              + "or use serializer constructor with SchemaRegistryClient.");
    }
    // null needs to treated specially since the client most likely just wants to send
    // an individual null value instead of making the subject a null type. Also, null in
    // Kafka has a special meaning for deletion in a topic with the compact retention policy.
    // Therefore, we will bypass schema registration and return a null value in Kafka, instead
    // of an encoded null.
    if (object == null) {
      return null;
    }
    String restClientErrorMsg = "";
    try {
      boolean autoRegisterForDeps = autoRegisterSchema && !onlyLookupReferencesBySchema;
      boolean useLatestForDeps = useLatestVersion && !onlyLookupReferencesBySchema;
      schema = resolveDependencies(schemaRegistry, normalizeSchema, autoRegisterForDeps,
          useLatestForDeps, latestCompatStrict, latestVersions,
          skipKnownTypes, referenceSubjectNameStrategy, topic, isKey, schema);
      int id;
      if (autoRegisterSchema) {
        restClientErrorMsg = "Error registering Protobuf schema: ";
        if (schemaFormat != null) {
          String formatted = schema.formattedString(schemaFormat);
          schema = schema.copyWithSchema(formatted);
        }
        id = schemaRegistry.register(subject, schema, normalizeSchema);
      } else if (useSchemaId >= 0) {
        restClientErrorMsg = "Error retrieving schema ID";
        if (schemaFormat != null) {
          String formatted = schema.formattedString(schemaFormat);
          schema = schema.copyWithSchema(formatted);
        }
        schema = (ProtobufSchema)
            lookupSchemaBySubjectAndId(subject, useSchemaId, schema, idCompatStrict);
        id = schemaRegistry.getId(subject, schema);
      } else if (useLatestVersion) {
        restClientErrorMsg = "Error retrieving latest version: ";
        schema = (ProtobufSchema) lookupLatestVersion(subject, schema, latestCompatStrict);
        id = schemaRegistry.getId(subject, schema);
      } else {
        restClientErrorMsg = "Error retrieving Protobuf schema: ";
        id = schemaRegistry.getId(subject, schema, normalizeSchema);
      }
      ByteArrayOutputStream out = new ByteArrayOutputStream();
      out.write(MAGIC_BYTE);
      out.write(ByteBuffer.allocate(idSize).putInt(id).array());
      MessageIndexes indexes = schema.toMessageIndexes(
          object.getDescriptorForType().getFullName(), normalizeSchema);
      out.write(indexes.toByteArray());
      object.writeTo(out);
      byte[] bytes = out.toByteArray();
      out.close();
      return bytes;
    } catch (InterruptedIOException e) {
      throw new TimeoutException("Error serializing Protobuf message", e);
    } catch (IOException | RuntimeException e) {
      throw new SerializationException("Error serializing Protobuf message", e);
    } catch (RestClientException e) {
      throw toKafkaException(e, restClientErrorMsg + schema);
    }
  }


  /**
   * Resolve schema dependencies recursively.
   *
   * @param schemaRegistry     schema registry client
   * @param autoRegisterSchema whether to automatically register schemas
   * @param useLatestVersion   whether to use the latest subject version for serialization
   * @param latestCompatStrict whether to check that the latest subject version is backward
   *                           compatible with the schema of the object
   * @param latestVersions     an optional cache of latest subject versions, may be null
   * @param strategy           the strategy for determining the subject name for a reference
   * @param topic              the topic
   * @param isKey              whether the object is the record key
   * @param schema             the schema
   * @return the schema with resolved dependencies
   */
  public static ProtobufSchema resolveDependencies(
      SchemaRegistryClient schemaRegistry,
      boolean autoRegisterSchema,
      boolean useLatestVersion,
      boolean latestCompatStrict,
      Map<SubjectSchema, ParsedSchema> latestVersions,
      ReferenceSubjectNameStrategy strategy,
      String topic,
      boolean isKey,
      ProtobufSchema schema
  ) throws IOException, RestClientException {
    return resolveDependencies(
        schemaRegistry,
        autoRegisterSchema,
        useLatestVersion,
        latestCompatStrict,
        latestVersions,
        true,
        strategy,
        topic,
        isKey,
        schema);
  }

  /**
   * Resolve schema dependencies recursively.
   *
   * @param schemaRegistry     schema registry client
   * @param autoRegisterSchema whether to automatically register schemas
   * @param useLatestVersion   whether to use the latest subject version for serialization
   * @param latestCompatStrict whether to check that the latest subject version is backward
   *                           compatible with the schema of the object
   * @param latestVersions     an optional cache of latest subject versions, may be null
   * @param skipKnownTypes     whether to skip known types when resolving schema dependencies
   * @param strategy           the strategy for determining the subject name for a reference
   * @param topic              the topic
   * @param isKey              whether the object is the record key
   * @param schema             the schema
   * @return the schema with resolved dependencies
   */
  public static ProtobufSchema resolveDependencies(
      SchemaRegistryClient schemaRegistry,
      boolean autoRegisterSchema,
      boolean useLatestVersion,
      boolean latestCompatStrict,
      Map<SubjectSchema, ParsedSchema> latestVersions,
      boolean skipKnownTypes,
      ReferenceSubjectNameStrategy strategy,
      String topic,
      boolean isKey,
      ProtobufSchema schema
  ) throws IOException, RestClientException {
    return resolveDependencies(
        schemaRegistry,
        false,
        autoRegisterSchema,
        useLatestVersion,
        latestCompatStrict,
        latestVersions,
        skipKnownTypes,
        strategy,
        topic,
        isKey,
        schema
    );
  }

  /**
   * Resolve schema dependencies recursively.
   *
   * @param schemaRegistry     schema registry client
   * @param normalizeSchema    whether to normalized the schema
   * @param autoRegisterSchema whether to automatically register schemas
   * @param useLatestVersion   whether to use the latest subject version for serialization
   * @param latestCompatStrict whether to check that the latest subject version is backward
   *                           compatible with the schema of the object
   * @param latestVersions     an optional cache of latest subject versions, may be null
   * @param skipKnownTypes     whether to skip known types when resolving schema dependencies
   * @param strategy           the strategy for determining the subject name for a reference
   * @param topic              the topic
   * @param isKey              whether the object is the record key
   * @param schema             the schema
   * @return the schema with resolved dependencies
   */
  public static ProtobufSchema resolveDependencies(
      SchemaRegistryClient schemaRegistry,
      boolean normalizeSchema,
      boolean autoRegisterSchema,
      boolean useLatestVersion,
      boolean latestCompatStrict,
      Map<SubjectSchema, ParsedSchema> latestVersions,
      boolean skipKnownTypes,
      ReferenceSubjectNameStrategy strategy,
      String topic,
      boolean isKey,
      ProtobufSchema schema
  ) throws IOException, RestClientException {
    if (schema.dependencies().isEmpty() || !schema.references().isEmpty()) {
      // Dependencies already resolved
      return schema;
    }
    HashMap<String, ProtoFileElement> dependencies = new HashMap<>(schema.dependencies());
    Schema s = resolveDependencies(schemaRegistry,
        normalizeSchema,
        autoRegisterSchema,
        useLatestVersion,
        latestCompatStrict,
        latestVersions,
        skipKnownTypes,
        strategy,
        topic,
        isKey,
        null,
        schema.rawSchema(),
        dependencies
    );
    return schema.copy(s.getReferences(), dependencies);
  }

  private static Schema resolveDependencies(
      SchemaRegistryClient schemaRegistry,
      boolean normalizeSchema,
      boolean autoRegisterSchema,
      boolean useLatestVersion,
      boolean latestCompatStrict,
      Map<SubjectSchema, ParsedSchema> latestVersions,
      boolean skipKnownTypes,
      ReferenceSubjectNameStrategy strategy,
      String topic,
      boolean isKey,
      String name,
      ProtoFileElement protoFileElement,
      Map<String, ProtoFileElement> dependencies
  ) throws IOException, RestClientException {
    List<SchemaReference> references = new ArrayList<>();
    for (String dep : protoFileElement.getImports()) {
      if (skipKnownTypes && ProtobufSchema.knownTypes().contains(dep)) {
        dependencies.remove(dep);
        continue;
      }
      Schema subschema = resolveDependencies(schemaRegistry,
          normalizeSchema,
          autoRegisterSchema,
          useLatestVersion,
          latestCompatStrict,
          latestVersions,
          skipKnownTypes,
          strategy,
          topic,
          isKey,
          dep,
          dependencies.get(dep),
          dependencies
      );
      references.add(new SchemaReference(dep, subschema.getSubject(), subschema.getVersion()));
    }
    for (String dep : protoFileElement.getPublicImports()) {
      if (skipKnownTypes && ProtobufSchema.knownTypes().contains(dep)) {
        dependencies.remove(dep);
        continue;
      }
      Schema subschema = resolveDependencies(schemaRegistry,
          normalizeSchema,
          autoRegisterSchema,
          useLatestVersion,
          latestCompatStrict,
          latestVersions,
          skipKnownTypes,
          strategy,
          topic,
          isKey,
          dep,
          dependencies.get(dep),
          dependencies
      );
      references.add(new SchemaReference(dep, subschema.getSubject(), subschema.getVersion()));
    }
    ProtobufSchema schema = new ProtobufSchema(protoFileElement, references, dependencies);
    Integer id = null;
    Integer version = null;
    String subject = name != null ? strategy.subjectName(name, topic, isKey, schema) : null;
    if (subject != null) {
      if (autoRegisterSchema) {
        id = schemaRegistry.register(subject, schema, normalizeSchema);
        version = schemaRegistry.getVersion(subject, schema, normalizeSchema);
      } else if (useLatestVersion) {
        schema = (ProtobufSchema) lookupLatestVersion(
            schemaRegistry, subject, schema, latestVersions, latestCompatStrict);
        id = schemaRegistry.getId(subject, schema);
        version = schemaRegistry.getVersion(subject, schema);
      } else {
        id = schemaRegistry.getId(subject, schema, normalizeSchema);
        version = schemaRegistry.getVersion(subject, schema, normalizeSchema);
      }
    }
    return new Schema(
        subject,
        version,
        id,
        schema
    );
  }
}
