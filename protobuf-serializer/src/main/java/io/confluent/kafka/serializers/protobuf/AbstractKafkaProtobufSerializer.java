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
import org.apache.kafka.common.cache.Cache;
import org.apache.kafka.common.config.ConfigException;
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

public abstract class AbstractKafkaProtobufSerializer<T extends Message>
    extends AbstractKafkaSchemaSerDe {

  protected boolean autoRegisterSchema;
  protected boolean useLatestVersion;
  protected ReferenceSubjectNameStrategy referenceSubjectNameStrategy;

  protected void configure(KafkaProtobufSerializerConfig config) {
    configureClientProperties(config, new ProtobufSchemaProvider());
    this.autoRegisterSchema = config.autoRegisterSchema();
    this.useLatestVersion = config.useLatestVersion();
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
  ) throws SerializationException {
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
      schema = resolveDependencies(schemaRegistry, autoRegisterSchema,
          useLatestVersion, latestVersions, referenceSubjectNameStrategy, topic, isKey, schema);
      int id;
      if (autoRegisterSchema) {
        restClientErrorMsg = "Error registering Protobuf schema: ";
        id = schemaRegistry.register(subject, schema);
      } else if (useLatestVersion) {
        restClientErrorMsg = "Error retrieving latest version: ";
        schema = (ProtobufSchema) lookupLatestVersion(subject, schema);
        id = schemaRegistry.getId(subject, schema);
      } else {
        restClientErrorMsg = "Error retrieving Protobuf schema: ";
        id = schemaRegistry.getId(subject, schema);
      }
      ByteArrayOutputStream out = new ByteArrayOutputStream();
      out.write(MAGIC_BYTE);
      out.write(ByteBuffer.allocate(idSize).putInt(id).array());
      MessageIndexes indexes = schema.toMessageIndexes(object.getDescriptorForType().getFullName());
      out.write(indexes.toByteArray());
      object.writeTo(out);
      byte[] bytes = out.toByteArray();
      out.close();
      return bytes;
    } catch (IOException | RuntimeException e) {
      throw new SerializationException("Error serializing Protobuf message", e);
    } catch (RestClientException e) {
      throw new SerializationException(restClientErrorMsg + schema, e);
    }
  }

  // Visible for testing
  public static ProtobufSchema resolveDependencies(
      SchemaRegistryClient schemaRegistry,
      boolean autoRegisterSchema,
      boolean useLatestVersion,
      Cache<SubjectSchema, ParsedSchema> latestVersions,
      ReferenceSubjectNameStrategy strategy,
      String topic,
      boolean isKey,
      ProtobufSchema schema
  ) throws IOException, RestClientException {
    if (schema.dependencies().isEmpty() || !schema.references().isEmpty()) {
      // Dependencies already resolved
      return schema;
    }
    Schema s = resolveDependencies(schemaRegistry,
        autoRegisterSchema,
        useLatestVersion,
        latestVersions,
        strategy,
        topic,
        isKey,
        null,
        schema.rawSchema(),
        schema.dependencies()
    );
    return schema.copy(s.getReferences());
  }

  private static Schema resolveDependencies(
      SchemaRegistryClient schemaRegistry,
      boolean autoRegisterSchema,
      boolean useLatestVersion,
      Cache<SubjectSchema, ParsedSchema> latestVersions,
      ReferenceSubjectNameStrategy strategy,
      String topic,
      boolean isKey,
      String name,
      ProtoFileElement protoFileElement,
      Map<String, ProtoFileElement> dependencies
  ) throws IOException, RestClientException {
    List<SchemaReference> references = new ArrayList<>();
    for (String dep : protoFileElement.getImports()) {
      Schema subschema = resolveDependencies(schemaRegistry,
          autoRegisterSchema,
          useLatestVersion,
          latestVersions,
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
      Schema subschema = resolveDependencies(schemaRegistry,
          autoRegisterSchema,
          useLatestVersion,
          latestVersions,
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
        id = schemaRegistry.register(subject, schema);
      } else if (useLatestVersion) {
        schema = (ProtobufSchema) lookupLatestVersion(
            schemaRegistry, subject, schema, latestVersions);
        id = schemaRegistry.getId(subject, schema);
      } else {
        id = schemaRegistry.getId(subject, schema);
      }
      version = schemaRegistry.getVersion(subject, schema);
    }
    return new Schema(
        subject,
        version,
        id,
        schema.schemaType(),
        schema.references(),
        schema.canonicalString()
    );
  }
}
