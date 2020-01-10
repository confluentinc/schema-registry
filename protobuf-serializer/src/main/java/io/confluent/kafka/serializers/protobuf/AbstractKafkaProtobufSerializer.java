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

import com.google.protobuf.MessageLite;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.errors.SerializationException;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;

import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchemaProvider;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchemaUtils;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDe;

public abstract class AbstractKafkaProtobufSerializer<T extends MessageLite>
    extends AbstractKafkaSchemaSerDe {

  protected boolean autoRegisterSchema;

  protected void configure(KafkaProtobufSerializerConfig config) {
    configureClientProperties(config, new ProtobufSchemaProvider());
    this.autoRegisterSchema = config.autoRegisterSchema();
  }

  protected KafkaProtobufSerializerConfig serializerConfig(Map<String, ?> props) {
    try {
      return new KafkaProtobufSerializerConfig(props);
    } catch (io.confluent.common.config.ConfigException e) {
      throw new ConfigException(e.getMessage());
    }
  }

  protected byte[] serializeImpl(
      String subject, T object, ProtobufSchema schema
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
      schema = ProtobufSchemaUtils.resolveDependencies(schemaRegistry, autoRegisterSchema, schema);
      int id;
      if (autoRegisterSchema) {
        restClientErrorMsg = "Error registering Protobuf schema: ";
        id = schemaRegistry.register(subject, schema);
      } else {
        restClientErrorMsg = "Error retrieving Protobuf schema: ";
        id = schemaRegistry.getId(subject, schema);
      }
      ByteArrayOutputStream out = new ByteArrayOutputStream();
      out.write(MAGIC_BYTE);
      out.write(ByteBuffer.allocate(idSize).putInt(id).array());
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

}
