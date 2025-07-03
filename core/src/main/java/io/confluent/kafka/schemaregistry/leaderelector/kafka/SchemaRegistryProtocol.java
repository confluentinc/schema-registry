/*
 * Copyright 2017-2021 Confluent Inc.
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

package io.confluent.kafka.schemaregistry.leaderelector.kafka;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.confluent.kafka.schemaregistry.storage.SchemaRegistryIdentity;
import io.confluent.kafka.schemaregistry.utils.JacksonMapper;

import java.io.IOException;
import java.nio.ByteBuffer;


/**
 * This class implements the protocol for Schema Registry instances in a Kafka group. It includes
 * the format of worker state used when joining the group.
 */
class SchemaRegistryProtocol {

  public static final short SR_PROTOCOL_V0 = 0;

  public static ByteBuffer serializeMetadata(SchemaRegistryIdentity identity) {
    return identity.toJsonBytes();
  }

  public static SchemaRegistryIdentity deserializeMetadata(ByteBuffer buffer) {
    return SchemaRegistryIdentity.fromJson(buffer);
  }

  public static ByteBuffer serializeAssignment(Assignment assignment) {
    return assignment.toJsonBytes();
  }

  public static Assignment deserializeAssignment(ByteBuffer buffer) {
    return Assignment.fromJson(buffer);
  }

  @JsonInclude(Include.NON_EMPTY)
  @JsonIgnoreProperties(ignoreUnknown = true)
  public static class Assignment {
    public static final int CURRENT_VERSION = 1;

    public static final short NO_ERROR = 0;
    // Some members have the same URL, which would result in them not being able to forward requests
    // properly. Usually indicates both are running in containers and using the same hostname.
    // User should explicitly set host.name in their configuration to a routable hostname.
    public static final short DUPLICATE_URLS = 1;

    private final int version;
    private final short error;
    private final String leader;
    private final SchemaRegistryIdentity leaderIdentity;

    public Assignment(@JsonProperty("error") short error,
                      @JsonProperty("master") String leader,
                      @JsonProperty("master_identity") SchemaRegistryIdentity leaderIdentity) {
      this.version = CURRENT_VERSION;
      this.error = error;
      this.leader = leader;
      this.leaderIdentity = leaderIdentity;
    }

    public static Assignment fromJson(ByteBuffer json) {
      try {
        byte[] jsonBytes = new byte[json.remaining()];
        json.get(jsonBytes);
        return JacksonMapper.INSTANCE.readValue(jsonBytes, Assignment.class);
      } catch (IOException e) {
        throw new IllegalArgumentException("Error deserializing identity information", e);
      }
    }

    @JsonProperty("version")
    public int version() {
      return version;
    }

    @JsonProperty("error")
    public short error() {
      return error;
    }

    @JsonProperty("master")
    public String leader() {
      return leader;
    }

    @JsonProperty("master_identity")
    public SchemaRegistryIdentity leaderIdentity() {
      return leaderIdentity;
    }

    public boolean failed() {
      return error != NO_ERROR;
    }

    public ByteBuffer toJsonBytes() {
      try {
        return ByteBuffer.wrap(JacksonMapper.INSTANCE.writeValueAsBytes(this));
      } catch (IOException e) {
        throw new IllegalArgumentException("Error serializing identity information", e);
      }
    }

    @Override
    public String toString() {
      return "Assignment{"
             + "version=" + version
             + ", error=" + error
             + ", leader='" + leader + '\''
             + ", leaderIdentity=" + leaderIdentity
             + '}';
    }
  }
}
