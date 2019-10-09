/*
 * Copyright 2019 Confluent Inc.
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

package io.confluent.kafka.schemaregistry.client.rest.entities;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableMap;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;

public class ServerClusterId {

  private static final String KAFKA_CLUSTER = "kafka-cluster";
  private static final String SCHEMA_REGISTRY_CLUSTER = "schema-registry-cluster";

  private final String id = "";
  private final Map<String, Object> scope;

  @JsonCreator
  public ServerClusterId(@JsonProperty("scope") final Map<String, Object> scope) {
    this.scope = ImmutableMap.copyOf(Objects.requireNonNull(scope, "scope"));
  }

  public String getId() {
    return id;
  }

  public Map<String, Object> getScope() {
    return scope;
  }

  public static ServerClusterId of(String kafkaClusterId, String schemaRegistryClusterId) {
    return new ServerClusterId(ImmutableMap.of(
        "path", Collections.emptyList(),
        "clusters", ImmutableMap.of(
            KAFKA_CLUSTER, kafkaClusterId,
            SCHEMA_REGISTRY_CLUSTER, schemaRegistryClusterId)
    ));
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }

    if (!(o instanceof ServerClusterId)) {
      return false;
    }

    ServerClusterId that = (ServerClusterId) o;

    return getId().equals(that.getId())
           && getScope().equals(that.getScope());
  }

  @Override
  public int hashCode() {
    return Objects.hash(getId(), getScope());
  }
}
