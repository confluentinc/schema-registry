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

package io.confluent.kafka.schemaregistry.rest.resources;

import io.confluent.kafka.schemaregistry.client.rest.Versions;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaRegistryServerVersion;
import io.confluent.kafka.schemaregistry.client.rest.entities.ServerClusterId;
import io.confluent.kafka.schemaregistry.storage.KafkaSchemaRegistry;
import io.confluent.kafka.schemaregistry.utils.AppInfoParser;
import io.confluent.rest.annotations.PerformanceMetric;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;

@Path("/v1/metadata")
@Produces({Versions.SCHEMA_REGISTRY_V1_JSON_WEIGHTED,
           Versions.SCHEMA_REGISTRY_DEFAULT_JSON_WEIGHTED,
           Versions.JSON_WEIGHTED})
@Consumes({Versions.SCHEMA_REGISTRY_V1_JSON,
           Versions.SCHEMA_REGISTRY_DEFAULT_JSON,
           Versions.JSON, Versions.GENERIC_REQUEST})
public class ServerMetadataResource {

  private static final Logger log = LoggerFactory.getLogger(ServerMetadataResource.class);
  private final KafkaSchemaRegistry schemaRegistry;

  public ServerMetadataResource(KafkaSchemaRegistry schemaRegistry) {
    this.schemaRegistry = schemaRegistry;
  }

  @GET
  @Path("/id")
  @ApiOperation("Get the server metadata")
  @ApiResponses(value = {
      @ApiResponse(code = 500, message = "Error code 50001 -- Error in the backend data store\n")})
  @PerformanceMetric("metadata.id")
  public ServerClusterId getClusterId() {
    String kafkaClusterId = schemaRegistry.getKafkaClusterId();
    String schemaRegistryClusterId = schemaRegistry.getGroupId();
    return ServerClusterId.of(kafkaClusterId, schemaRegistryClusterId);
  }

  @GET
  @Path("/version")
  @ApiOperation("Get Schema Registry server version")
  @ApiResponses(value = {
      @ApiResponse(code = 500, message = "Error code 50001 -- Error in the backend data store\n")})
  public SchemaRegistryServerVersion getSchemaRegistryVersion() {
    return new SchemaRegistryServerVersion(AppInfoParser.getVersion(), AppInfoParser.getCommitId());
  }
}
