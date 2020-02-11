/*
 * Copyright 2018 Confluent Inc.
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

import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.Consumes;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;

import io.confluent.kafka.schemaregistry.client.rest.Versions;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaString;
import io.confluent.kafka.schemaregistry.exceptions.SchemaRegistryException;
import io.confluent.kafka.schemaregistry.exceptions.SchemaRegistryStoreException;
import io.confluent.kafka.schemaregistry.rest.exceptions.Errors;
import io.confluent.kafka.schemaregistry.storage.KafkaSchemaRegistry;
import io.confluent.rest.annotations.PerformanceMetric;

import java.util.Set;

@Path("/schemas")
@Produces({Versions.SCHEMA_REGISTRY_V1_JSON_WEIGHTED,
           Versions.SCHEMA_REGISTRY_DEFAULT_JSON_WEIGHTED,
           Versions.JSON_WEIGHTED})
@Consumes({Versions.SCHEMA_REGISTRY_V1_JSON,
           Versions.SCHEMA_REGISTRY_DEFAULT_JSON,
           Versions.JSON, Versions.GENERIC_REQUEST})
public class SchemasResource {

  private static final Logger log = LoggerFactory.getLogger(SchemasResource.class);
  private final KafkaSchemaRegistry schemaRegistry;

  public SchemasResource(KafkaSchemaRegistry schemaRegistry) {
    this.schemaRegistry = schemaRegistry;
  }

  @GET
  @Path("/ids/{id}")
  @ApiOperation("Get the schema string identified by the input ID.")
  @ApiResponses(value = {
      @ApiResponse(code = 404, message = "Error code 40403 -- Schema not found\n"),
      @ApiResponse(code = 500, message = "Error code 50001 -- Error in the backend data store\n")})
  @PerformanceMetric("schemas.ids.get-schema")
  public SchemaString getSchema(
      @ApiParam(value = "Globally unique identifier of the schema", required = true)
      @PathParam("id") Integer id,
      @DefaultValue("") @QueryParam("format") String format,
      @DefaultValue("false") @QueryParam("fetchMaxId") boolean fetchMaxId) {
    SchemaString schema = null;
    String errorMessage = "Error while retrieving schema with id " + id + " from the schema "
                          + "registry";
    try {
      schema = schemaRegistry.get(id, format, fetchMaxId);
    } catch (SchemaRegistryStoreException e) {
      log.debug(errorMessage, e);
      throw Errors.storeException(errorMessage, e);
    } catch (SchemaRegistryException e) {
      throw Errors.schemaRegistryException(errorMessage, e);
    }
    if (schema == null) {
      throw Errors.schemaNotFoundException(id);
    }
    return schema;
  }

  @GET
  @Path("/ids/{id}/subjects")
  @ApiOperation("Get all the subjects associated with the input ID.")
  @ApiResponses(value = {
      @ApiResponse(code = 404, message = "Error code 40403 -- Schema not found\n"),
      @ApiResponse(code = 500, message = "Error code 50001 -- Error in the backend data store\n")})
  public Set<String> getSubjects(
      @ApiParam(value = "Globally unique identifier of the schema", required = true)
      @PathParam("id") Integer id) {
    Set<String> subjects;
    String errorMessage = "Error while retrieving all subjects associated with schema id "
                          + id + " from the schema registry";

    try {
      subjects = schemaRegistry.listSubjectsForId(id);
    } catch (SchemaRegistryStoreException e) {
      log.debug(errorMessage, e);
      throw Errors.storeException(errorMessage, e);
    } catch (SchemaRegistryException e) {
      throw Errors.schemaRegistryException(errorMessage, e);
    }

    if (subjects == null) {
      throw Errors.schemaNotFoundException();
    }

    return subjects;
  }
}
