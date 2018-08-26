/**
 * Copyright 2014 Confluent Inc.
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

package io.confluent.kafka.schemaregistry.rest.resources;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.UriInfo;

import java.io.IOException;

import io.confluent.kafka.schemaregistry.client.rest.Versions;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaString;
import io.confluent.kafka.schemaregistry.exceptions.SchemaRegistryException;
import io.confluent.kafka.schemaregistry.exceptions.SchemaRegistryStoreException;
import io.confluent.kafka.schemaregistry.rest.exceptions.Errors;
import io.confluent.kafka.schemaregistry.storage.KafkaSchemaRegistry;
import io.confluent.rest.annotations.PerformanceMetric;

import static io.confluent.kafka.schemaregistry.rest.extensions.PrettyQueryExtension.QUERY_PARAM_PRETTY;
import static io.confluent.kafka.schemaregistry.rest.extensions.PrettyQueryExtension.hasPrettyQueryParam;

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
  @PerformanceMetric("schemas.ids.get-schema")
  public SchemaString getSchema(@PathParam("id") Integer id) {
    SchemaString schema = null;
    String errorMessage = "Error while retrieving schema with id " + id + " from the schema "
                          + "registry";
    try {
      schema = schemaRegistry.get(id);
    } catch (SchemaRegistryStoreException e) {
      log.debug(errorMessage, e);
      throw Errors.storeException(errorMessage, e);
    } catch (SchemaRegistryException e) {
      throw Errors.schemaRegistryException(errorMessage, e);
    }
    if (schema == null) {
      throw Errors.schemaNotFoundException();
    }
    return schema;
  }

  @GET
  @Path("/ids/{id}/schema")
  @PerformanceMetric("schemas.ids.get-schema.only")
  public String getSchemaOnly(@PathParam("id") Integer id, @Context UriInfo ui) {
    SchemaString schema = getSchema(id);

    // Since this URL returns a raw string rather than an object, JAX-RS doesn't know
    // it should pretty-print it. Therefore, we manually apply the ObjectMapper pretty-printer
    if (hasPrettyQueryParam(ui)) {
      try {
        return schema.schemaStringToJson(true);
      } catch (IOException e) {
        log.debug(String.format(
                "Unable to apply ?%s query resource handler", QUERY_PARAM_PRETTY), e);
        return schema.getSchemaString();
      }
    }
    return schema.getSchemaString();
  }
}
