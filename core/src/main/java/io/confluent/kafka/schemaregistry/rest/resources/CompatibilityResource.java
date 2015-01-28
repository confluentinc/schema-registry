/*
 * Copyright 2015 Confluent Inc.
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

import java.util.HashMap;
import java.util.Map;

import javax.ws.rs.ClientErrorException;
import javax.ws.rs.Consumes;
import javax.ws.rs.HeaderParam;
import javax.ws.rs.NotFoundException;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.container.Suspended;
import javax.ws.rs.core.Response;

import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.avro.AvroUtils;
import io.confluent.kafka.schemaregistry.client.rest.Versions;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.CompatibilityCheckResponse;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.RegisterSchemaRequest;
import io.confluent.kafka.schemaregistry.rest.entities.Schema;
import io.confluent.kafka.schemaregistry.storage.KafkaSchemaRegistry;
import io.confluent.kafka.schemaregistry.storage.exceptions.SchemaRegistryException;

@Path("/compatibility")
@Produces({Versions.SCHEMA_REGISTRY_V1_JSON_WEIGHTED,
           Versions.SCHEMA_REGISTRY_DEFAULT_JSON_WEIGHTED,
           Versions.JSON_WEIGHTED})
@Consumes({Versions.SCHEMA_REGISTRY_V1_JSON,
           Versions.SCHEMA_REGISTRY_DEFAULT_JSON,
           Versions.JSON, Versions.GENERIC_REQUEST})
public class CompatibilityResource {

  public final static String MESSAGE_SCHEMA_NOT_FOUND = "Schema not found.";
  private static final Logger log = LoggerFactory.getLogger(CompatibilityResource.class);
  private final KafkaSchemaRegistry schemaRegistry;

  public CompatibilityResource(KafkaSchemaRegistry schemaRegistry) {
    this.schemaRegistry = schemaRegistry;
  }

  @POST
  @Path("/subjects/{subject}/versions/{version}")
  public void getSchemaUnderSubject(final @Suspended AsyncResponse asyncResponse,
                                    final @HeaderParam("Content-Type") String contentType,
                                    final @HeaderParam("Accept") String accept,
                                    @PathParam("subject") String subject,
                                    @PathParam("version") String version,
                                    RegisterSchemaRequest request) {
    // returns true if posted schema is compatible with the specified version. "latest" is 
    // a special version
    Map<String, String> headerProperties = new HashMap<String, String>();
    headerProperties.put("Content-Type", contentType);
    headerProperties.put("Accept", accept);
    Schema schema = new Schema(subject, 0, 0, request.getSchema());
    boolean isCompatible = false;
    CompatibilityCheckResponse compatibilityCheckResponse = new CompatibilityCheckResponse();
    try {
      Schema schemaForSpecifiedVersion = null;
      if (version.trim().toLowerCase().equals("latest")) {
        schemaForSpecifiedVersion = schemaRegistry.getLatestVersion(subject);
      } else {
        int versionId = Integer.valueOf(version.trim());
        schemaForSpecifiedVersion = schemaRegistry.get(subject, versionId);
      }
      if (schemaForSpecifiedVersion == null) {
        if (version.trim().toLowerCase().equals("latest")) {
          isCompatible = true;
          compatibilityCheckResponse.setIsCompatible(isCompatible);
          asyncResponse.resume(compatibilityCheckResponse);
        } else {
          throw new NotFoundException(MESSAGE_SCHEMA_NOT_FOUND);
        }
      } else {
        AvroSchema avroSchemaForSpecifiedVersion =
            AvroUtils.parseSchema(schemaForSpecifiedVersion.getSchema());
        AvroSchema avroInputSchema =
            AvroUtils.parseSchema(schema.getSchema());
        isCompatible =
            schemaRegistry.isCompatible(subject, avroInputSchema, avroSchemaForSpecifiedVersion);
        compatibilityCheckResponse.setIsCompatible(isCompatible);
        asyncResponse.resume(compatibilityCheckResponse);
      }
    } catch (SchemaRegistryException e) {
      throw new ClientErrorException(Response.Status.INTERNAL_SERVER_ERROR, e);
    }
  }
}
