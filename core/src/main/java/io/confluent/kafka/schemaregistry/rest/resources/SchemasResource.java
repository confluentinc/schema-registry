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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import javax.ws.rs.ClientErrorException;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.HeaderParam;
import javax.ws.rs.NotFoundException;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.container.Suspended;
import javax.ws.rs.core.Response;

import io.confluent.kafka.schemaregistry.client.rest.Versions;
import io.confluent.kafka.schemaregistry.rest.entities.Schema;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.RegisterSchemaRequest;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.RegisterSchemaResponse;
import io.confluent.kafka.schemaregistry.storage.KafkaSchemaRegistry;
import io.confluent.kafka.schemaregistry.storage.exceptions.SchemaRegistryException;

@Path("/subjects/{subject}/versions")
@Produces({Versions.SCHEMA_REGISTRY_V1_JSON_WEIGHTED,
           Versions.SCHEMA_REGISTRY_DEFAULT_JSON_WEIGHTED,
           Versions.JSON_WEIGHTED})
@Consumes({Versions.SCHEMA_REGISTRY_V1_JSON,
           Versions.SCHEMA_REGISTRY_DEFAULT_JSON,
           Versions.JSON, Versions.GENERIC_REQUEST})
public class SchemasResource {

  public final static String MESSAGE_SUBJECT_NOT_FOUND = "Subject not found.";
  public final static String MESSAGE_SCHEMA_NOT_FOUND = "Schema not found.";
  private static final Logger log = LoggerFactory.getLogger(SchemasResource.class);

  private final KafkaSchemaRegistry schemaRegistry;

  public SchemasResource(KafkaSchemaRegistry registry) {
    this.schemaRegistry = registry;
  }

  @GET
  @Path("/{version}")
  public Schema getSchema(@PathParam("subject") String subject,
                          @PathParam("version") Integer version) {
    subject = cleanSubjectName(subject);
    Schema schema = null;
    try {
      schema = schemaRegistry.get(subject, version);
    } catch (SchemaRegistryException e) {
      log.debug("Error while retrieving schema for subject " + subject + " with version " +
                version + " from the schema registry", e);
      throw new ClientErrorException(Response.Status.INTERNAL_SERVER_ERROR, e);
    }
    if (schema == null) {
      throw new NotFoundException(MESSAGE_SCHEMA_NOT_FOUND);
    }
    return schema;
  }

  @GET
  public List<Integer> list(@PathParam("subject") String subject) {
    subject = cleanSubjectName(subject);
    Iterator<Schema> allSchemasForThisTopic = null;
    List<Integer> allVersions = new ArrayList<Integer>();
    try {
      allSchemasForThisTopic = schemaRegistry.getAllVersions(subject);
    } catch (SchemaRegistryException e) {
      throw new ClientErrorException(Response.Status.INTERNAL_SERVER_ERROR, e);
    }
    while (allSchemasForThisTopic.hasNext()) {
      Schema schema = allSchemasForThisTopic.next();
      allVersions.add(schema.getVersion());
    }
    return allVersions;
  }

  /**
   * @throws {@link io.confluent.kafka.schemaregistry.client.rest.exceptions.InvalidAvroException ,
   *                IncompatibleAvroSchemaException}
   */
  @POST
  public void register(final @Suspended AsyncResponse asyncResponse,
                       final @HeaderParam("Content-Type") String contentType,
                       final @HeaderParam("Accept") String accept,
                       @PathParam("subject") String subjectName,
                       @QueryParam("dry_run") String dryRun,
                       RegisterSchemaRequest request) {

    subjectName = cleanSubjectName(subjectName);
    Map<String, String> headerProperties = new HashMap<String, String>();
    headerProperties.put("Content-Type", contentType);
    headerProperties.put("Accept", accept);
    Schema schema = new Schema(subjectName, 0, 0, request.getSchema());
    int id = 0;
    // note that parseBoolean(null) returns false which is what we want
    boolean isDryRun = Boolean.parseBoolean(dryRun);

    try {
      id = schemaRegistry.registerOrForward(subjectName, schema, headerProperties, isDryRun);
    } catch (SchemaRegistryException e) {
      throw new ClientErrorException(Response.Status.INTERNAL_SERVER_ERROR, e);
    }
    RegisterSchemaResponse registerSchemaResponse = new RegisterSchemaResponse();
    registerSchemaResponse.setId(id);
    asyncResponse.resume(registerSchemaResponse);
  }

  private String cleanSubjectName(final String subjectName) {
    if (subjectName != null) {
      return subjectName.trim();
    } else {
      throw new NotFoundException(MESSAGE_SUBJECT_NOT_FOUND);
    }
  }
}
