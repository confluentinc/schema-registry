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
import javax.ws.rs.ServerErrorException;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.container.Suspended;
import javax.ws.rs.core.Response;

import io.confluent.kafka.schemaregistry.client.rest.Versions;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.RegisterSchemaRequest;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.RegisterSchemaResponse;
import io.confluent.kafka.schemaregistry.rest.VersionId;
import io.confluent.kafka.schemaregistry.rest.entities.Schema;
import io.confluent.kafka.schemaregistry.rest.exceptions.Errors;
import io.confluent.kafka.schemaregistry.rest.exceptions.InvalidVersionException;
import io.confluent.kafka.schemaregistry.storage.KafkaSchemaRegistry;
import io.confluent.kafka.schemaregistry.storage.exceptions.SchemaRegistryException;
import io.confluent.rest.annotations.PerformanceMetric;

@Produces({Versions.SCHEMA_REGISTRY_V1_JSON_WEIGHTED,
           Versions.SCHEMA_REGISTRY_DEFAULT_JSON_WEIGHTED,
           Versions.JSON_WEIGHTED})
@Consumes({Versions.SCHEMA_REGISTRY_V1_JSON,
           Versions.SCHEMA_REGISTRY_DEFAULT_JSON,
           Versions.JSON, Versions.GENERIC_REQUEST})
public class SubjectVersionsResource {

  private static final Logger log = LoggerFactory.getLogger(SubjectVersionsResource.class);

  private final String subject;
  private final KafkaSchemaRegistry schemaRegistry;

  public SubjectVersionsResource(KafkaSchemaRegistry registry, String subject) {
    this.schemaRegistry = registry;
    this.subject = subject;
  }

  @GET
  @Path("/{version}")
  @PerformanceMetric("subjects.versions.get-schema")
  public Schema getSchema(@PathParam("version") String version) {
    VersionId versionId = new VersionId(version);
    Schema schema = null;
    try {
      schema = schemaRegistry.get(this.subject, versionId.getVersionId());
      if (schema == null) {
        if (!schemaRegistry.listSubjects().contains(this.subject)) {
          throw Errors.subjectNotFoundException();
        } else {
          throw Errors.versionNotFoundException();
        }
      }
    } catch (SchemaRegistryException e) {
      log.debug("Error while retrieving schema for subject " + this.subject + " with version " +
                version + " from the schema registry", e);
      throw new ClientErrorException(Response.Status.INTERNAL_SERVER_ERROR, e);
    }
    return schema;
  }

  @GET
  @PerformanceMetric("subjects.versions.list")
  public List<Integer> list() {
    // check if subject exists. If not, throw 404
    Iterator<Schema> allSchemasForThisTopic = null;
    List<Integer> allVersions = new ArrayList<Integer>();
    try {
      if (!schemaRegistry.listSubjects().contains(this.subject)) {
        throw new NotFoundException("Subject " + this.subject + " does not exist in the registry");
      }
      allSchemasForThisTopic = schemaRegistry.getAllVersions(this.subject);
    } catch (SchemaRegistryException e) {
      throw new ServerErrorException(Response.Status.INTERNAL_SERVER_ERROR, e);
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
  @PerformanceMetric("subjects.versions.register")
  public void register(final @Suspended AsyncResponse asyncResponse,
                       final @HeaderParam("Content-Type") String contentType,
                       final @HeaderParam("Accept") String accept,
                       @PathParam("subject") String subjectName,
                       RegisterSchemaRequest request) {

    Map<String, String> headerProperties = new HashMap<String, String>();
    headerProperties.put("Content-Type", contentType);
    headerProperties.put("Accept", accept);
    Schema schema = new Schema(subjectName, 0, 0, request.getSchema());
    int id = 0;
    try {
      id = schemaRegistry.registerOrForward(subjectName, schema, headerProperties);
    } catch (SchemaRegistryException e) {
      throw new ClientErrorException(Response.Status.INTERNAL_SERVER_ERROR, e);
    }
    RegisterSchemaResponse registerSchemaResponse = new RegisterSchemaResponse();
    registerSchemaResponse.setId(id);
    asyncResponse.resume(registerSchemaResponse);
  }
}
