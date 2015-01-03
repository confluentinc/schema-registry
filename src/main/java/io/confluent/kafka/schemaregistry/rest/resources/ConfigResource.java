/*
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

import java.util.Set;

import javax.ws.rs.ClientErrorException;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.NotFoundException;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Response;

import io.confluent.kafka.schemaregistry.rest.Versions;
import io.confluent.kafka.schemaregistry.rest.entities.Config;
import io.confluent.kafka.schemaregistry.rest.entities.requests.ConfigUpdateRequest;
import io.confluent.kafka.schemaregistry.storage.KafkaSchemaRegistry;
import io.confluent.kafka.schemaregistry.storage.exceptions.SchemaRegistryException;

@Path("/config")
@Produces({Versions.SCHEMA_REGISTRY_V1_JSON_WEIGHTED,
           Versions.SCHEMA_REGISTRY_DEFAULT_JSON_WEIGHTED,
           Versions.JSON_WEIGHTED})
@Consumes({Versions.SCHEMA_REGISTRY_V1_JSON,
           Versions.SCHEMA_REGISTRY_DEFAULT_JSON,
           Versions.JSON, Versions.GENERIC_REQUEST})
public class ConfigResource {

  public final static String MESSAGE_SUBJECT_NOT_FOUND = "Subject not found.";
  private static final Logger log = LoggerFactory.getLogger(ConfigResource.class);
  private final KafkaSchemaRegistry schemaRegistry;

  public ConfigResource(KafkaSchemaRegistry schemaRegistry) {
    this.schemaRegistry = schemaRegistry;
  }

  @Path("/{subject}")
  @PUT
  public void updateSubjectLevelConfig(@PathParam("subject") String subject,
                                       ConfigUpdateRequest request) {
    if (request != null) {
      try {
        Set<String> subjects = schemaRegistry.listSubjects();
        if (!subjects.contains(subject)) {
          throw new NotFoundException(MESSAGE_SUBJECT_NOT_FOUND);
        }
        schemaRegistry.updateCompatibilityLevel(subject, request.getCompatibilityLevel());
        log.debug("Updated compatibility level to " + request.getCompatibilityLevel());
      } catch (SchemaRegistryException e) {
        throw new ClientErrorException(Response.Status.INTERNAL_SERVER_ERROR, e);
      }
    }
  }

  @Path("/{subject}")
  @GET
  public Config getSubjectLevelConfig(@PathParam("subject") String subject) {
    Config config = null;
    try {
      Set<String> subjects = schemaRegistry.listSubjects();
      if (!subjects.contains(subject)) {
        throw new NotFoundException(MESSAGE_SUBJECT_NOT_FOUND);
      }
      config = new Config(schemaRegistry.getCompatibilityLevel(subject));
    } catch (SchemaRegistryException e) {
      throw new ClientErrorException(Response.Status.INTERNAL_SERVER_ERROR, e);
    }
    return config;
  }

  @PUT
  public void updateTopLevelConfig(ConfigUpdateRequest request) {
    if (request.getCompatibilityLevel() != null) {
      try {
        schemaRegistry.updateCompatibilityLevel(null, request.getCompatibilityLevel());
        log.debug("Updated compatibility level to " + request.getCompatibilityLevel());
      } catch (SchemaRegistryException e) {
        throw new ClientErrorException(Response.Status.INTERNAL_SERVER_ERROR, e);
      }
    }
  }

  @GET
  public Config getTopLevelConfig() {
    Config config = null;
    try {
      config = new Config(schemaRegistry.getCompatibilityLevel(null));
    } catch (SchemaRegistryException e) {
      throw new ClientErrorException(Response.Status.INTERNAL_SERVER_ERROR, e);
    }
    return config;
  }
}
