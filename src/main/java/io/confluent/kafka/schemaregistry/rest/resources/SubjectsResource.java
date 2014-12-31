package io.confluent.kafka.schemaregistry.rest.resources;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;

import javax.ws.rs.ClientErrorException;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.NotFoundException;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Response;

import io.confluent.kafka.schemaregistry.rest.Versions;
import io.confluent.kafka.schemaregistry.rest.entities.Schema;
import io.confluent.kafka.schemaregistry.storage.SchemaRegistry;
import io.confluent.kafka.schemaregistry.storage.exceptions.SchemaRegistryException;

@Path("/subjects")
@Produces({Versions.SCHEMA_REGISTRY_V1_JSON_WEIGHTED,
           Versions.SCHEMA_REGISTRY_DEFAULT_JSON_WEIGHTED,
           Versions.JSON_WEIGHTED})
@Consumes({Versions.SCHEMA_REGISTRY_V1_JSON,
           Versions.SCHEMA_REGISTRY_DEFAULT_JSON,
           Versions.JSON, Versions.GENERIC_REQUEST})
public class SubjectsResource {

  public final static String MESSAGE_SUBJECT_NOT_FOUND = "Subject not found.";
  private static final Logger log = LoggerFactory.getLogger(SubjectsResource.class);
  private final SchemaRegistry schemaRegistry;

  public SubjectsResource(SchemaRegistry schemaRegistry) {
    this.schemaRegistry = schemaRegistry;
  }

  @Path("/{subject}/versions")
  public SchemasResource getSchemas(@PathParam("subject") String subjectName) {
    if (subjectName != null) {
      subjectName = subjectName.trim();
    } else {
      throw new NotFoundException(MESSAGE_SUBJECT_NOT_FOUND);
    }
    return new SchemasResource(schemaRegistry, subjectName);
  }

  @GET
  public Set<String> list() {
    try {
      return schemaRegistry.listSubjects();
    } catch (SchemaRegistryException e) {
      throw new ClientErrorException(Response.Status.INTERNAL_SERVER_ERROR, e);
    }
  }
}
