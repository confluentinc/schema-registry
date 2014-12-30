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

  public final static String MESSAGE_SCHEMA_NOT_FOUND = "Schema not found.";
  public final static String MESSAGE_SUBJECT_NOT_FOUND = "Subject not found.";
  private static final Logger log = LoggerFactory.getLogger(SubjectsResource.class);
  private final SchemaRegistry schemaRegistry;

  public SubjectsResource(SchemaRegistry schemaRegistry) {
    this.schemaRegistry = schemaRegistry;
  }

  @GET
  @Path("/{id}")
  public Schema getSchema(@PathParam("id") Long id) {
    Schema schema = null;
    try {
      schema = schemaRegistry.get(id);
    } catch (SchemaRegistryException e) {
      log.debug("Error while retrieving schema with id " + id + " from the schema registry",
                e);
      throw new NotFoundException(MESSAGE_SCHEMA_NOT_FOUND, e);
    }
    if (schema == null) {
      throw new NotFoundException(MESSAGE_SCHEMA_NOT_FOUND);
    }
    return schema;
  }

  @Path("/{subject}/versions")
  public SchemasResource getKeySchemas(@PathParam("subject") String subjectName) {
    // check if suffix is "key"
    if (subjectName != null) {
      subjectName = subjectName.trim();
    } else {
      throw new NotFoundException(MESSAGE_SUBJECT_NOT_FOUND);
    }
    if (subjectName.endsWith(",key")) {
      return new SchemasResource(schemaRegistry, subjectName, true);
    } else {
      return new SchemasResource(schemaRegistry, subjectName, false);
    }
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
