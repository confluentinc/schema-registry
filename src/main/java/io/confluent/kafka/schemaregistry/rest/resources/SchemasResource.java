package io.confluent.kafka.schemaregistry.rest.resources;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import javax.ws.rs.ClientErrorException;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.NotFoundException;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.container.Suspended;
import javax.ws.rs.core.Response;

import io.confluent.kafka.schemaregistry.rest.Versions;
import io.confluent.kafka.schemaregistry.rest.entities.Schema;
import io.confluent.kafka.schemaregistry.rest.entities.requests.RegisterSchemaRequest;
import io.confluent.kafka.schemaregistry.rest.entities.requests.RegisterSchemaResponse;
import io.confluent.kafka.schemaregistry.storage.SchemaRegistry;
import io.confluent.kafka.schemaregistry.storage.exceptions.SchemaRegistryException;
import io.confluent.kafka.schemaregistry.storage.exceptions.StoreException;

@Produces({Versions.SCHEMA_REGISTRY_V1_JSON_WEIGHTED,
           Versions.SCHEMA_REGISTRY_DEFAULT_JSON_WEIGHTED,
           Versions.JSON_WEIGHTED})
@Consumes({Versions.SCHEMA_REGISTRY_V1_JSON,
           Versions.SCHEMA_REGISTRY_DEFAULT_JSON,
           Versions.JSON, Versions.GENERIC_REQUEST})
public class SchemasResource {

  public final static String MESSAGE_SCHEMA_NOT_FOUND = "Schema not found.";
  private static final Logger log = LoggerFactory.getLogger(SchemasResource.class);

  private final String topic;
  private final String schemaSubType;
  private final SchemaRegistry schemaRegistry;

  public SchemasResource(SchemaRegistry registry, String topic, String schemaSubType) {
    this.schemaRegistry = registry;
    this.topic = topic;
    this.schemaSubType = schemaSubType;
  }

  @GET
  @Path("/{version}")
  public Schema getSchema(@PathParam("version") Integer version) {
    Schema schema = null;
    try {
      schema = schemaRegistry.get(this.topic, this.schemaSubType, version);
    } catch (SchemaRegistryException e) {
      log.debug("Error while retrieving schema with version " + version + " from the schema registry",
                e);
      throw new NotFoundException(MESSAGE_SCHEMA_NOT_FOUND, e);
    }
    if (schema == null) {
      throw new NotFoundException(MESSAGE_SCHEMA_NOT_FOUND);
    }
    return schema;
  }

  @GET
  public List<Integer> list() {
    Iterator<Schema> allSchemasForThisTopic = null;
    List<Integer> allVersions = new ArrayList<Integer>();
    try {
      allSchemasForThisTopic = schemaRegistry.getAllVersions(this.topic, this.schemaSubType);
    } catch (StoreException e) {
      throw new ClientErrorException(Response.Status.INTERNAL_SERVER_ERROR, e);
    }
    while (allSchemasForThisTopic.hasNext()) {
      Schema schema = allSchemasForThisTopic.next();
      allVersions.add(schema.getVersion());
    }
    return allVersions;
  }

  @POST
  public void register(final @Suspended AsyncResponse asyncResponse,
                       @PathParam("topic") String topicName, RegisterSchemaRequest request) {
    int version = 0;
    try {
      version = schemaRegistry.register(topicName, this.schemaSubType, request.getSchema());
    } catch (SchemaRegistryException e) {
      throw new ClientErrorException(Response.Status.INTERNAL_SERVER_ERROR, e);
    }
    RegisterSchemaResponse registerSchemaResponse = new RegisterSchemaResponse();
    registerSchemaResponse.setVersion(version);
    asyncResponse.resume(registerSchemaResponse);
  }
}
