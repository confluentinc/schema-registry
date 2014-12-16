package io.confluent.kafka.schemaregistry.rest.resources;

import io.confluent.kafka.schemaregistry.rest.Versions;
import io.confluent.kafka.schemaregistry.rest.entities.Schema;
import io.confluent.kafka.schemaregistry.rest.entities.requests.RegisterSchemaRequest;
import io.confluent.kafka.schemaregistry.rest.entities.requests.RegisterSchemaResponse;
import io.confluent.kafka.schemaregistry.storage.SchemaRegistry;
import io.confluent.kafka.schemaregistry.storage.exceptions.StoreException;

import javax.ws.rs.*;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.container.Suspended;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

@Produces({Versions.SCHEMA_REGISTRY_V1_JSON_WEIGHTED,
    Versions.SCHEMA_REGISTRY_DEFAULT_JSON_WEIGHTED,
    Versions.JSON_WEIGHTED})
@Consumes({Versions.SCHEMA_REGISTRY_V1_JSON,
    Versions.SCHEMA_REGISTRY_DEFAULT_JSON,
    Versions.JSON, Versions.GENERIC_REQUEST})
public class SchemasResource {
    public final static String MESSAGE_SCHEMA_NOT_FOUND = "Schema not found.";

    private final String topic;
    private final boolean isKey;
    private final SchemaRegistry schemaRegistry;

    public SchemasResource(SchemaRegistry registry, String topic, boolean isKey) {
        this.schemaRegistry = registry;
        this.topic = topic;
        this.isKey = isKey;
    }

    @GET
    @Path("/{id}")
    public Schema getSchema(@PathParam("id") Integer id) {
        Schema schema = schemaRegistry.get(this.topic, id);
        if (schema == null)
            throw new NotFoundException(MESSAGE_SCHEMA_NOT_FOUND);
        return schema;
    }

    @GET
    public List<Integer> list() {
        Iterator<Schema> allSchemasForThisTopic = null;
        List<Integer> allVersions = new ArrayList<Integer>();
        try {
            allSchemasForThisTopic = schemaRegistry.getAllVersions(this.topic);
        } catch (StoreException e) {
            // TODO: throw meaningful exception
            e.printStackTrace();
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
        Schema schema = new Schema(topicName, 0, request.getSchema(), true, false, true);
        int version = schemaRegistry.register(topicName, schema);
        RegisterSchemaResponse registerSchemaResponse = new RegisterSchemaResponse();
        registerSchemaResponse.setVersion(version);
        asyncResponse.resume(registerSchemaResponse);
    }
}
