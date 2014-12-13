package io.confluent.kafka.schemaregistry.rest.resources;

import io.confluent.kafka.schemaregistry.rest.Versions;
import io.confluent.kafka.schemaregistry.rest.entities.Schema;
import io.confluent.kafka.schemaregistry.rest.entities.Topic;

import javax.ws.rs.*;
import java.util.ArrayList;
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
    private boolean isKey;
    List<Integer> versions = new ArrayList<Integer>();
    List<Topic> topics = null;

    public SchemasResource() {
        this.topic = null;
        this.isKey = false;
        versions.add(1);
        versions.add(2);
    }

    public SchemasResource(List<Topic> topics, String topic, boolean isKey) {
        this.topics = topics;
        this.topic = topic;
        this.isKey = isKey;
        versions.add(1);
        versions.add(2);
    }

    @GET
    @Path("/{id}")
    public Schema getSchema(@PathParam("id") Integer id) {
        String schema = "dummy";
        if (schema == null)
            throw new NotFoundException(MESSAGE_SCHEMA_NOT_FOUND);
        return new Schema(topic, 1, schema, true, false, true);
    }

    @GET
    public List<Integer> list() {
        return versions;
    }
}
