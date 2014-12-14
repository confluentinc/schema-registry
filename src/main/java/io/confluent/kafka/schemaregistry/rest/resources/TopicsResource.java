package io.confluent.kafka.schemaregistry.rest.resources;

import io.confluent.kafka.schemaregistry.rest.Versions;
import io.confluent.kafka.schemaregistry.rest.entities.Schema;
import io.confluent.kafka.schemaregistry.rest.entities.Topic;
import io.confluent.kafka.schemaregistry.storage.SchemaRegistry;

import javax.ws.rs.*;
import java.util.Set;

@Path("/topics")
@Produces({Versions.SCHEMA_REGISTRY_V1_JSON_WEIGHTED,
    Versions.SCHEMA_REGISTRY_DEFAULT_JSON_WEIGHTED,
    Versions.JSON_WEIGHTED})
@Consumes({Versions.SCHEMA_REGISTRY_V1_JSON,
    Versions.SCHEMA_REGISTRY_DEFAULT_JSON,
    Versions.JSON, Versions.GENERIC_REQUEST})
public class TopicsResource {
    public final static String MESSAGE_TOPIC_NOT_FOUND = "Topic not found.";
    private final SchemaRegistry schemaRegistry;

    public TopicsResource(SchemaRegistry schemaRegistry) {
        this.schemaRegistry = schemaRegistry;
    }

    @GET
    @Path("/{topic}")
    public Topic getTopic(@PathParam("topic") String topicName) {
        System.out.println("Received get topic request for " + topicName);
        if (!schemaRegistry.listTopics().contains(topicName))
            throw new NotFoundException(MESSAGE_TOPIC_NOT_FOUND);
        // TODO: Implement topic/schema metadata
        return new Topic(topicName);
    }

    @Path("/{topic}/key/versions")
    public SchemasResource getKeySchemas(@PathParam("topic") String topicName) {
        return new SchemasResource(schemaRegistry, topicName, true);
    }

    @Path("/{topic}/value/versions")
    public SchemasResource getValueSchemas(@PathParam("topic") String topicName) {
        return new SchemasResource(schemaRegistry, topicName, false);
    }

    @GET
    public Set<String> list() {
        System.out.println("Received list topics request");
        return schemaRegistry.listTopics();
    }

}
