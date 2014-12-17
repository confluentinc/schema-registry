package io.confluent.kafka.schemaregistry.rest.resources;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;

import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.NotFoundException;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;

import io.confluent.kafka.schemaregistry.rest.Versions;
import io.confluent.kafka.schemaregistry.rest.entities.Topic;
import io.confluent.kafka.schemaregistry.storage.SchemaRegistry;

@Path("/topics")
@Produces({Versions.SCHEMA_REGISTRY_V1_JSON_WEIGHTED,
    Versions.SCHEMA_REGISTRY_DEFAULT_JSON_WEIGHTED,
    Versions.JSON_WEIGHTED})
@Consumes({Versions.SCHEMA_REGISTRY_V1_JSON,
    Versions.SCHEMA_REGISTRY_DEFAULT_JSON,
    Versions.JSON, Versions.GENERIC_REQUEST})
public class TopicsResource {
    public final static String MESSAGE_TOPIC_NOT_FOUND = "Topic not found.";
  private static final Logger log = LoggerFactory.getLogger(TopicsResource.class);
  private final SchemaRegistry schemaRegistry;

    public TopicsResource(SchemaRegistry schemaRegistry) {
        this.schemaRegistry = schemaRegistry;
    }

    @GET
    @Path("/{topic}")
    public Topic getTopic(@PathParam("topic") String topicName) {
        if (!schemaRegistry.listTopics().contains(topicName))
            throw new NotFoundException(MESSAGE_TOPIC_NOT_FOUND);
      // TODO: https://github.com/confluentinc/schema-registry/issues/3 Implement metadata
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
        return schemaRegistry.listTopics();
    }

}
