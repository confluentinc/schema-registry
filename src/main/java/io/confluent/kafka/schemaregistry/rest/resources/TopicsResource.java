package io.confluent.kafka.schemaregistry.rest.resources;

import io.confluent.kafka.schemaregistry.rest.Versions;
import io.confluent.kafka.schemaregistry.rest.entities.Topic;

import javax.ws.rs.*;
import java.util.ArrayList;
import java.util.List;

@Path("/topics")
@Produces({Versions.SCHEMA_REGISTRY_V1_JSON_WEIGHTED,
    Versions.SCHEMA_REGISTRY_DEFAULT_JSON_WEIGHTED,
    Versions.JSON_WEIGHTED})
@Consumes({Versions.SCHEMA_REGISTRY_V1_JSON,
    Versions.SCHEMA_REGISTRY_DEFAULT_JSON,
    Versions.JSON, Versions.GENERIC_REQUEST})
public class TopicsResource {
    public final static String MESSAGE_TOPIC_NOT_FOUND = "Topic not found.";
    private List<Topic> topics = new ArrayList<Topic>();

    public TopicsResource() {
        topics.add(new Topic("Kafka", "full", "all", "all", null));
        topics.add(new Topic("Rocks", "backward", "all", "all", null));
    }

    @GET
    @Path("/{topic}")
    public Topic getTopic(@PathParam("topic") String topicName) {
        Topic topic = null;
        for (Topic t : topics) {
            if (t.getName().equals(topicName)) {
                topic = t;
                break;
            }
        }
        if (topic == null)
            throw new NotFoundException(MESSAGE_TOPIC_NOT_FOUND);
        return topic;
    }

    @Path("/{topic}/key/versions")
    public SchemasResource getKeySchemas(@PathParam("topic") String topicName) {
        return new SchemasResource(topics, topicName, true);
    }

    @Path("/{topic}/value/versions")
    public SchemasResource getValueSchemas(@PathParam("topic") String topicName) {
        return new SchemasResource(topics, topicName, false);
    }

    @GET
    public List<String> list() {
        List<String> topicNames = new ArrayList<String>();
        for (Topic topic : topics) {
            topicNames.add(topic.getName());
        }
        return topicNames;
    }

}
