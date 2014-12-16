package io.confluent.kafka.schemaregistry.rest;

import io.confluent.kafka.schemaregistry.rest.resources.RootResource;
import io.confluent.kafka.schemaregistry.rest.resources.SchemasResource;
import io.confluent.kafka.schemaregistry.rest.resources.TopicsResource;
import io.confluent.kafka.schemaregistry.storage.SchemaRegistry;
import io.confluent.kafka.schemaregistry.storage.SchemaRegistryConfig;
import io.confluent.kafka.schemaregistry.storage.serialization.SchemaSerializer;
import io.confluent.rest.Application;
import io.confluent.rest.ConfigurationException;

import javax.ws.rs.core.Configurable;
import java.util.Properties;

public class SchemaRegistryRestApplication extends Application<SchemaRegistryRestConfiguration> {
    public SchemaRegistryRestApplication() throws ConfigurationException {
        this(new Properties());
    }

    public SchemaRegistryRestApplication(Properties props) throws ConfigurationException {
        this(new SchemaRegistryRestConfiguration(props));
    }

    public SchemaRegistryRestApplication(SchemaRegistryRestConfiguration config) {
        this.config = config;
    }

    @Override
    public void setupResources(Configurable<?> config, SchemaRegistryRestConfiguration appConfig) {
        SchemaRegistryConfig schemaRegistryConfig = new SchemaRegistryConfig(new Properties());
        SchemaRegistry schemaRegistry =
            new SchemaRegistry(schemaRegistryConfig, new SchemaSerializer());
        config.register(RootResource.class);
        config.register(new TopicsResource(schemaRegistry));
        config.register(SchemasResource.class);
    }

    @Override
    public SchemaRegistryRestConfiguration configure() throws ConfigurationException {
        return config;
    }

}
