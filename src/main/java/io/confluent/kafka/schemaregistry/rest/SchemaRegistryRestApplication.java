package io.confluent.kafka.schemaregistry.rest;

import io.confluent.kafka.schemaregistry.rest.resources.RootResource;
import io.confluent.kafka.schemaregistry.rest.resources.SchemasResource;
import io.confluent.kafka.schemaregistry.rest.resources.TopicsResource;
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
//        MetadataObserver mdObserver = new MetadataObserver(appConfig);
//        ProducerPool producerPool = new ProducerPool(appConfig);
//        ConsumerManager consumerManager = new ConsumerManager(appConfig, mdObserver);
//        Context ctx = new Context(appConfig, mdObserver, producerPool, consumerManager);
        config.register(RootResource.class);
        config.register(new TopicsResource());
        config.register(SchemasResource.class);
//        config.register(new BrokersResource(ctx));
//        config.register(new TopicsResource(ctx));
//        config.register(PartitionsResource.class);
//        config.register(new ConsumersResource(ctx));
    }

    @Override
    public SchemaRegistryRestConfiguration configure() throws ConfigurationException {
        return config;
    }

}
