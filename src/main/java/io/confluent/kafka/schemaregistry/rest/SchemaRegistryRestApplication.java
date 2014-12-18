package io.confluent.kafka.schemaregistry.rest;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

import javax.ws.rs.core.Configurable;

import io.confluent.kafka.schemaregistry.rest.resources.RootResource;
import io.confluent.kafka.schemaregistry.rest.resources.SchemasResource;
import io.confluent.kafka.schemaregistry.rest.resources.TopicsResource;
import io.confluent.kafka.schemaregistry.storage.KafkaSchemaRegistry;
import io.confluent.kafka.schemaregistry.storage.KafkaStoreConfig;
import io.confluent.kafka.schemaregistry.storage.SchemaRegistry;
import io.confluent.kafka.schemaregistry.storage.SchemaRegistryConfig;
import io.confluent.kafka.schemaregistry.storage.exceptions.SchemaRegistryException;
import io.confluent.kafka.schemaregistry.storage.serialization.SchemaSerializer;
import io.confluent.rest.Application;
import io.confluent.rest.ConfigurationException;

public class SchemaRegistryRestApplication extends Application<SchemaRegistryRestConfiguration> {

  private static final Logger log = LoggerFactory.getLogger(SchemaRegistryRestApplication.class);

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
    Properties props = new Properties();
    props.put(KafkaStoreConfig.KAFKASTORE_CONNECTION_URL_CONFIG, "localhost:2181");
    props.put(KafkaStoreConfig.KAFKASTORE_TOPIC_CONFIG, "_schemas");
    SchemaRegistryConfig schemaRegistryConfig = new SchemaRegistryConfig(props);
    SchemaRegistry schemaRegistry = null;
    try {
      schemaRegistry = new KafkaSchemaRegistry(schemaRegistryConfig,
                                               new SchemaSerializer());
    } catch (SchemaRegistryException e) {
      log.error("Error starting the schema registry", e);
      System.exit(1);
    }
    config.register(RootResource.class);
    config.register(new TopicsResource(schemaRegistry));
    config.register(SchemasResource.class);
  }

  @Override
  public SchemaRegistryRestConfiguration configure() throws ConfigurationException {
    return config;
  }

}
