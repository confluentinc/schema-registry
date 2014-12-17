package io.confluent.kafka.schemaregistry.storage;

import java.util.Iterator;
import java.util.Set;

import io.confluent.kafka.schemaregistry.rest.entities.Schema;
import io.confluent.kafka.schemaregistry.storage.exceptions.SchemaRegistryException;
import io.confluent.kafka.schemaregistry.storage.exceptions.StoreException;

public interface SchemaRegistry {

  int register(String topic, Schema schema) throws SchemaRegistryException;

  Schema get(String topic, int version) throws SchemaRegistryException;

  Set<String> listTopics();

  Iterator<Schema> getAll(String topic) throws StoreException;

  Iterator<Schema> getAllVersions(String topic) throws StoreException;

  boolean isCompatible(String topic, Schema schema1, Schema schema2);

  void close();
}
