package io.confluent.kafka.schemaregistry.storage;

import java.util.Iterator;
import java.util.Set;

import io.confluent.kafka.schemaregistry.rest.entities.Schema;
import io.confluent.kafka.schemaregistry.rest.entities.SchemaSubType;
import io.confluent.kafka.schemaregistry.storage.exceptions.SchemaRegistryException;
import io.confluent.kafka.schemaregistry.storage.exceptions.StoreException;

public interface SchemaRegistry {

  int register(String topic, SchemaSubType schemaSubType, String schema) throws SchemaRegistryException;

  Schema get(String topic, SchemaSubType schemaSubType, int version) throws SchemaRegistryException;

  Set<String> listTopics();

  Iterator<Schema> getAll(String topic, SchemaSubType schemaSubType) throws StoreException;

  Iterator<Schema> getAllVersions(String topic, SchemaSubType schemaSubType) throws StoreException;

  boolean isCompatible(Schema schema1, Schema schema2);

  void close();
}
