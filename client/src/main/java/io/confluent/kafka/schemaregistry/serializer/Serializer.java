package io.confluent.kafka.schemaregistry.serializer;

import java.util.Map;

/**
 *
 * @param <T> Type to be serialized from.
 *
 * A class that implements this interface is expected to have a constructor with no parameter.
 */
public interface Serializer<T> {

  /**
   * Configure this class.
   * @param configs configs in key/value pairs
   * @param isKey whether is for key or value
   */
  public void configure(Map<String, ?> configs, boolean isKey);

  /**
   * @param topic topic associated with data
   * @param data typed data
   * @return serialized bytes
   */
  public byte[] serialize(String topic, T data);

  /**
   * Close this serializer
   */
  public void close();
}
