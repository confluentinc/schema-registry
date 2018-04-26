package io.confluent.kafka.exceptions;

import org.apache.kafka.common.errors.SerializationException;

/**
 * Indicates that the SpecificRecord class was not found when SPECIFIC_AVRO_READER_CONFIG
 * is enabled
 */
public class SpecificRecordClassNotFound extends SerializationException {

  public SpecificRecordClassNotFound(String message) {
    super(message);
  }
}
