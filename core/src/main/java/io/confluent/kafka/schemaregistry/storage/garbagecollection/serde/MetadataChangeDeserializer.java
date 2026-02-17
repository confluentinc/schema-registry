package io.confluent.kafka.schemaregistry.storage.garbagecollection.serde;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.util.JsonFormat;
import io.confluent.protobuf.events.catalog.v1.MetadataChange;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class MetadataChangeDeserializer {
  private static final String PROTOBUF = "protobuf";
  private static final String JSON = "json";
  private static ObjectMapper MAPPER = new ObjectMapper();

  public MetadataChange deserialize(byte[] data, String format) {
    MetadataChange metadataChange;

    try {
      if (format.equals(PROTOBUF)) {
        metadataChange = MetadataChange.parseFrom(data);
      } else if (format.equals(JSON)) {
        MetadataChange.Builder builder = MetadataChange.newBuilder();
        String str = new String(data, StandardCharsets.UTF_8);
        JsonFormat.parser().merge(str, builder);
        metadataChange = builder.build();
      } else {
        throw new IllegalArgumentException(
                "Unsupported MetadataChange deserialization format: " + format);
      }
    } catch (IOException e) {
      throw new IllegalArgumentException("Can't parse metadataChange with format " + format, e);
    }
    return metadataChange;
  }
}
